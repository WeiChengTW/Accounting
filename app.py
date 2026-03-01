import sqlite3
import re
from datetime import datetime, timedelta, timezone
import os
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

from flask import Flask, request, abort

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    MessageEvent,
    TextMessage,
    TextSendMessage,
)

app = Flask(__name__)


HELP_TEXT = """請用以下格式：
記帳：
@記帳 項目 金額 [收支] [日期] [@對象]
（支援多行輸入）
記帳預設：支出、當天
記帳項目含「銀行」會自動視為 "收入"
-
補款：
@記帳 補款 名稱 金額
（表示打指令的人補款給對方，例如：@記帳 補款 @小明 2000）
 -
刪除：
@記帳 刪除 ID
-
修改：
@記帳 修改 ID 項目 金額 [收支] [日期]
@記帳 修改 ID [收支或日期]
@記帳 修改 ID [項目|金額|日期|收支] 值
可一次改多欄位：
@記帳 修改 ID 項目 A 金額 1000 日期 2/20 收支 收入
-
查詢：
@記帳 查詢 [範圍]
-
範圍查詢：
@記帳 範圍查詢 起始月到結束月
-
算錢：
@記帳 算錢 [月份]
（預設 3 人；月份未填用當月，例如：@記帳 算錢、@記帳 算錢 4、@記帳 算錢 4月）
-
詳細查詢：
@記帳 詳細查詢 [範圍]
-
範圍選項：日 / 周 / 月 / 年 / 全部
可用範圍例子：2/25、2月、2025、2月到5月
欄位分隔支援：空白 / ， / ,
查詢預設範圍：月
算錢固定人數：3
算錢預設月份：當月
詳細查詢預設範圍：月
"""
# 狀態（暫不顯示在教學）
# @記帳 狀態
# （查看目前資料庫模式）
#
# 成員檢查（暫不顯示在教學）
# @記帳 成員檢查
# （顯示 API / 算錢採用成員）
#
# 新增成員（暫不顯示在教學）
# @記帳 新增成員 名稱
# （手動登記成員名稱，供算錢補位顯示）
# @記帳 刪除成員 ID
# （依成員檢查採用名單的序號刪除手動補登成員）


def resolve_db_path():
    env_db_path = os.getenv("DB_PATH")
    if env_db_path:
        return env_db_path

    if os.getenv("VERCEL") == "1" or os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        return "/tmp/bookkeeping.db"

    return "bookkeeping.db"


def resolve_database_url():
    def sanitize_postgres_url(raw_url):
        db_url = raw_url.strip().strip('"').strip("'")
        if not db_url:
            return None

        parsed = urlparse(db_url)
        if parsed.scheme not in {"postgres", "postgresql"}:
            return db_url

        allowed_query_keys = {
            "application_name",
            "channel_binding",
            "connect_timeout",
            "gssencmode",
            "keepalives",
            "keepalives_count",
            "keepalives_idle",
            "keepalives_interval",
            "options",
            "passfile",
            "service",
            "sslcert",
            "sslkey",
            "sslmode",
            "sslpassword",
            "sslrootcert",
            "target_session_attrs",
        }
        filtered_query = urlencode(
            [
                (key, value)
                for key, value in parse_qsl(parsed.query, keep_blank_values=True)
                if key in allowed_query_keys
            ],
            doseq=True,
        )
        return urlunparse(parsed._replace(query=filtered_query))

    def build_postgres_url_from_components():
        host = (os.getenv("DATABASE_POSTGRES_HOST") or "").strip()
        user = (os.getenv("DATABASE_POSTGRES_USER") or "").strip()
        password = (os.getenv("DATABASE_POSTGRES_PASSWORD") or "").strip()
        database = (os.getenv("DATABASE_POSTGRES_DATABASE") or "").strip() or "postgres"
        port = (os.getenv("DATABASE_POSTGRES_PORT") or "").strip() or "5432"

        if not host or not user or not password:
            return None

        if ":" in host and not os.getenv("DATABASE_POSTGRES_PORT"):
            host_part = host
        else:
            host_part = f"{host}:{port}"

        return (
            "postgresql://"
            f"{quote(user, safe='')}:{quote(password, safe='')}@"
            f"{host_part}/{quote(database, safe='')}"
            "?sslmode=require"
        )

    for env_key in (
        "DATABASE_URL",
        "DATABASE_POSTGRES_URL",
        "DATABASE_POSTGRES_URL_NON_POOLING",
        "DATABASE_POSTGRES_PRISMA_URL",
        "SUPABASE_DB_URL",
        "SUPABASE_DB_URI",
        "SUPABASE_DATABASE_URL",
        "SUPABASE_DATABASE_URI",
        "SUPABASE_POOLER_URL",
        "SUPABASE_POOL_URL",
        "POSTGRES_URL",
        "POSTGRESQL_URL",
        "POSTGRES_PRISMA_URL",
        "POSTGRES_URL_NON_POOLING",
    ):
        env_value = os.getenv(env_key)
        if env_value and env_value.strip():
            return sanitize_postgres_url(env_value), env_key

    component_url = build_postgres_url_from_components()
    if component_url:
        return component_url, "DATABASE_POSTGRES_*"

    return None, None


APP_TIMEZONE = timezone(timedelta(hours=8))


def get_now():
    return datetime.now(APP_TIMEZONE).replace(tzinfo=None, microsecond=0)


DB_PATH = resolve_db_path()
DATABASE_URL, DATABASE_URL_SOURCE = resolve_database_url()
IS_POSTGRES = bool(DATABASE_URL)
IS_SERVERLESS = os.getenv("VERCEL") == "1" or bool(
    os.getenv("AWS_LAMBDA_FUNCTION_NAME")
)
USING_EPHEMERAL_SQLITE = IS_SERVERLESS and not IS_POSTGRES
psycopg = None
PENDING_DELETE = {}
LAST_DETAIL_VIEW = {}
BOT_USER_ID = None

if USING_EPHEMERAL_SQLITE:
    print(
        "[WARN] 目前在雲端環境執行，但未設定 Postgres 連線字串。"
        "將暫時使用 SQLite（/tmp），資料不保證持久化。"
        "建議設定：DATABASE_URL / SUPABASE_DB_URL / POSTGRES_URL"
    )


def with_storage_warning(text):
    if not USING_EPHEMERAL_SQLITE:
        return text
    return (
        f"{text}\n\n"
        "⚠️目前為雲端臨時資料庫模式（SQLite /tmp），可能在幾分鐘後清空。"
        "請設定 DATABASE_URL（Supabase Postgres）以持久保存。"
    )


def build_storage_status_text():
    lines = ["記帳系統狀態"]

    if IS_POSTGRES:
        lines.append("資料庫：Postgres（持久化）")
        if DATABASE_URL_SOURCE:
            lines.append(f"連線來源：{DATABASE_URL_SOURCE}")
        else:
            lines.append("連線來源：已設定（來源未知）")
    else:
        lines.append("資料庫：SQLite")
        lines.append(f"資料檔：{DB_PATH}")
        if USING_EPHEMERAL_SQLITE:
            lines.append("模式：雲端臨時（可能重置）")
        else:
            lines.append("模式：本機")

    lines.append("時區：Asia/Taipei (UTC+8)")
    return "\n".join(lines)


line_bot_api = LineBotApi(os.getenv("CHANNEL_ACCESS_TOKEN"))
line_handler = WebhookHandler(os.getenv("CHANNEL_SECRET"))


def to_db_created_at(created_at):
    if IS_POSTGRES:
        return created_at
    return created_at.isoformat()


def from_db_created_at(created_at_value):
    if isinstance(created_at_value, datetime):
        return created_at_value.replace(microsecond=0)
    return datetime.fromisoformat(created_at_value)


def adapt_query(query):
    if IS_POSTGRES:
        return query.replace("?", "%s")
    return query


def run_query(query, params=(), fetch_mode=None):
    global psycopg
    adapted_query = adapt_query(query)

    if IS_POSTGRES:
        if psycopg is None:
            try:
                import importlib

                psycopg = importlib.import_module("psycopg")
            except ImportError as exc:
                raise RuntimeError(
                    "DATABASE_URL 已設定，但缺少 psycopg 套件，請安裝 requirements.txt 依賴"
                ) from exc

        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(adapted_query, params)
                if fetch_mode == "one":
                    return cur.fetchone()
                if fetch_mode == "all":
                    return cur.fetchall()
                return cur.rowcount

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(adapted_query, params)
        if fetch_mode == "one":
            return cur.fetchone()
        if fetch_mode == "all":
            return cur.fetchall()
        return cur.rowcount


def init_db():
    if IS_POSTGRES:
        run_query(
            """
            CREATE TABLE IF NOT EXISTS records (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                chat_id TEXT NOT NULL DEFAULT 'unknown',
                item TEXT NOT NULL,
                amount INTEGER NOT NULL,
                record_type TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        run_query(
            "ALTER TABLE records ADD COLUMN IF NOT EXISTS chat_id TEXT NOT NULL DEFAULT 'unknown'"
        )
        run_query(
            """
            CREATE TABLE IF NOT EXISTS manual_members (
                chat_id TEXT NOT NULL,
                member_name TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (chat_id, member_name)
            )
            """
        )
        run_query(
            """
            CREATE TABLE IF NOT EXISTS settlement_payments (
                id BIGSERIAL PRIMARY KEY,
                chat_id TEXT NOT NULL,
                from_user_id TEXT NOT NULL,
                to_name TEXT NOT NULL,
                amount INTEGER NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        return

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                chat_id TEXT NOT NULL DEFAULT 'unknown',
                item TEXT NOT NULL,
                amount INTEGER NOT NULL,
                record_type TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(records)").fetchall()
        }
        if "chat_id" not in columns:
            conn.execute(
                "ALTER TABLE records ADD COLUMN chat_id TEXT NOT NULL DEFAULT 'unknown'"
            )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_members (
                chat_id TEXT NOT NULL,
                member_name TEXT NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (chat_id, member_name)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settlement_payments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                from_user_id TEXT NOT NULL,
                to_name TEXT NOT NULL,
                amount INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )


def parse_record_message(text):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines or not lines[0].startswith("@記帳"):
        return None

    def normalize_record_type(type_input):
        if type_input in {"支出", "expense", "Expense", "EXPENSE"}:
            return "支出"
        if type_input in {"收入", "income", "Income", "INCOME"}:
            return "收入"
        raise ValueError("收支類型只能填：支出 或 收入")

    def parse_mmdd_date(date_text):
        try:
            parsed = datetime.strptime(date_text, "%m/%d")
        except ValueError as exc:
            raise ValueError("日期格式請用 MM/DD，例如 02/27") from exc

        now = get_now()
        return datetime(
            year=now.year,
            month=parsed.month,
            day=parsed.day,
            hour=now.hour,
            minute=now.minute,
            second=now.second,
            microsecond=0,
        )

    command_keywords = {
        "刪除",
        "刪除成員",
        "修改",
        "新增成員",
        "補款",
        "查詢",
        "總覽",
        "算錢",
        "成員檢查",
        "成員",
        "餘額",
        "查餘額",
        "範圍查詢",
        "詳細查詢",
        "明細",
        "詳細",
        "格式",
    }

    parsed_records = []
    for line_number, line in enumerate(lines, start=1):
        if not line.startswith("@記帳"):
            raise ValueError(
                f"第{line_number}行格式錯誤，請用：@記帳 項目 金額 [支出或收入] [MM/DD] [@對象]"
            )

        payload = line[len("@記帳") :].strip()
        fields = [field for field in re.split(r"[\s,，]+", payload) if field]
        if len(fields) < 2 or len(fields) > 5:
            raise ValueError(
                f"第{line_number}行格式錯誤，請用：@記帳 項目 金額 [支出或收入] [MM/DD] [@對象]（分隔可用空白/，/,）"
            )

        item = fields[0]
        amount_text = fields[1]
        optional_fields = fields[2:]

        if item in command_keywords:
            raise ValueError(
                "多行輸入僅支援記帳格式：@記帳 項目 金額 [支出或收入] [MM/DD] [@對象]（分隔可用空白/，/,）"
            )

        if not item:
            raise ValueError(f"第{line_number}行格式錯誤，項目不可空白")

        record_type = "收入" if "銀行" in item else "支出"
        record_datetime = get_now()
        target_member_name = None
        type_or_date_options = []

        for optional_value in optional_fields:
            if optional_value.startswith("@"):
                if target_member_name is not None:
                    raise ValueError(f"第{line_number}行格式錯誤，@對象只能填一位")
                target_member_name = normalize_manual_member_name(optional_value)
            else:
                type_or_date_options.append(optional_value)

        if len(type_or_date_options) > 2:
            raise ValueError(
                f"第{line_number}行格式錯誤，請用：@記帳 項目 金額 [支出或收入] [MM/DD] [@對象]（分隔可用空白/，/,）"
            )

        if len(type_or_date_options) == 1:
            try:
                record_type = normalize_record_type(type_or_date_options[0])
            except ValueError:
                record_datetime = parse_mmdd_date(type_or_date_options[0])

        if len(type_or_date_options) == 2:
            record_type = normalize_record_type(type_or_date_options[0])
            record_datetime = parse_mmdd_date(type_or_date_options[1])

        try:
            amount = int(amount_text)
            if amount <= 0:
                raise ValueError
        except ValueError as exc:
            raise ValueError(
                f"第{line_number}行金額錯誤，金額必須是正整數，例如：@記帳 銀行，50000 收入"
            ) from exc

        parsed_records.append(
            (item, amount, record_type, record_datetime, target_member_name)
        )

    return parsed_records


def normalize_record_type_input(type_input):
    if type_input in {"支出", "expense", "Expense", "EXPENSE"}:
        return "支出"
    if type_input in {"收入", "income", "Income", "INCOME"}:
        return "收入"
    raise ValueError("收支類型只能填：支出 或 收入")


def parse_mmdd_date_input(date_text):
    try:
        parsed = datetime.strptime(date_text, "%m/%d")
    except ValueError as exc:
        raise ValueError("日期格式請用 MM/DD，例如 02/27") from exc

    now = get_now()
    return datetime(
        year=now.year,
        month=parsed.month,
        day=parsed.day,
        hour=now.hour,
        minute=now.minute,
        second=now.second,
        microsecond=0,
    )


def parse_modify_command(text):
    format_error_message = (
        "修改格式：@記帳 修改 ID 項目 金額 [收支] [日期]，"
        "或 @記帳 修改 ID [收支或日期]，"
        "或 @記帳 修改 ID [項目|金額|日期|收支] 值 ...（可一次改多欄位，分隔可用空白/，/,）"
    )

    parts = [part for part in re.split(r"[\s,，]+", text.strip()) if part]
    if len(parts) < 2 or parts[0] != "@記帳" or parts[1] != "修改":
        return None

    if len(parts) < 3:
        raise ValueError(format_error_message)

    try:
        record_id = int(parts[2])
        if record_id <= 0:
            raise ValueError
    except ValueError as exc:
        raise ValueError(format_error_message) from exc

    remaining_parts = parts[3:]
    if not remaining_parts:
        raise ValueError(format_error_message)

    if remaining_parts[0] == "更新":
        remaining_parts = remaining_parts[1:]
        if not remaining_parts:
            raise ValueError(format_error_message)

    keyword_map = {
        "項目": "item",
        "金額": "amount",
        "日期": "date",
        "收支": "record_type",
        "類型": "record_type",
    }

    if len(remaining_parts) >= 2 and remaining_parts[0] in keyword_map:
        if len(remaining_parts) % 2 != 0:
            raise ValueError(format_error_message)

        modify_data = {
            "record_id": record_id,
            "item": None,
            "amount": None,
            "record_type": None,
            "record_datetime": None,
        }

        for index in range(0, len(remaining_parts), 2):
            field_token = remaining_parts[index]
            field_value = remaining_parts[index + 1]
            if field_token not in keyword_map:
                raise ValueError(format_error_message)

            field_name = keyword_map[field_token]
            if field_name == "item":
                modify_data["item"] = field_value
                continue

            if field_name == "amount":
                try:
                    amount = int(field_value)
                    if amount <= 0:
                        raise ValueError
                except ValueError as exc:
                    raise ValueError("金額必須是正整數") from exc

                modify_data["amount"] = amount
                continue

            if field_name == "date":
                modify_data["record_datetime"] = parse_mmdd_date_input(field_value)
                continue

            modify_data["record_type"] = normalize_record_type_input(field_value)

        return modify_data

    if len(remaining_parts) == 1:
        option = remaining_parts[0]
        record_type = None
        record_datetime = None

        try:
            record_type = normalize_record_type_input(option)
        except ValueError:
            record_datetime = parse_mmdd_date_input(option)

        return {
            "record_id": record_id,
            "item": None,
            "amount": None,
            "record_type": record_type,
            "record_datetime": record_datetime,
        }

    if len(parts) < 5:
        raise ValueError(format_error_message)

    if len(remaining_parts) < 2:
        raise ValueError(format_error_message)

    item = remaining_parts[0]
    amount_text = remaining_parts[1]

    try:
        amount = int(amount_text)
        if amount <= 0:
            raise ValueError
    except ValueError as exc:
        raise ValueError("金額必須是正整數") from exc

    option_parts = remaining_parts[2:]
    if len(option_parts) > 2:
        raise ValueError(format_error_message)

    record_type = None
    record_datetime = None

    if len(option_parts) == 1:
        try:
            record_type = normalize_record_type_input(option_parts[0])
        except ValueError:
            record_datetime = parse_mmdd_date_input(option_parts[0])

    if len(option_parts) == 2:
        record_type = normalize_record_type_input(option_parts[0])
        record_datetime = parse_mmdd_date_input(option_parts[1])

    return {
        "record_id": record_id,
        "item": item,
        "amount": amount,
        "record_type": record_type,
        "record_datetime": record_datetime,
    }


def parse_delete_command(text):
    parts = [part for part in re.split(r"[\s,，]+", text.strip()) if part]
    if len(parts) != 3 or parts[0] != "@記帳" or parts[1] != "刪除":
        return None

    try:
        record_id = int(parts[2])
        if record_id <= 0:
            raise ValueError
    except ValueError as exc:
        raise ValueError("刪除格式：@記帳 刪除 ID（分隔可用空白/，/,）") from exc

    return record_id


def normalize_manual_member_name(name_text):
    if not name_text:
        raise ValueError("新增成員格式：@記帳 新增成員 名稱")

    member_name = name_text.strip()
    if not member_name:
        raise ValueError("新增成員格式：@記帳 新增成員 名稱")

    if member_name.startswith("@"):
        member_name = member_name[1:].strip()

    if not member_name:
        raise ValueError("新增成員格式：@記帳 新增成員 名稱")

    if len(member_name) > 30:
        raise ValueError("成員名稱請控制在 30 字以內")

    return member_name


def parse_add_member_command(text):
    parts = [part for part in re.split(r"[\s,，]+", text.strip()) if part]
    if len(parts) < 3 or parts[0] != "@記帳" or parts[1] != "新增成員":
        return None

    return normalize_manual_member_name(" ".join(parts[2:]))


def parse_delete_member_command(text):
    parts = [part for part in re.split(r"[\s,，]+", text.strip()) if part]
    if len(parts) != 3 or parts[0] != "@記帳" or parts[1] != "刪除成員":
        return None

    try:
        member_index = int(parts[2])
        if member_index <= 0:
            raise ValueError
    except ValueError as exc:
        raise ValueError("刪除成員格式：@記帳 刪除成員 ID") from exc

    return member_index


def parse_settlement_payment_command(text):
    parts = [part for part in re.split(r"[\s,，]+", text.strip()) if part]
    if len(parts) != 4 or parts[0] != "@記帳" or parts[1] != "補款":
        return None

    to_name = normalize_manual_member_name(parts[2])

    try:
        amount = int(parts[3])
        if amount <= 0:
            raise ValueError
    except ValueError as exc:
        raise ValueError("補款格式：@記帳 補款 名稱 金額（金額須為正整數）") from exc

    return to_name, amount


def save_manual_member(chat_id, member_name):
    normalized_name = normalize_manual_member_name(member_name)
    created_at = to_db_created_at(get_now())

    if IS_POSTGRES:
        run_query(
            """
            INSERT INTO manual_members (chat_id, member_name, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT (chat_id, member_name)
            DO UPDATE SET created_at = EXCLUDED.created_at
            """,
            (chat_id, normalized_name, created_at),
        )
    else:
        run_query(
            """
            INSERT INTO manual_members (chat_id, member_name, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id, member_name)
            DO UPDATE SET created_at = excluded.created_at
            """,
            (chat_id, normalized_name, created_at),
        )

    return normalized_name


def get_manual_members(chat_id):
    rows = run_query(
        """
        SELECT member_name
        FROM manual_members
        WHERE chat_id = ?
        ORDER BY created_at DESC
        """,
        (chat_id,),
        fetch_mode="all",
    )
    return [row[0] for row in rows]


def delete_manual_member(chat_id, member_name):
    normalized_name = normalize_manual_member_name(member_name)
    return run_query(
        "DELETE FROM manual_members WHERE chat_id = ? AND member_name = ?",
        (chat_id, normalized_name),
    )


def save_settlement_payment(chat_id, from_user_id, to_name, amount, created_at):
    run_query(
        """
        INSERT INTO settlement_payments (chat_id, from_user_id, to_name, amount, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (chat_id, from_user_id, to_name, amount, to_db_created_at(created_at)),
    )


def get_settlement_payments(chat_id, range_spec):
    range_start, range_end = get_range_start_end(range_spec)

    where_clause = "chat_id = ?"
    params = [chat_id]
    if range_start is not None:
        where_clause += " AND created_at >= ?"
        params.append(to_db_created_at(range_start))
    if range_end is not None:
        where_clause += " AND created_at < ?"
        params.append(to_db_created_at(range_end))

    return run_query(
        f"""
        SELECT from_user_id, to_name, amount
        FROM settlement_payments
        WHERE {where_clause}
        ORDER BY created_at ASC
        """,
        params,
        fetch_mode="all",
    )


def save_record(user_id, chat_id, item, amount, record_type, created_at):
    run_query(
        """
        INSERT INTO records (user_id, chat_id, item, amount, record_type, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (user_id, chat_id, item, amount, record_type, to_db_created_at(created_at)),
    )


def get_record_by_id(chat_id, record_id):
    return run_query(
        """
        SELECT id, item, amount, record_type, created_at
        FROM records
        WHERE chat_id = ? AND id = ?
        """,
        (chat_id, record_id),
        fetch_mode="one",
    )


def get_record_by_display_id(chat_id, display_id):
    if display_id <= 0:
        return None

    return run_query(
        """
        SELECT id, item, amount, record_type, created_at
        FROM records
        WHERE chat_id = ?
        ORDER BY created_at DESC, id DESC
        LIMIT 1 OFFSET ?
        """,
        (chat_id, display_id - 1),
        fetch_mode="one",
    )


def get_record_by_last_detail_id(chat_id, display_id):
    if display_id <= 0:
        return None

    detail_record_ids = LAST_DETAIL_VIEW.get(chat_id) or []
    if display_id > len(detail_record_ids):
        return None

    real_record_id = detail_record_ids[display_id - 1]
    return get_record_by_id(chat_id, real_record_id)


def format_record_detail_for_delete(display_id, record_row):
    _, item, amount, record_type, created_at = record_row
    created_at_text = from_db_created_at(created_at).strftime("%Y/%m/%d")
    return (
        f"即將刪除以下紀錄：\n"
        f"ID：{display_id}\n"
        f"日期：{created_at_text}\n"
        f"類型：{record_type}\n"
        f"項目：{item}\n"
        f"金額：{amount}\n"
        f"請回覆「確認」後刪除"
    )


def delete_record_by_id(chat_id, record_id):
    return run_query(
        "DELETE FROM records WHERE chat_id = ? AND id = ?",
        (chat_id, record_id),
    )


def update_record_by_id(chat_id, record_id, item, amount, record_type, created_at):
    return run_query(
        """
        UPDATE records
        SET item = ?, amount = ?, record_type = ?, created_at = ?
        WHERE chat_id = ? AND id = ?
        """,
        (item, amount, record_type, to_db_created_at(created_at), chat_id, record_id),
    )


def get_chat_id(event_source):
    group_id = getattr(event_source, "group_id", None)
    if group_id:
        return f"group:{group_id}"

    room_id = getattr(event_source, "room_id", None)
    if room_id:
        return f"room:{room_id}"

    user_id = getattr(event_source, "user_id", "unknown")
    return f"user:{user_id}"


def format_user_id(user_id):
    if not user_id or user_id == "unknown":
        return "未知使用者"
    if len(user_id) <= 10:
        return user_id
    return f"{user_id[:6]}...{user_id[-4:]}"


def resolve_display_name(event_source, user_id):
    if not user_id or user_id == "unknown":
        return "未知使用者"

    user_id_text = str(user_id)
    if user_id_text.startswith("__manual_"):
        return user_id_text.replace("__manual_", "", 1)
    if user_id_text.startswith("__untracked_"):
        return f"未記帳成員{user_id_text.split('_')[-1]}"

    source_type = getattr(event_source, "type", None)
    group_id = getattr(event_source, "group_id", None)
    room_id = getattr(event_source, "room_id", None)

    try:
        if source_type == "group" and group_id:
            profile = line_bot_api.get_group_member_profile(group_id, user_id)
            return profile.display_name

        if source_type == "room" and room_id:
            profile = line_bot_api.get_room_member_profile(room_id, user_id)
            return profile.display_name

        profile = line_bot_api.get_profile(user_id)
        return profile.display_name
    except LineBotApiError:
        return format_user_id(user_id)


def get_bot_user_id():
    global BOT_USER_ID
    if BOT_USER_ID:
        return BOT_USER_ID

    try:
        bot_info = line_bot_api.get_bot_info()
        BOT_USER_ID = getattr(bot_info, "user_id", None)
    except LineBotApiError:
        BOT_USER_ID = None

    return BOT_USER_ID


def list_group_member_ids(group_id):
    member_ids = []
    start = None

    while True:
        response = line_bot_api.get_group_member_ids(group_id, start)
        member_ids.extend(getattr(response, "member_ids", []) or [])
        start = getattr(response, "next", None)
        if not start:
            break

    return member_ids


def list_room_member_ids(room_id):
    member_ids = []
    start = None

    while True:
        response = line_bot_api.get_room_member_ids(room_id, start)
        member_ids.extend(getattr(response, "member_ids", []) or [])
        start = getattr(response, "next", None)
        if not start:
            break

    return member_ids


def get_chat_participant_user_ids(event_source, chat_id=None):
    participant_sources = get_chat_participant_sources(event_source, chat_id)
    return participant_sources["merged_member_ids"]


def get_chat_participant_sources(event_source, chat_id=None):
    source_type = getattr(event_source, "type", None)
    group_id = getattr(event_source, "group_id", None)
    room_id = getattr(event_source, "room_id", None)

    api_member_ids = []
    api_error_message = None

    try:
        if source_type == "group" and group_id:
            api_member_ids = list_group_member_ids(group_id)
        elif source_type == "room" and room_id:
            api_member_ids = list_room_member_ids(room_id)
    except LineBotApiError as exc:
        api_member_ids = []
        status_code = getattr(exc, "status_code", None)
        error_message = getattr(exc, "message", str(exc))
        if status_code is not None:
            api_error_message = f"HTTP {status_code}: {error_message}"
        else:
            api_error_message = str(error_message)

    merged_member_ids = []
    seen = set()
    for user_id in api_member_ids:
        if not user_id or user_id in seen:
            continue
        merged_member_ids.append(user_id)
        seen.add(user_id)

    if merged_member_ids:
        return {
            "api_member_ids": api_member_ids,
            "merged_member_ids": merged_member_ids,
            "api_error_message": api_error_message,
        }

    if source_type in {"group", "room"}:
        return {
            "api_member_ids": api_member_ids,
            "merged_member_ids": [],
            "api_error_message": api_error_message,
        }

    user_id = getattr(event_source, "user_id", None)
    fallback_member_ids = [user_id] if user_id else []
    return {
        "api_member_ids": api_member_ids,
        "merged_member_ids": fallback_member_ids,
        "api_error_message": api_error_message,
    }


def normalize_scope(scope_text, default_scope):
    if not scope_text:
        return default_scope

    scope_map = {
        "日": "日",
        "天": "日",
        "周": "周",
        "週": "周",
        "月": "月",
        "年": "年",
        "全部": "全部",
        "all": "全部",
        "ALL": "全部",
    }

    normalized = scope_map.get(scope_text)
    if not normalized:
        raise ValueError("範圍只能填：日、周、月、年、全部")

    return normalized


def parse_range_spec(range_parts, default_scope):
    if not range_parts:
        scope = normalize_scope(None, default_scope)
        return {"type": "scope", "scope": scope, "label": scope}

    if len(range_parts) == 1:
        token = range_parts[0]

        date_match = re.fullmatch(r"(\d{1,2})/(\d{1,2})", token)
        if date_match:
            month = int(date_match.group(1))
            day = int(date_match.group(2))
            year = get_now().year
            try:
                datetime(year, month, day)
            except ValueError as exc:
                raise ValueError("日期格式請用 M/D 或 MM/DD，且需是有效日期") from exc

            return {
                "type": "date",
                "year": year,
                "month": month,
                "day": day,
                "label": f"{year}/{month:02d}/{day:02d}",
            }

        month_match = re.fullmatch(r"(\d{1,2})月", token)
        if month_match:
            month = int(month_match.group(1))
            if month < 1 or month > 12:
                raise ValueError("月份需介於 1 到 12")
            year = get_now().year
            return {
                "type": "month_year",
                "year": year,
                "month": month,
                "label": f"{year}年{month}月",
            }

        year_match = re.fullmatch(r"(\d{4})(?:年)?", token)
        if year_match:
            year = int(year_match.group(1))
            return {
                "type": "year_exact",
                "year": year,
                "label": f"{year}年",
            }

        scope = normalize_scope(token, default_scope)
        return {"type": "scope", "scope": scope, "label": scope}

    if len(range_parts) == 2:
        month = None
        year = None

        for token in range_parts:
            month_match = re.fullmatch(r"(\d{1,2})月", token)
            year_match = re.fullmatch(r"(\d{4})(?:年)?", token)

            if month_match:
                month = int(month_match.group(1))
                continue

            if year_match:
                year = int(year_match.group(1))
                continue

            raise ValueError(
                "範圍格式錯誤，可用：日/周/月/年/全部，或 2月、2025、2月 2025年、2/25"
            )

        if not month or not year:
            raise ValueError(
                "範圍格式錯誤，可用：日/周/月/年/全部，或 2月、2025、2月 2025年、2/25"
            )

        if month < 1 or month > 12:
            raise ValueError("月份需介於 1 到 12")

        return {
            "type": "month_year",
            "year": year,
            "month": month,
            "label": f"{year}年{month}月",
        }

    raise ValueError("範圍參數過多")


def parse_month_range_spec(range_parts):
    if not range_parts:
        raise ValueError(
            "範圍查詢格式：@記帳 範圍查詢 起始月到結束月（例如：2月到5月）"
        )

    joined = "".join(range_parts)
    match = re.fullmatch(r"(\d{1,2})月到(\d{1,2})月", joined)
    if not match:
        raise ValueError(
            "範圍查詢格式：@記帳 範圍查詢 起始月到結束月（例如：2月到5月）"
        )

    start_month = int(match.group(1))
    end_month = int(match.group(2))

    if start_month < 1 or start_month > 12 or end_month < 1 or end_month > 12:
        raise ValueError("月份需介於 1 到 12")

    if start_month > end_month:
        raise ValueError("起始月不可大於結束月")

    year = get_now().year
    return {
        "type": "month_range",
        "year": year,
        "start_month": start_month,
        "end_month": end_month,
        "label": f"{start_month}月到{end_month}月",
    }


def parse_settlement_month_spec(range_parts):
    format_error_message = "算錢格式：@記帳 算錢 [月份]，例如：@記帳 算錢 4月"

    now = get_now()
    if not range_parts:
        return {
            "type": "month_year",
            "year": now.year,
            "month": now.month,
            "label": f"{now.year}年{now.month}月",
        }

    if len(range_parts) == 1:
        token = range_parts[0]

        month_number_match = re.fullmatch(r"(\d{1,2})", token)
        if month_number_match:
            month = int(month_number_match.group(1))
            if month < 1 or month > 12:
                raise ValueError("月份需介於 1 到 12")
            return {
                "type": "month_year",
                "year": now.year,
                "month": month,
                "label": f"{now.year}年{month}月",
            }

        month_match = re.fullmatch(r"(\d{1,2})月", token)
        if month_match:
            month = int(month_match.group(1))
            if month < 1 or month > 12:
                raise ValueError("月份需介於 1 到 12")
            return {
                "type": "month_year",
                "year": now.year,
                "month": month,
                "label": f"{now.year}年{month}月",
            }

        year_month_match = re.fullmatch(r"(\d{4})年(\d{1,2})月", token)
        if year_month_match:
            year = int(year_month_match.group(1))
            month = int(year_month_match.group(2))
            if month < 1 or month > 12:
                raise ValueError("月份需介於 1 到 12")
            return {
                "type": "month_year",
                "year": year,
                "month": month,
                "label": f"{year}年{month}月",
            }

        raise ValueError(format_error_message)

    if len(range_parts) == 2:
        month = None
        year = None

        for token in range_parts:
            month_match = re.fullmatch(r"(\d{1,2})月", token)
            year_match = re.fullmatch(r"(\d{4})(?:年)?", token)

            if month_match:
                month = int(month_match.group(1))
                continue

            if year_match:
                year = int(year_match.group(1))
                continue

            raise ValueError(format_error_message)

        if year is None:
            year = now.year

        if month is None:
            raise ValueError(format_error_message)

        if month < 1 or month > 12:
            raise ValueError("月份需介於 1 到 12")

        return {
            "type": "month_year",
            "year": year,
            "month": month,
            "label": f"{year}年{month}月",
        }

    raise ValueError(format_error_message)


def get_scope_start_datetime(scope):
    now = get_now()

    if scope == "全部":
        return None
    if scope == "日":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if scope == "周":
        week_start = now - timedelta(days=now.weekday())
        return week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    if scope == "月":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if scope == "年":
        return now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)

    return None


def get_range_start_end(range_spec):
    range_type = range_spec["type"]

    if range_type == "scope":
        scope = range_spec["scope"]
        start = get_scope_start_datetime(scope)
        return start, None

    if range_type == "month_year":
        year = range_spec["year"]
        month = range_spec["month"]
        start = datetime(year, month, 1)
        if month == 12:
            end = datetime(year + 1, 1, 1)
        else:
            end = datetime(year, month + 1, 1)
        return start, end

    if range_type == "date":
        year = range_spec["year"]
        month = range_spec["month"]
        day = range_spec["day"]
        start = datetime(year, month, day)
        end = start + timedelta(days=1)
        return start, end

    if range_type == "month_range":
        year = range_spec["year"]
        start_month = range_spec["start_month"]
        end_month = range_spec["end_month"]
        start = datetime(year, start_month, 1)
        if end_month == 12:
            end = datetime(year + 1, 1, 1)
        else:
            end = datetime(year, end_month + 1, 1)
        return start, end

    if range_type == "year_exact":
        year = range_spec["year"]
        start = datetime(year, 1, 1)
        end = datetime(year + 1, 1, 1)
        return start, end

    return None, None


def get_balance_summary(chat_id, range_spec):
    range_start, range_end = get_range_start_end(range_spec)

    where_clause = "chat_id = ?"
    params = [chat_id]
    if range_start is not None:
        where_clause += " AND created_at >= ?"
        params.append(to_db_created_at(range_start))
    if range_end is not None:
        where_clause += " AND created_at < ?"
        params.append(to_db_created_at(range_end))

    total_expense = run_query(
        f"SELECT COALESCE(SUM(amount), 0) FROM records WHERE {where_clause} AND record_type = '支出'",
        params,
        fetch_mode="one",
    )[0]
    total_income = run_query(
        f"SELECT COALESCE(SUM(amount), 0) FROM records WHERE {where_clause} AND record_type = '收入'",
        params,
        fetch_mode="one",
    )[0]
    paid_by_user_rows = run_query(
        f"""
        SELECT user_id, COALESCE(SUM(amount), 0) AS paid
        FROM records
        WHERE {where_clause} AND record_type = '支出'
        GROUP BY user_id
        ORDER BY paid DESC
        """,
        params,
        fetch_mode="all",
    )

    return total_expense, total_income, paid_by_user_rows


def get_previous_month_window(range_spec):
    now = get_now()

    range_type = range_spec.get("type") if range_spec else None
    if range_type == "month_year":
        reference_year = range_spec["year"]
        reference_month = range_spec["month"]
    elif range_type == "date":
        reference_year = range_spec["year"]
        reference_month = range_spec["month"]
    elif range_type == "month_range":
        reference_year = range_spec["year"]
        reference_month = range_spec["start_month"]
    elif range_type == "year_exact":
        reference_year = range_spec["year"]
        reference_month = 1
    else:
        reference_year = now.year
        reference_month = now.month

    current_month_start = datetime(reference_year, reference_month, 1)
    if reference_month == 1:
        previous_month_start = datetime(reference_year - 1, 12, 1)
    else:
        previous_month_start = datetime(reference_year, reference_month - 1, 1)

    return previous_month_start, current_month_start


def get_balance_for_window(chat_id, range_start, range_end):
    where_clause = "chat_id = ?"
    params = [chat_id]

    if range_start is not None:
        where_clause += " AND created_at >= ?"
        params.append(to_db_created_at(range_start))
    if range_end is not None:
        where_clause += " AND created_at < ?"
        params.append(to_db_created_at(range_end))

    total_expense = run_query(
        f"SELECT COALESCE(SUM(amount), 0) FROM records WHERE {where_clause} AND record_type = '支出'",
        params,
        fetch_mode="one",
    )[0]
    total_income = run_query(
        f"SELECT COALESCE(SUM(amount), 0) FROM records WHERE {where_clause} AND record_type = '收入'",
        params,
        fetch_mode="one",
    )[0]

    return total_income - total_expense


def get_detailed_records(chat_id, range_spec, limit=30):
    range_start, range_end = get_range_start_end(range_spec)

    where_clause = "chat_id = ?"
    params = [chat_id]
    if range_start is not None:
        where_clause += " AND created_at >= ?"
        params.append(to_db_created_at(range_start))
    if range_end is not None:
        where_clause += " AND created_at < ?"
        params.append(to_db_created_at(range_end))

    return run_query(
        f"""
        SELECT
            r.id,
            r.created_at,
            r.item,
            r.amount,
            r.user_id,
            (
                SELECT COUNT(*)
                FROM records AS seq
                WHERE seq.chat_id = r.chat_id
                  AND (
                      seq.created_at > r.created_at
                      OR (seq.created_at = r.created_at AND seq.id >= r.id)
                  )
            ) AS display_id
        FROM records AS r
        WHERE {where_clause}
        ORDER BY r.created_at DESC, r.id DESC
        LIMIT ?
        """,
        [*params, limit],
        fetch_mode="all",
    )


def build_summary_text(chat_id, event_source, range_spec):
    total_expense, total_income, paid_by_user_rows = get_balance_summary(
        chat_id, range_spec
    )
    previous_month_start, current_month_start = get_previous_month_window(range_spec)
    previous_month_balance = get_balance_for_window(
        chat_id, previous_month_start, current_month_start
    )
    balance = previous_month_balance + total_income - total_expense

    range_type = range_spec.get("type") if range_spec else None
    scope = range_spec.get("scope") if range_type == "scope" else None
    is_monthly_view = range_type == "month_year" or scope == "月"
    income_label = "本月總收入" if is_monthly_view else "總收入"
    expense_label = "本月總支出" if is_monthly_view else "總支出"

    lines = [
        f"記帳總覽（{range_spec['label']}）",
        f"前月總結餘：{previous_month_balance}",
        f"{income_label}：{total_income}",
        f"{expense_label}：{total_expense}",
        f"目前餘額：{balance}",
        "",
        "當月各付了多少：",
    ]

    if not paid_by_user_rows:
        lines.append("尚無支出紀錄")
    else:
        for index, row in enumerate(paid_by_user_rows, start=1):
            user_id, paid = row
            display_name = resolve_display_name(event_source, user_id)
            lines.append(f"{index}. {display_name}：{paid}")

    return "\n".join(lines)


def get_expense_by_user(chat_id, range_spec):
    range_start, range_end = get_range_start_end(range_spec)

    where_clause = "chat_id = ?"
    params = [chat_id]
    if range_start is not None:
        where_clause += " AND created_at >= ?"
        params.append(to_db_created_at(range_start))
    if range_end is not None:
        where_clause += " AND created_at < ?"
        params.append(to_db_created_at(range_end))

    return run_query(
        f"""
        SELECT user_id, COALESCE(SUM(amount), 0) AS paid
        FROM records
        WHERE {where_clause} AND record_type = '支出'
        GROUP BY user_id
        ORDER BY paid DESC
        """,
        params,
        fetch_mode="all",
    )


def get_settlement_display_name(event_source, user_id):
    user_id_text = str(user_id)
    if user_id_text.startswith("__manual_"):
        return user_id_text.replace("__manual_", "", 1)
    if user_id_text.startswith("__untracked_"):
        return f"未記帳成員{user_id_text.split('_')[-1]}"
    return resolve_display_name(event_source, user_id)


def allocate_proportional_amounts(total_amount, weighted_ids, weight_map):
    if total_amount <= 0 or not weighted_ids:
        return {user_id: 0 for user_id in weighted_ids}

    total_weight = sum(max(weight_map.get(user_id, 0), 0) for user_id in weighted_ids)
    if total_weight <= 0:
        return {user_id: 0 for user_id in weighted_ids}

    allocations = {}
    fraction_rows = []
    allocated = 0

    for user_id in weighted_ids:
        weight = max(weight_map.get(user_id, 0), 0)
        raw_value = total_amount * weight / total_weight
        base_value = int(raw_value)
        allocations[user_id] = base_value
        allocated += base_value
        fraction_rows.append((raw_value - base_value, user_id))

    remaining = total_amount - allocated
    for _, user_id in sorted(fraction_rows, key=lambda row: row[0], reverse=True):
        if remaining <= 0:
            break
        allocations[user_id] += 1
        remaining -= 1

    return allocations


def build_settlement_text(chat_id, event_source, range_spec):
    participant_count_input = 3
    paid_by_user_rows = get_expense_by_user(chat_id, range_spec)
    paid_map = {user_id: paid for user_id, paid in paid_by_user_rows}
    total_expense = sum(paid_map.values())

    settlement_label = range_spec["label"]
    if range_spec.get("type") == "scope" and range_spec.get("scope") == "月":
        now = get_now()
        settlement_label = f"{now.year}年{now.month}月"

    if total_expense == 0:
        return "\n".join(
            [
                f"算錢結果（{settlement_label}）",
                "該範圍尚無支出紀錄，無需算錢",
            ]
        )

    _, total_income, _ = get_balance_summary(chat_id, range_spec)
    previous_month_start, current_month_start = get_previous_month_window(range_spec)
    previous_month_balance = get_balance_for_window(
        chat_id, previous_month_start, current_month_start
    )
    available_bank_funds = max(previous_month_balance + total_income, 0)
    bank_reimbursement_total = min(total_expense, available_bank_funds)
    member_extra_total = total_expense - bank_reimbursement_total

    participant_sources = get_chat_participant_sources(event_source, chat_id)
    participant_user_ids = participant_sources["merged_member_ids"]
    api_error_message = participant_sources.get("api_error_message")
    bot_user_id = get_bot_user_id()
    participant_user_ids = [
        user_id
        for user_id in participant_user_ids
        if user_id and user_id != bot_user_id
    ]

    participant_display_names = {
        resolve_display_name(event_source, user_id).strip()
        for user_id in participant_user_ids
        if user_id
    }

    missing_payer_ids = []
    for user_id in paid_map.keys():
        if not user_id or user_id == bot_user_id or user_id in participant_user_ids:
            continue

        payer_display_name = get_settlement_display_name(event_source, user_id).strip()
        if payer_display_name and payer_display_name in participant_display_names:
            continue

        missing_payer_ids.append(user_id)
        if payer_display_name:
            participant_display_names.add(payer_display_name)

    participant_user_ids.extend(missing_payer_ids)

    if not participant_user_ids:
        lines = [f"算錢結果（{range_spec['label']}）"]
        lines.append("目前無法取得群組成員名單，請稍後再試")
        if api_error_message:
            lines.append(f"API 錯誤：{api_error_message}")
        lines.append("請確認：LINE 官方帳號已加入該群組，且群組成員可被 API 讀取")
        return "\n".join(lines)

    participant_rows = [
        (user_id, paid_map.get(user_id, 0)) for user_id in participant_user_ids
    ]

    existing_participant_count = len(participant_rows)
    effective_participant_count = max(
        participant_count_input, existing_participant_count
    )
    missing_count = effective_participant_count - existing_participant_count
    manual_member_names = get_manual_members(chat_id)
    used_display_names = {
        get_settlement_display_name(event_source, user_id).strip()
        for user_id, _ in participant_rows
    }
    manual_index = 0
    next_untracked_index = 1
    for _ in range(missing_count):
        added = False
        while manual_index < len(manual_member_names):
            candidate_name = manual_member_names[manual_index].strip()
            manual_index += 1
            if not candidate_name or candidate_name in used_display_names:
                continue
            participant_rows.append((f"__manual_{candidate_name}", 0))
            used_display_names.add(candidate_name)
            added = True
            break

        if not added:
            while True:
                placeholder_name = f"未記帳成員{next_untracked_index}"
                placeholder_id = f"__untracked_{next_untracked_index}"
                next_untracked_index += 1
                if placeholder_name in used_display_names:
                    continue
                participant_rows.append((placeholder_id, 0))
                used_display_names.add(placeholder_name)
                break

    lines = [f"算錢結果（{settlement_label}）"]
    if not participant_rows:
        lines.append("該範圍尚無支出紀錄，無需算錢")
        return "\n".join(lines)

    if sum(amount for _, amount in participant_rows) == 0:
        lines.append("該範圍尚無支出紀錄，無需算錢")
        return "\n".join(lines)

    participant_count = len(participant_rows)
    per_person_extra = member_extra_total / participant_count

    participant_ids = [user_id for user_id, _ in participant_rows]
    participant_name_to_id = {}
    for user_id in participant_ids:
        display_name = get_settlement_display_name(event_source, user_id).strip()
        if display_name:
            participant_name_to_id[display_name] = user_id

    bank_withdraw_map = allocate_proportional_amounts(
        bank_reimbursement_total,
        participant_ids,
        {user_id: paid for user_id, paid in participant_rows},
    )
    after_bank_paid_map = {
        user_id: paid_map.get(user_id, 0) - bank_withdraw_map.get(user_id, 0)
        for user_id in participant_ids
    }

    creditors = []
    debtors = []
    base_share = member_extra_total // participant_count
    share_remainder = member_extra_total % participant_count
    target_share_map = {}
    for index, user_id in enumerate(participant_ids):
        target_share_map[user_id] = base_share + (1 if index < share_remainder else 0)

    payment_rows = get_settlement_payments(chat_id, range_spec)
    payment_adjust_map = {user_id: 0 for user_id in participant_ids}
    for from_user_id, to_name, amount in payment_rows:
        normalized_to_name = normalize_manual_member_name(to_name)
        to_user_id = participant_name_to_id.get(normalized_to_name)

        if from_user_id in payment_adjust_map and to_user_id in payment_adjust_map:
            payment_adjust_map[from_user_id] += amount
            payment_adjust_map[to_user_id] -= amount

    for user_id in participant_ids:
        delta = (
            after_bank_paid_map[user_id]
            - target_share_map[user_id]
            + payment_adjust_map.get(user_id, 0)
        )
        if delta > 0:
            creditors.append([user_id, delta])
        elif delta < 0:
            debtors.append([user_id, -delta])

    transfers = []
    creditor_index = 0
    debtor_index = 0
    while creditor_index < len(creditors) and debtor_index < len(debtors):
        creditor_user_id, creditor_need = creditors[creditor_index]
        debtor_user_id, debtor_need = debtors[debtor_index]

        amount = min(creditor_need, debtor_need)
        if amount > 0:
            transfers.append((debtor_user_id, creditor_user_id, amount))

        creditor_need -= amount
        debtor_need -= amount
        creditors[creditor_index][1] = creditor_need
        debtors[debtor_index][1] = debtor_need

        if creditor_need == 0:
            creditor_index += 1
        if debtor_need == 0:
            debtor_index += 1

    lines.append(f"前月結餘：{previous_month_balance}")
    lines.append(f"本月收入：{total_income}")
    lines.append(f"本期總支出：{total_expense}")
    lines.append(f"每人最終須補差額：{int(round(per_person_extra))}")
    lines.append("")
    lines.append("付款明細（代墊）：")

    for index, (user_id, paid) in enumerate(participant_rows, start=1):
        display_name = get_settlement_display_name(event_source, user_id)
        lines.append(f"{index}. {display_name} 已付：{paid}")

    lines.append("")
    lines.append("可從銀行提領：")
    for index, (user_id, _) in enumerate(participant_rows, start=1):
        display_name = get_settlement_display_name(event_source, user_id)
        lines.append(f"{index}. {display_name}：{bank_withdraw_map.get(user_id, 0)}")

    lines.append("")
    lines.append("轉帳建議：")
    if member_extra_total <= 0:
        lines.append("本期由銀行資金可完全支應，無需彼此補款")
    elif not transfers:
        lines.append("目前無需互相轉帳")
    else:
        for index, (from_user_id, to_user_id, amount) in enumerate(transfers, start=1):
            from_name = get_settlement_display_name(event_source, from_user_id)
            to_name = get_settlement_display_name(event_source, to_user_id)
            lines.append(f"{index}. {from_name} 要給 {to_name}：{amount}")

    if payment_rows:
        lines.append("")
        lines.append("本月已登記補款：")
        for index, (from_user_id, to_name, amount) in enumerate(payment_rows, start=1):
            from_name = get_settlement_display_name(event_source, from_user_id)
            lines.append(f"{index}. {from_name} 已給 {to_name}：{amount}")

    return "\n".join(lines)


def build_member_check_data(chat_id, event_source):
    participant_sources = get_chat_participant_sources(event_source, chat_id)
    api_member_ids = participant_sources["api_member_ids"]
    merged_member_ids = participant_sources["merged_member_ids"]
    api_error_message = participant_sources.get("api_error_message")
    paid_by_user_rows = get_expense_by_user(
        chat_id,
        parse_range_spec([], "月"),
    )
    paid_user_ids = [row[0] for row in paid_by_user_rows]

    bot_user_id = get_bot_user_id()
    filtered_member_ids = [
        user_id for user_id in merged_member_ids if user_id and user_id != bot_user_id
    ]
    supplemented_user_ids = [
        user_id
        for user_id in paid_user_ids
        if user_id and user_id != bot_user_id and user_id not in filtered_member_ids
    ]

    settlement_members = []
    seen_display_names = set()

    def append_member(user_id, source):
        display_name = get_settlement_display_name(event_source, user_id).strip()
        if not display_name or display_name in seen_display_names:
            return False
        settlement_members.append(
            {
                "user_id": user_id,
                "display_name": display_name,
                "source": source,
            }
        )
        seen_display_names.add(display_name)
        return True

    for user_id in filtered_member_ids:
        append_member(user_id, "api")

    supplemented_count = 0
    for user_id in supplemented_user_ids:
        if append_member(user_id, "record"):
            supplemented_count += 1

    manual_added_count = 0
    for member_name in get_manual_members(chat_id):
        if append_member(f"__manual_{member_name}", "manual"):
            manual_added_count += 1

    return {
        "api_member_ids": api_member_ids,
        "api_error_message": api_error_message,
        "settlement_members": settlement_members,
        "supplemented_count": supplemented_count,
        "manual_added_count": manual_added_count,
    }


def build_member_check_text(chat_id, event_source):
    member_check_data = build_member_check_data(chat_id, event_source)
    api_member_ids = member_check_data["api_member_ids"]
    api_error_message = member_check_data.get("api_error_message")
    settlement_members = member_check_data["settlement_members"]
    supplemented_count = member_check_data["supplemented_count"]
    manual_added_count = member_check_data["manual_added_count"]

    lines = ["成員檢查"]
    lines.append(f"API 成員數：{len(api_member_ids)}")
    lines.append(f"算錢採用成員數（排除機器人）：{len(settlement_members)}")
    lines.append(f"本期記帳補入成員數：{supplemented_count}")
    lines.append(f"手動補登成員數：{manual_added_count}")
    if api_error_message:
        lines.append(f"API 錯誤：{api_error_message}")
        lines.append("提示：請確認 LINE 官方帳號已加入群組，且群組成員可被 API 讀取")

    if not settlement_members:
        lines.append("目前沒有可用成員名單")
        return "\n".join(lines)

    lines.append("")
    lines.append("採用名單：")
    for index, member in enumerate(settlement_members, start=1):
        suffix = ""
        if member["source"] == "manual":
            suffix = "（補登）"
        elif member["source"] == "record":
            suffix = "（本期記帳）"
        lines.append(f"{index}. {member['display_name']}{suffix}")

    return "\n".join(lines)


def build_detail_text(chat_id, event_source, range_spec):
    rows = get_detailed_records(chat_id, range_spec)
    scope = range_spec["scope"] if range_spec["type"] == "scope" else "全部"

    lines = [
        f"記帳詳細（{range_spec['label']}）",
    ]

    if not rows:
        LAST_DETAIL_VIEW.pop(chat_id, None)
        lines.append("該範圍尚無紀錄")
        return "\n".join(lines)

    LAST_DETAIL_VIEW[chat_id] = [row[0] for row in rows]

    use_month_day_format = scope in {"日", "周", "月"}
    shown_year = None

    for index, (_, created_at, item, amount, user_id, _) in enumerate(rows, start=1):
        created_at_dt = from_db_created_at(created_at)

        if use_month_day_format:
            current_year = created_at_dt.year
            if shown_year != current_year:
                lines.append(f"【{current_year}】")
                shown_year = current_year
            created_at_text = created_at_dt.strftime("%m/%d")
        else:
            created_at_text = created_at_dt.strftime("%Y/%m/%d")

        display_name = resolve_display_name(event_source, user_id)
        lines.append(f"ID：{index}　")
        lines.append(f"日期：{created_at_text}")
        lines.append(f"項目：{item}")
        lines.append(f"金額：{amount}")
        lines.append(f"登記人：{display_name}")
        if index < len(rows):
            lines.append("-")

    return "\n".join(lines)


def parse_query_command(text):
    parts = [part for part in re.split(r"[\s,，]+", text.strip()) if part]
    if not parts or parts[0] != "@記帳":
        return None

    if len(parts) >= 2 and parts[1] in {"查詢", "總覽", "餘額", "查餘額"}:
        range_spec = parse_range_spec(parts[2:], "月")
        return "summary", range_spec

    if len(parts) >= 2 and parts[1] in {"算錢", "分帳"}:
        range_spec = parse_settlement_month_spec(parts[2:])
        return "settlement", range_spec

    if len(parts) >= 2 and parts[1] in {"成員檢查", "成員"}:
        return "member_check", None

    if len(parts) >= 2 and parts[1] in {"範圍查詢"}:
        range_spec = parse_month_range_spec(parts[2:])
        return "summary", range_spec

    if len(parts) >= 2 and parts[1] in {"詳細查詢", "明細", "詳細"}:
        range_spec = parse_range_spec(parts[2:], "月")
        return "detail", range_spec

    if len(parts) >= 2 and parts[1] in {"狀態", "status", "STATUS"}:
        return "status", None

    return None


init_db()


@app.route("/callback", methods=["POST"])
def callback():
    # get X-Line-Signature header value
    signature = request.headers["X-Line-Signature"]

    # get request body as text
    body = request.get_data(as_text=True)
    app.logger.info("Request body: " + body)

    # handle webhook body
    try:
        line_handler.handle(body, signature)
    except InvalidSignatureError:
        print(
            "Invalid signature. Please check your channel access token/channel secret."
        )
        abort(400)

    return "OK"


@line_handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    incoming_text = event.message.text.strip()
    chat_id = get_chat_id(event.source)
    sender_user_id = getattr(event.source, "user_id", "unknown")

    confirm_keywords = {"確定", "確認", "ok", "OK", "Ok", "好"}
    pending_delete = PENDING_DELETE.get(chat_id)
    if pending_delete is not None:
        if incoming_text in confirm_keywords:
            deleted_count = delete_record_by_id(chat_id, pending_delete["real_id"])
            if deleted_count == 0:
                reply_text = f"找不到可刪除的紀錄 ID：{pending_delete['display_id']}"
            else:
                reply_text = f"已刪除紀錄 ID：{pending_delete['display_id']}"

            PENDING_DELETE.pop(chat_id, None)
            line_bot_api.reply_message(
                event.reply_token, TextSendMessage(text=reply_text)
            )
            return

        PENDING_DELETE.pop(chat_id, None)

    if not incoming_text.startswith("@記帳"):
        return

    if incoming_text in {"@記帳", "@記帳格式", "@記帳 格式"}:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=HELP_TEXT),
        )
        return

    try:
        add_member_name = parse_add_member_command(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(err)))
        return

    if add_member_name is not None:
        saved_name = save_manual_member(chat_id, add_member_name)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"已新增成員：{saved_name}"),
        )
        return

    try:
        delete_member_index = parse_delete_member_command(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(err)))
        return

    if delete_member_index is not None:
        member_check_data = build_member_check_data(chat_id, event.source)
        settlement_members = member_check_data["settlement_members"]

        if delete_member_index > len(settlement_members):
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"找不到成員 ID：{delete_member_index}"),
            )
            return

        target_member = settlement_members[delete_member_index - 1]
        target_name = target_member["display_name"]
        target_source = target_member.get("source")

        if target_source != "manual":
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(
                    text=(
                        f"成員 ID：{delete_member_index}（{target_name}）不是手動補登成員，"
                        "無法刪除"
                    )
                ),
            )
            return

        deleted_count = delete_manual_member(chat_id, target_name)
        if deleted_count == 0:
            reply_text = f"找不到可刪除的補登成員：{target_name}"
        else:
            reply_text = f"已刪除補登成員：{target_name}"

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    try:
        settlement_payment = parse_settlement_payment_command(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(err)))
        return

    if settlement_payment is not None:
        to_name, amount = settlement_payment
        save_manual_member(chat_id, to_name)
        save_settlement_payment(
            chat_id=chat_id,
            from_user_id=sender_user_id,
            to_name=to_name,
            amount=amount,
            created_at=get_now(),
        )

        from_name = resolve_display_name(event.source, sender_user_id)
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"已記錄補款：{from_name} 給 {to_name} {amount}"),
        )
        return

    try:
        delete_record_id = parse_delete_command(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(err)))
        return

    if delete_record_id is not None:
        display_record_id = delete_record_id
        record = get_record_by_last_detail_id(chat_id, display_record_id)
        if not record:
            record = get_record_by_display_id(chat_id, display_record_id)
        if not record:
            reply_text = f"找不到可刪除的紀錄 ID：{display_record_id}"
        else:
            PENDING_DELETE[chat_id] = {
                "real_id": record[0],
                "display_id": display_record_id,
            }
            reply_text = format_record_detail_for_delete(display_record_id, record)
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    try:
        modify_command = parse_modify_command(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(err)))
        return

    if modify_command:
        display_record_id = modify_command["record_id"]
        old_record = get_record_by_last_detail_id(chat_id, display_record_id)
        if not old_record:
            old_record = get_record_by_display_id(chat_id, display_record_id)
        if not old_record:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"找不到可修改的紀錄 ID：{display_record_id}"),
            )
            return

        real_record_id, old_item, old_amount, old_record_type, old_created_at = (
            old_record
        )
        item = (
            modify_command["item"] if modify_command["item"] is not None else old_item
        )
        amount = (
            modify_command["amount"]
            if modify_command["amount"] is not None
            else old_amount
        )
        record_type = modify_command["record_type"] or old_record_type
        record_datetime = (
            modify_command["record_datetime"]
            if modify_command["record_datetime"] is not None
            else from_db_created_at(old_created_at)
        )

        updated_count = update_record_by_id(
            chat_id=chat_id,
            record_id=real_record_id,
            item=item,
            amount=amount,
            record_type=record_type,
            created_at=record_datetime,
        )
        if updated_count == 0:
            reply_text = f"找不到可修改的紀錄 ID：{display_record_id}"
        else:
            updated_date_text = record_datetime.strftime("%Y/%m/%d")
            reply_text = (
                f"已修改紀錄 ID：{display_record_id}\n"
                f"類型：{record_type}\n"
                f"項目：{item}\n"
                f"金額：{amount}\n"
                f"日期：{updated_date_text}"
            )
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    try:
        query_command = parse_query_command(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=f"{err}\n可用範圍例子：2/25、2月、2025、2月到5月"),
        )
        return

    if query_command:
        command_type, range_spec = query_command
        if command_type == "status":
            reply_text = build_storage_status_text()
        elif command_type == "member_check":
            reply_text = build_member_check_text(chat_id, event.source)
        elif command_type == "settlement":
            reply_text = build_settlement_text(chat_id, event.source, range_spec)
        elif command_type == "summary":
            reply_text = build_summary_text(chat_id, event.source, range_spec)
        else:
            reply_text = build_detail_text(chat_id, event.source, range_spec)

        reply_text = with_storage_warning(reply_text)

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    try:
        parsed = parse_record_message(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(err)))
        return

    if not parsed:
        return

    for item, amount, record_type, record_datetime, target_member_name in parsed:
        record_user_id = sender_user_id
        if target_member_name:
            save_manual_member(chat_id, target_member_name)
            record_user_id = f"__manual_{target_member_name}"

        save_record(
            user_id=record_user_id,
            chat_id=chat_id,
            item=item,
            amount=amount,
            record_type=record_type,
            created_at=record_datetime,
        )

    if len(parsed) == 1:
        item, amount, record_type, _, target_member_name = parsed[0]
        reply_text = f"記帳成功\n類型：{record_type}\n項目：{item}\n金額：{amount}"
        if target_member_name:
            reply_text += f"\n補登對象：{target_member_name}"
    else:
        summary_lines = [f"記帳成功（共{len(parsed)}筆）"]
        for index, (item, amount, record_type, _, target_member_name) in enumerate(
            parsed, start=1
        ):
            target_text = (
                f"（補登：{target_member_name}）" if target_member_name else ""
            )
            summary_lines.append(f"{index}. {record_type} {item} {amount}{target_text}")
        reply_text = "\n".join(summary_lines)

    reply_text = with_storage_warning(reply_text)

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))


if __name__ == "__main__":
    debug = True
    app.run(debug=debug)
