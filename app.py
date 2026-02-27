import sqlite3
import re
from datetime import datetime, timedelta
import os

from flask import Flask, request, abort

from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError, LineBotApiError
from linebot.models import (
    MessageEvent,
    TextMessage,
    TextSendMessage,
)

app = Flask(__name__)


def resolve_db_path():
    env_db_path = os.getenv("DB_PATH")
    if env_db_path:
        return env_db_path

    if os.getenv("VERCEL") == "1" or os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        return "/tmp/bookkeeping.db"

    return "bookkeeping.db"


DB_PATH = resolve_db_path()
DATABASE_URL = os.getenv("DATABASE_URL")
IS_POSTGRES = bool(DATABASE_URL)
psycopg = None
PENDING_DELETE = {}

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

        now = datetime.now()
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
        "修改",
        "查詢",
        "總覽",
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
                f"第{line_number}行格式錯誤，請用：@記帳 項目 金額 [支出或收入] [MM/DD]"
            )

        payload = line[len("@記帳") :].strip()
        fields = [field for field in re.split(r"[\s,，]+", payload) if field]
        if len(fields) < 2 or len(fields) > 4:
            raise ValueError(
                f"第{line_number}行格式錯誤，請用：@記帳 項目 金額 [支出或收入] [MM/DD]（分隔可用空白/，/,）"
            )

        item = fields[0]
        amount_text = fields[1]
        option_1 = fields[2] if len(fields) >= 3 else None
        option_2 = fields[3] if len(fields) >= 4 else None

        if item in command_keywords:
            raise ValueError(
                "多行輸入僅支援記帳格式：@記帳 項目 金額 [支出或收入] [MM/DD]（分隔可用空白/，/,）"
            )

        if not item:
            raise ValueError(f"第{line_number}行格式錯誤，項目不可空白")

        record_type = "支出"
        record_datetime = datetime.now().replace(microsecond=0)

        if option_1 and not option_2:
            try:
                record_type = normalize_record_type(option_1)
            except ValueError:
                record_datetime = parse_mmdd_date(option_1)

        if option_1 and option_2:
            record_type = normalize_record_type(option_1)
            record_datetime = parse_mmdd_date(option_2)

        try:
            amount = int(amount_text)
            if amount <= 0:
                raise ValueError
        except ValueError as exc:
            raise ValueError(
                f"第{line_number}行金額錯誤，金額必須是正整數，例如：@記帳 銀行，50000 收入"
            ) from exc

        parsed_records.append((item, amount, record_type, record_datetime))

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

    now = datetime.now()
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
            year = datetime.now().year
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
            year = datetime.now().year
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

    year = datetime.now().year
    return {
        "type": "month_range",
        "year": year,
        "start_month": start_month,
        "end_month": end_month,
        "label": f"{start_month}月到{end_month}月",
    }


def get_scope_start_datetime(scope):
    now = datetime.now()

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
    balance = total_income - total_expense

    lines = [
        f"記帳總覽（{range_spec['label']}）",
        f"總收入：{total_income}",
        f"總支出：{total_expense}",
        f"目前餘額：{balance}",
        "",
        "誰目前付了多少：",
    ]

    if not paid_by_user_rows:
        lines.append("尚無支出紀錄")
    else:
        for index, row in enumerate(paid_by_user_rows, start=1):
            user_id, paid = row
            display_name = resolve_display_name(event_source, user_id)
            lines.append(f"{index}. {display_name}：{paid}")

    return "\n".join(lines)


def build_detail_text(chat_id, event_source, range_spec):
    rows = get_detailed_records(chat_id, range_spec)
    scope = range_spec["scope"] if range_spec["type"] == "scope" else "全部"

    lines = [
        f"記帳詳細（{range_spec['label']}）",
    ]

    if not rows:
        lines.append("該範圍尚無紀錄")
        return "\n".join(lines)

    use_month_day_format = scope in {"日", "周", "月"}
    shown_year = None

    for _, created_at, item, amount, user_id, display_id in rows:
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
        lines.append(f"日期：{created_at_text}　ID：{display_id}　")
        lines.append(f"項目：{item}")
        lines.append(f"金額：{amount}　登記人：{display_name}")
        lines.append("-")

    return "\n".join(lines)


def parse_query_command(text):
    parts = [part for part in re.split(r"[\s,，]+", text.strip()) if part]
    if not parts or parts[0] != "@記帳":
        return None

    if len(parts) >= 2 and parts[1] in {"查詢", "總覽", "餘額", "查餘額"}:
        range_spec = parse_range_spec(parts[2:], "全部")
        return "summary", range_spec

    if len(parts) >= 2 and parts[1] in {"範圍查詢"}:
        range_spec = parse_month_range_spec(parts[2:])
        return "summary", range_spec

    if len(parts) >= 2 and parts[1] in {"詳細查詢", "明細", "詳細"}:
        range_spec = parse_range_spec(parts[2:], "月")
        return "detail", range_spec

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
            TextSendMessage(
                text="請用以下格式：\n記帳：\n@記帳 項目 金額 [收支] [日期]\n（欄位分隔支援：空白 / ， / ,；支援多行輸入）\n-\n刪除：\n@記帳 刪除 ID\n（欄位分隔支援：空白 / ， / ,）\n-\n修改：\n@記帳 修改 ID 項目 金額 [收支] [日期]\n@記帳 修改 ID [收支或日期]\n@記帳 修改 ID [項目|金額|日期|收支] 值\n可一次改多欄位：@記帳 修改 ID 項目 A 金額 1000 日期 2/20 收支 收入\n（欄位分隔支援：空白 / ， / ,）\n-\n查詢：\n@記帳 查詢 [範圍]\n（欄位分隔支援：空白 / ， / ,）\n-\n範圍查詢：\n@記帳 範圍查詢 起始月到結束月\n（欄位分隔支援：空白 / ， / ,）\n-\n詳細查詢：\n@記帳 詳細查詢 [範圍]\n（欄位分隔支援：空白 / ， / ,）\n-\n範圍選項：日 / 周 / 月 / 年 / 全部\n可用範圍例子：2/25、2月、2025、2月到5月\n查詢預設範圍：全部\n詳細查詢預設範圍：月\n記帳預設：支出、當天"
            ),
        )
        return

    try:
        delete_record_id = parse_delete_command(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(err)))
        return

    if delete_record_id is not None:
        display_record_id = delete_record_id
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
        if command_type == "summary":
            reply_text = build_summary_text(chat_id, event.source, range_spec)
        else:
            reply_text = build_detail_text(chat_id, event.source, range_spec)

        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))
        return

    try:
        parsed = parse_record_message(incoming_text)
    except ValueError as err:
        line_bot_api.reply_message(event.reply_token, TextSendMessage(text=str(err)))
        return

    if not parsed:
        return

    user_id = getattr(event.source, "user_id", "unknown")
    for item, amount, record_type, record_datetime in parsed:
        save_record(
            user_id=user_id,
            chat_id=chat_id,
            item=item,
            amount=amount,
            record_type=record_type,
            created_at=record_datetime,
        )

    if len(parsed) == 1:
        item, amount, record_type, _ = parsed[0]
        reply_text = f"記帳成功\n類型：{record_type}\n項目：{item}\n金額：{amount}"
    else:
        summary_lines = [f"記帳成功（共{len(parsed)}筆）"]
        for index, (item, amount, record_type, _) in enumerate(parsed, start=1):
            summary_lines.append(f"{index}. {record_type} {item} {amount}")
        reply_text = "\n".join(summary_lines)

    line_bot_api.reply_message(event.reply_token, TextSendMessage(text=reply_text))


if __name__ == "__main__":
    debug = True
    app.run(debug=debug)
