"""金蝶 → PostgreSQL 分块导出工具（MCP 内置）。

设计要点
--------
- 分块（plan / chunk / finish），避免单次工具调用长时间阻塞导致客户端超时。
- 只读访问金蝶：仅复用 ``_post("query", ...)``（ExecuteBillQuery），绝不调用
  保存/提交/审核/下推/删除等写接口。
- 分页在服务端内部进行，每页默认 2000 行（工具入参的 100 行上限只作用于
  ``kingdee_query_bills``，内部 ``ExecuteBillQuery`` 不受该限制）。
- 落库默认全部 ``text``（金蝶 WebAPI 返回的都是字符串，元数据也不提供 SQL 类型），
  需要类型时用 ``types`` 显式声明，避免猜测导致精度丢失。
- DSN 默认只允许本机，防止误写生产库；远程需显式 ``allow_remote=true``。

工具
----
- ``kingdee_export_plan``   解析字段、探测无效字段、生成建表 DDL（不落库）
- ``kingdee_export_chunk``  取一页并写入（首块负责建表），返回游标与进度
- ``kingdee_export_finish`` 校验行数/主键唯一性，写入表与列注释并结束会话
"""

from __future__ import annotations

import asyncio
import datetime as _dt
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field

MAX_CHUNK = 2000
DEFAULT_DSN = "postgresql://apt:apt@127.0.0.1:5432/data_platform"
LOCAL_HOSTS = {"", "localhost", "127.0.0.1", "::1", "/var/run/postgresql", "/tmp"}

# 仅在允许的 PG 类型白名单内做 CAST，防止 types 参数注入 SQL。
_TYPE_RE = re.compile(
    r"^(text|varchar(\(\d+\))?|numeric(\(\d+(,\d+)?\))?|int|integer|bigint|"
    r"date|timestamp|timestamptz|boolean)$",
    re.IGNORECASE,
)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# register() 注入的服务器内部依赖（_post/_rows/_fmt/_err 等）
_DEPS: Dict[str, Any] = {}
# 导出会话：session_id -> 状态。MCP server 为单进程，进程内字典足够。
_STATE: Dict[str, dict] = {}


# ─────────────────────────────────────────────
# 输入模型
# ─────────────────────────────────────────────

class ExportPlanInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    form_id: str = Field(..., description="金蝶单据类型，如 AP_Payable、STK_InStock")
    fields: str = Field(..., description="要导出的字段（逗号分隔，API 字段名，如 FID,FBillNo,FMaterialId.FNumber）")
    order_string: str = Field(..., description="分页排序列，必须是唯一且稳定的列（如 FID ASC），否则翻页会漏行/重复")
    filter_string: str = Field(default="", description="过滤条件，如 \"FDocumentStatus='C'\"")
    table: str = Field(default="", description="目标表名；留空则为 {form_id小写}_tmp_{yyyyMMdd}")
    target_schema: str = Field(default="public", description="目标 schema，默认 public（本机测试表规范）")
    mode: str = Field(default="reload", description="reload=先删表再建（默认）；upsert=按主键合并")
    pk: str = Field(default="", description="主键列（逗号分隔，ODS 列名）；upsert 模式必填")
    columns: str = Field(default="", description="可选：显式指定 ODS 列名（逗号分隔，数量须与 fields 一致）")
    types: str = Field(default="", description="可选：列类型覆盖，如 \"fqty:numeric(18,6),fdate:date\"；默认全部 text")
    dsn: str = Field(default="", description="目标 PG DSN；默认读 KINGDEE_EXPORT_DSN，再默认本机 data_platform")
    allow_remote: bool = Field(default=False, description="允许写入非本机数据库（默认禁止，防止误写生产）")


class ExportChunkInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    session_id: str = Field(..., description="kingdee_export_plan 返回的会话 ID")
    limit: int = Field(default=2000, ge=1, le=MAX_CHUNK, description=f"本页行数，最大 {MAX_CHUNK}")


class ExportFinishInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    session_id: str = Field(..., description="kingdee_export_plan 返回的会话 ID")


# ─────────────────────────────────────────────
# 通用小工具
# ─────────────────────────────────────────────

def _split(csv: str) -> List[str]:
    return [p.strip() for p in (csv or "").split(",") if p.strip()]


def _col_name(field_key: str) -> str:
    """API 字段名 → ODS 列名：小写，非字母数字转下划线，必要时加前缀。"""
    col = re.sub(r"[^0-9a-zA-Z_]+", "_", field_key.strip().lower())
    col = re.sub(r"_+", "_", col).strip("_")
    if not col:
        raise ValueError(f"字段名无法转换为列名: {field_key!r}")
    if col[0].isdigit():
        col = "c_" + col
    return col


def _q(ident: str) -> str:
    if not _IDENT_RE.match(ident):
        raise ValueError(f"非法标识符: {ident!r}")
    return '"' + ident + '"'


def _resolve_dsn(dsn: str, allow_remote: bool) -> str:
    dsn = (dsn or os.getenv("KINGDEE_EXPORT_DSN") or DEFAULT_DSN).strip()
    host = urlparse(dsn).hostname or ""
    if not allow_remote and host.lower() not in LOCAL_HOSTS:
        raise ValueError(
            f"拒绝写入非本机数据库（host={host!r}）。确需写远程库请显式传 allow_remote=true。"
        )
    return dsn


def _split_types(types: str) -> List[str]:
    """按逗号切分类型声明，但忽略括号内的逗号（numeric(18,6)）。"""
    out: List[str] = []
    depth = 0
    cur = ""
    for ch in types or "":
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            if cur.strip():
                out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def _parse_types(types: str, columns: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {c: "text" for c in columns}
    for item in _split_types(types):
        if ":" not in item:
            raise ValueError(f"types 格式应为 列名:类型，实际 {item!r}")
        col, typ = item.split(":", 1)
        col, typ = col.strip(), typ.strip()
        if col not in out:
            raise ValueError(f"types 中列 {col!r} 不存在")
        if not _TYPE_RE.match(typ):
            raise ValueError(f"不支持的列类型 {typ!r}（可用 text/varchar(n)/numeric(p,s)/int/bigint/date/timestamp/boolean）")
        out[col] = typ
    return out


def _api_error(result: Any) -> Optional[str]:
    """从 ExecuteBillQuery 返回中提取错误信息。

    金蝶会把错误嵌在 data 里，且层级不固定（实测为 ``[[{"Result": {...}}]]``），
    所以递归查找 ResponseStatus.IsSuccess == False，避免把错误当数据写入。
    """
    if isinstance(result, dict):
        rs = result.get("ResponseStatus")
        if isinstance(rs, dict) and rs.get("IsSuccess") is False:
            return _rs_message(rs)
        for value in result.values():
            found = _api_error(value)
            if found:
                return found
        return None
    if isinstance(result, (list, tuple)):
        for item in result:
            found = _api_error(item)
            if found:
                return found
    return None


def _rs_message(rs: dict) -> str:
    errs = rs.get("Errors") or []
    if errs and isinstance(errs[0], dict):
        return str(errs[0].get("Message") or "金蝶返回错误")
    return "金蝶返回错误"


_FIELD_ERR_PATTERNS = (
    # 标识为X的字段不存在
    (re.compile(r"标识为(.+?)的字段不存在"), 0),
    # 输出的字段X其在指定实体...
    (re.compile(r"输出的字段(.+?)其在指定实体"), 0),
    # 标识为X的字段上的引用字段Y不存在
    (re.compile(r"标识为(.+?)的字段上的引用字段(\w+)不存在"), 1),
    # 列名 'X' 无效 / 列名X无效
    (re.compile(r"列名\s*'?([^'\s]+)'?\s*无效"), 0),
)


def _invalid_field(msg: str) -> Optional[str]:
    for pat, group in _FIELD_ERR_PATTERNS:
        m = pat.search(msg)
        if m:
            if group == 1:
                return f"{m.group(1).strip()}.{m.group(2).strip()}"
            return m.group(1).strip().strip("'\"")
    return None


async def _fetch(
    form_id: str, field_keys: str, filter_string: str,
    order_string: str, start_row: int, limit: int,
) -> Tuple[List[list], Optional[str]]:
    payload = {
        "FormId": form_id,
        "FieldKeys": field_keys,
        "FilterString": filter_string,
        "OrderString": order_string,
        "StartRow": start_row,
        "Limit": limit,
    }
    result = await _DEPS["post"]("query", payload)
    err = _api_error(result)
    if err:
        return [], err
    rows = _DEPS["rows"](result)
    return rows if isinstance(rows, list) else [], None


async def _captions(form_id: str) -> Dict[str, str]:
    """尽力获取字段中文名用于列注释；失败返回空字典。"""
    try:
        md = await _DEPS["query_metadata"](form_id)
        if not md:
            return {}
        v = _DEPS["validator_cls"](md)
        out: Dict[str, str] = {}

        def add(api_name: str, caption: str) -> None:
            if not caption:
                return
            out[_col_name(api_name)] = caption
            if "." in api_name:
                # 例如 FMaterialId.FNumber 的元数据只登记了子字段 FNumber
                out.setdefault(_col_name(api_name.split(".")[-1]), caption)

        for name, d in v.fields.items():
            if getattr(d, "is_entry", False):
                for c in getattr(d, "children", []) or []:
                    add(c.name, c.caption)
            else:
                add(name, getattr(d, "caption", ""))
        return out
    except Exception:
        return {}


def _ddl(s: dict, drop: bool) -> str:
    cols = ",\n  ".join(
        f"{_q(c)} {s['types'][c]}" for c in s["columns"]
    )
    body = f"{cols},\n  \"_pulled_at\" timestamptz NOT NULL DEFAULT now()"
    if s["mode"] == "upsert":
        body += f",\n  PRIMARY KEY ({', '.join(_q(c) for c in s['pk'])})"
    head = ""
    if drop:
        head = f"DROP TABLE IF EXISTS {_q(s['schema'])}.{_q(s['table'])};\n"
    return head + f"CREATE TABLE {_q(s['schema'])}.{_q(s['table'])} (\n  {body}\n)"


# ─────────────────────────────────────────────
# PG 写入（同步，放到线程里执行）
# ─────────────────────────────────────────────

def _load_chunk(s: dict, rows: List[list], is_first: bool) -> None:
    import psycopg2
    from psycopg2.extras import execute_values

    fqtn = f"{_q(s['schema'])}.{_q(s['table'])}"
    conn = psycopg2.connect(s["dsn"])
    try:
        with conn:  # 单块一个事务：整块成功或整块回滚
            with conn.cursor() as cur:
                if is_first:
                    if s["mode"] == "reload":
                        cur.execute(_ddl(s, drop=True))
                    else:
                        cur.execute(_ddl(s, drop=False))
                template = "(" + ", ".join(
                    "%s" if s["types"][c] == "text" else f"CAST(%s AS {s['types'][c]})"
                    for c in s["columns"]
                ) + ")"
                insert = f"INSERT INTO {fqtn} ({', '.join(_q(c) for c in s['columns'])}) VALUES %s"
                if s["mode"] == "upsert":
                    nonpk = [c for c in s["columns"] if c not in s["pk"]]
                    if nonpk:
                        sets = ", ".join(f"{_q(c)} = EXCLUDED.{_q(c)}" for c in nonpk)
                        insert += f" ON CONFLICT ({', '.join(_q(c) for c in s['pk'])}) DO UPDATE SET {sets}"
                    else:
                        insert += f" ON CONFLICT ({', '.join(_q(c) for c in s['pk'])}) DO NOTHING"
                execute_values(cur, insert, rows, template=template, page_size=500)
    finally:
        conn.close()


def _verify(s: dict, captions: Dict[str, str]) -> dict:
    import psycopg2

    fqtn = f"{_q(s['schema'])}.{_q(s['table'])}"
    conn = psycopg2.connect(s["dsn"])
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {fqtn}")
                total = cur.fetchone()[0]

                dup = None
                if s["pk"]:
                    cols = ", ".join(_q(c) for c in s["pk"])
                    cur.execute(
                        f"SELECT count(*) FROM (SELECT {cols} FROM {fqtn} "
                        f"GROUP BY {cols} HAVING count(*) > 1) x"
                    )
                    dup = cur.fetchone()[0]

                ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                table_comment = (
                    f"金蝶 {s['form_id']} 导出（kingdee-mcp exporter） | "
                    f"字段 {len(s['columns'])} | 过滤: {s['filter'] or '(无)'} | "
                    f"排序: {s['order']} | 导出时间 {ts}"
                )
                cur.execute(f"COMMENT ON TABLE {fqtn} IS %s", (table_comment,))
                for c in s["columns"]:
                    cap = captions.get(c, "")
                    if cap:
                        cur.execute(
                            f"COMMENT ON COLUMN {fqtn}.{_q(c)} IS %s", (cap,)
                        )
    finally:
        conn.close()
    return {"rows_in_table": total, "pk_duplicates": dup}


# ─────────────────────────────────────────────
# 工具实现
# ─────────────────────────────────────────────

async def kingdee_export_plan(params: ExportPlanInput) -> str:
    """第一步：解析字段并生成导出计划（不写数据库）。

    会用 limit=1 探测字段是否有效；金蝶报「字段不存在」时自动剔除该字段并重试
    （最多 10 次），剔除结果在 invalid_fields 中返回，请据此修正字段清单后重新
    规划，避免残缺导出。

    返回：session_id / 生效字段 / ODS 列 / 建表 DDL / 被剔除字段。
    """
    try:
        try:
            import psycopg2  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise ValueError(
                "未安装 psycopg2，无法写入 PostgreSQL；请 pip install -e '.[export]'"
            ) from exc

        dsn = _resolve_dsn(params.dsn, params.allow_remote)
        if params.mode not in ("reload", "upsert"):
            raise ValueError("mode 只能是 reload 或 upsert")

        fields = _split(params.fields)
        if not fields:
            raise ValueError("fields 不能为空")
        if not params.order_string.strip():
            raise ValueError("order_string 不能为空（翻页必须依赖唯一稳定排序）")

        accepted = fields[:]
        dropped: List[str] = []
        for _ in range(10):
            _, err = await _fetch(
                params.form_id, ",".join(accepted), params.filter_string,
                params.order_string, 0, 1,
            )
            if not err:
                break
            bad = _invalid_field(err)
            if not bad or bad not in accepted:
                raise ValueError(f"探测字段失败: {err}")
            accepted.remove(bad)
            dropped.append(bad)
        else:
            raise ValueError("剔除无效字段超过 10 次，请检查字段清单")
        if not accepted:
            raise ValueError("所有字段都无效")

        columns = _split(params.columns) if params.columns else [_col_name(f) for f in accepted]
        if len(columns) != len(accepted):
            raise ValueError(f"columns 数量（{len(columns)}）必须与 fields（{len(accepted)}）一致")
        if len(set(columns)) != len(columns):
            raise ValueError("columns 有重复列名")
        for c in columns:
            _q(c)  # 校验标识符

        types = _parse_types(params.types, columns)

        table = params.table.strip() or f"{params.form_id.lower()}_tmp_{_dt.date.today():%Y%m%d}"
        _q(table)
        _q(params.target_schema)

        pk = [_col_name(p) for p in _split(params.pk)] if params.pk else []
        if params.mode == "upsert":
            if not pk:
                raise ValueError("upsert 模式必须提供 pk")
            missing = [c for c in pk if c not in columns]
            if missing:
                raise ValueError(f"pk 列不在 columns 中: {missing}")

        sid = os.urandom(6).hex()
        s = {
            "session_id": sid,
            "form_id": params.form_id,
            "fields": accepted,
            "columns": columns,
            "types": types,
            "filter": params.filter_string,
            "order": params.order_string,
            "schema": params.target_schema,
            "table": table,
            "mode": params.mode,
            "pk": pk,
            "dsn": dsn,
            "start_row": 0,
            "total_loaded": 0,
            "chunks": 0,
            "done": False,
            "invalid_fields": dropped,
        }
        _STATE[sid] = s

        return _DEPS["fmt"]({
            "session_id": sid,
            "form_id": params.form_id,
            "fields": accepted,
            "columns": columns,
            "types": types,
            "table": f"{params.target_schema}.{table}",
            "mode": params.mode,
            "pk": pk,
            "invalid_fields": dropped,
            "next": "调用 kingdee_export_chunk(session_id, limit<=2000) 取第一块",
            "ddl_preview": _ddl(s, drop=(params.mode == "reload")),
        })
    except Exception as e:  # noqa: BLE001
        return _DEPS["err"](e)


async def kingdee_export_chunk(params: ExportChunkInput) -> str:
    """第二步：取一页写入 PostgreSQL，返回游标与进度。

    首次调用会建表（reload 模式先 DROP）。每块一个事务，失败整块回滚、游标不动，
    可安全重试。当 has_more=false 且 rows_in_chunk=0 时结束，调用
    kingdee_export_finish 做校验与注释。
    """
    try:
        s = _STATE.get(params.session_id)
        if not s:
            raise ValueError("session 不存在或已结束，请重新 kingdee_export_plan")

        limit = min(params.limit, MAX_CHUNK)
        rows, err = await _fetch(
            s["form_id"], ",".join(s["fields"]), s["filter"],
            s["order"], s["start_row"], limit,
        )
        if err:
            raise ValueError(f"取数失败（游标 {s['start_row']} 未推进，可重试）: {err}")

        if rows and len(rows[0]) != len(s["columns"]):
            raise ValueError(
                f"返回列数 {len(rows[0])} 与计划列数 {len(s['columns'])} 不一致，请重新规划"
            )

        if rows:
            await asyncio.to_thread(_load_chunk, s, rows, s["start_row"] == 0)
            s["start_row"] += len(rows)
            s["total_loaded"] += len(rows)
            s["chunks"] += 1

        has_more = len(rows) == limit
        s["done"] = not has_more

        return _DEPS["fmt"]({
            "session_id": s["session_id"],
            "rows_in_chunk": len(rows),
            "rows_loaded_total": s["total_loaded"],
            "next_start_row": s["start_row"],
            "has_more": has_more,
            "chunks": s["chunks"],
            "next": "继续 kingdee_export_chunk" if has_more else "调用 kingdee_export_finish",
        })
    except Exception as e:  # noqa: BLE001
        return _DEPS["err"](e)


async def kingdee_export_finish(params: ExportFinishInput) -> str:
    """第三步：校验并收尾。检查表内行数与主键唯一性，写入表/列注释，结束会话。"""
    try:
        s = _STATE.get(params.session_id)
        if not s:
            raise ValueError("session 不存在或已结束")
        if not s["done"]:
            raise ValueError("还有未取完的数据，请继续调用 kingdee_export_chunk")
        if s["chunks"] == 0:
            raise ValueError("没有写入任何数据（可能过滤条件无匹配），未建表")

        caps = await _captions(s["form_id"])
        result = await asyncio.to_thread(_verify, s, caps)

        warnings = []
        if result["rows_in_table"] != s["total_loaded"]:
            warnings.append(
                f"表内行数 {result['rows_in_table']} 与累计载入 {s['total_loaded']} 不一致"
            )
        if result["pk_duplicates"]:
            warnings.append(f"主键重复 {result['pk_duplicates']} 组（order_string 可能不唯一）")

        out = {
            "session_id": s["session_id"],
            "table": f"{s['schema']}.{s['table']}",
            "rows_loaded": s["total_loaded"],
            "rows_in_table": result["rows_in_table"],
            "chunks": s["chunks"],
            "pk_duplicates": result["pk_duplicates"],
            "invalid_fields": s["invalid_fields"],
            "warnings": warnings,
        }
        _STATE.pop(s["session_id"], None)
        return _DEPS["fmt"](out)
    except Exception as e:  # noqa: BLE001
        return _DEPS["err"](e)


# ─────────────────────────────────────────────
# 注册
# ─────────────────────────────────────────────

_TOOLS = (
    (kingdee_export_plan, "kingdee_export_plan", "导出计划（解析字段/生成DDL，不落库）",
     "规划金蝶单据导出：探测字段有效性、生成建表 DDL 与会话。不写数据库。"),
    (kingdee_export_chunk, "kingdee_export_chunk", "导出分块（取一页并写入 PostgreSQL）",
     "按会话游标取一页（最多 2000 行）写入 PostgreSQL，返回进度。首次调用建表。"),
    (kingdee_export_finish, "kingdee_export_finish", "导出收尾（校验行数与主键、写注释）",
     "校验导出结果（行数、主键唯一性），写入表/列注释并结束会话。"),
)


def register(*, mcp, post, rows, fmt, err, query_metadata, validator_cls) -> None:
    """由 server.py 调用，注入内部依赖并注册三个导出工具。"""
    _DEPS.update({
        "post": post, "rows": rows, "fmt": fmt, "err": err,
        "query_metadata": query_metadata, "validator_cls": validator_cls,
    })
    for fn, name, title, desc in _TOOLS:
        fn.__doc__ = desc
        mcp.tool(
            name=name,
            annotations={
                "title": title,
                "readOnlyHint": False,   # 会写 PostgreSQL（但不写金蝶）
                "destructiveHint": False,
                "idempotentHint": False,
                "openWorldHint": False,
            },
        )(fn)
