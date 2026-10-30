"""kingdee_export_* 工具的离线单元测试（不连金蝶、不连 PG）。

覆盖金蝶返回形状的解析（错误嵌在 data 里）、字段错误识别、类型白名单、
DSN 本机守卫和 DDL 生成。
"""

import pytest

from kingdee_mcp import exporter as ex


# ── 列名与类型 ──────────────────────────────────────────────

@pytest.mark.parametrize("api_key,expected", [
    ("FBillNo", "fbillno"),
    ("FMaterialId.FNumber", "fmaterialid_fnumber"),
    ("FEntryID", "fentryid"),
    ("F_XTR_Text1", "f_xtr_text1"),
    ("2Qty", "c_2qty"),
])
def test_col_name(api_key, expected):
    assert ex._col_name(api_key) == expected


def test_parse_types_defaults_to_text_and_validates():
    cols = ["a", "b"]
    assert ex._parse_types("", cols) == {"a": "text", "b": "text"}
    assert ex._parse_types("a:numeric(18,6)", cols)["a"] == "numeric(18,6)"
    with pytest.raises(ValueError):
        ex._parse_types("zzz:text", cols)          # 列不存在
    with pytest.raises(ValueError):
        ex._parse_types("a:text); DROP TABLE x;--", cols)  # 注入形状
    with pytest.raises(ValueError):
        ex._parse_types("a:jsonb", cols)           # 不在白名单


# ── 金蝶返回形状 ────────────────────────────────────────────

def _err_envelope(msg):
    return [[{"Result": {"ResponseStatus": {
        "ErrorCode": 500, "IsSuccess": False,
        "Errors": [{"Message": msg}],
    }}}]]


def test_api_error_detects_error_nested_in_data():
    # 实测形状：data = [[{Result: ...}]]，错误不是异常而是数据
    assert ex._api_error(_err_envelope("元数据中标识为FID的字段不存在")) \
        == "元数据中标识为FID的字段不存在"


def test_api_error_ignores_normal_rows():
    assert ex._api_error([["100137", "深圳市福尔利有限公司"]]) is None
    assert ex._api_error({"count": 2, "data": [["a"], ["b"]]}) is None


@pytest.mark.parametrize("msg,expected", [
    ("元数据中标识为FID的字段不存在", "FID"),
    ("输出的字段FNOPE其在指定实体中不存在", "FNOPE"),
    ("标识为FMaterialId的字段上的引用字段FNumber不存在", "FMaterialId.FNumber"),
    ("列名 'FID' 无效。", "FID"),
])
def test_invalid_field_patterns(msg, expected):
    assert ex._invalid_field(msg) == expected


def test_invalid_field_unknown_message_returns_none():
    assert ex._invalid_field("网络超时") is None


# ── DSN 守卫 ────────────────────────────────────────────────

def test_dsn_guard_blocks_remote_by_default():
    with pytest.raises(ValueError):
        ex._resolve_dsn("postgresql://u:p@10.0.0.9:5432/db", allow_remote=False)


def test_dsn_guard_allows_local_and_explicit_remote():
    assert ex._resolve_dsn("postgresql://u:p@127.0.0.1:5432/db", False).endswith("/db")
    assert ex._resolve_dsn("postgresql://u:p@10.0.0.9:5432/db", True).endswith("/db")


def test_dsn_guard_defaults_to_local(monkeypatch):
    monkeypatch.delenv("KINGDEE_EXPORT_DSN", raising=False)
    assert "127.0.0.1" in ex._resolve_dsn("", False)


# ── DDL ─────────────────────────────────────────────────────

def _session(mode="reload", pk=()):
    return {
        "schema": "public", "table": "t", "mode": mode, "pk": list(pk),
        "columns": ["a", "b"],
        "types": {"a": "text", "b": "numeric(18,6)"},
    }


def test_ddl_reload_drops_and_creates():
    sql = ex._ddl(_session(), drop=True)
    assert sql.startswith('DROP TABLE IF EXISTS "public"."t"')
    assert '"b" numeric(18,6)' in sql
    assert "PRIMARY KEY" not in sql
    assert "_pulled_at" in sql


def test_ddl_upsert_has_primary_key_and_no_drop():
    sql = ex._ddl(_session("upsert", ["a"]), drop=False)
    assert not sql.startswith("DROP")
    assert 'PRIMARY KEY ("a")' in sql


# ── 列注释兜底 ──────────────────────────────────────────────

def test_caption_for_dotted_lookup_fields():
    caps = {"fsupplierid": "供应商", "fmaterialid": "物料编码", "fbillno": "单据编号"}
    assert ex._caption_for(caps, "fsupplierid_fname") == "供应商（名称）"
    assert ex._caption_for(caps, "fmaterialid_fnumber") == "物料编码（编码）"
    assert ex._caption_for(caps, "fbillno") == "单据编号"
    assert ex._caption_for(caps, "fid") == ""


# ── 注册 ────────────────────────────────────────────────────

class _FakeMCP:
    def __init__(self):
        self.names = []

    def tool(self, *, name, annotations):
        def deco(fn):
            self.names.append(name)
            return fn
        return deco


def test_register_adds_three_tools_and_injects_deps():
    mcp = _FakeMCP()
    ex.register(mcp=mcp, post=object(), rows=object(), fmt=object(), err=object(),
                query_metadata=object(), validator_cls=object())
    assert mcp.names == ["kingdee_export_plan", "kingdee_export_chunk", "kingdee_export_finish"]
    assert ex._DEPS["post"] is not None
