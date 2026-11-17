#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""国网评标演示 · BFF（Backend for Frontend）最小可跑实现

定位：业务界面不直连 apemind.ai，也不在浏览器里放密钥。界面只请求本服务，
本服务用 api-key 账户调用 apemind CLI，把结果整理成界面要的形状。

为什么封装 CLI 而不是拼 HTTP：
  - 官方未公开 /api/v2 的完整接口清单；`apemind api` 只放行极少数路径
    （实测 /api/v2/collections/{id}/documents 可通，/api/v2/functions 不通）；
  - CLI 已封装认证、workspace header、分页、重试与统一信封；
  - 换成 api-key 账户后（apemind auth connect --api-key-stdin），凭证长期有效，
    不再受 9 分钟 OAuth token 影响。

设计约束（来自实测，不是拍脑袋）：
  - 列表/检索 2~4 秒   → 必须短缓存 + 并发合并，否则界面每点一次都转圈
  - 函数抽取中位 23 秒 → 必须异步任务 + 轮询，不能同步等
  - 函数限流 4 并发 / 3 rps → 必须单队列串行化，超限会被服务端拒
  - 只读               → 禁止 --confirm / --force / --input 写字段

只用标准库。启动：python sgcc_bff.py
"""
import gzip
import hashlib
import json
import os
import queue
import threading
import time
import uuid
from concurrent import futures
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ID = "orge0066e5cc05abc86"
AUDIT = os.path.join(HERE, "sgcc_bff_audit.jsonl")
MATRIX = os.path.join(HERE, "国网Demo_W3v2_页面数据_6家8条.json")

PORT = int(os.environ.get("BFF_PORT", "8787"))
DEMO_TOKEN = os.environ.get("BFF_TOKEN", "")      # 留空则不校验（仅限本机演示）
ALLOW_ORIGIN = os.environ.get("BFF_ORIGIN", "*")

# ---- 公网模式（B 方案：cloudflared 隧道）----
# 一旦挂上隧道，这个端口世界上任何人都能敲。所以公网模式与本地模式必须分开：
#   PUBLIC=1 → 必须给 token（没给直接拒绝启动），页面也由 BFF 自己托管（同源，
#              客户浏览器里的 127.0.0.1 是他自己的机器，跨端口取数一定失败）；
#   本地模式 → 保持原样，零配置、双击即用。
PUBLIC = os.environ.get("BFF_PUBLIC", "") == "1"
PAGE_FILE = os.path.join(HERE, "国网物资部_评标辅助工作台_W3.html")
RATE_PER_MIN = int(os.environ.get("BFF_RATE", "120"))
_rate = {}
_rate_lock = threading.Lock()
CLI_TIMEOUT = 180

# 只放行本项目 8 个库，其余一律拒绝
KBS = {
    "colcf0af5d45fd7ef6b": {"name": "国网湖北招标文件包·真实样本 L1", "side": "tender"},
    "col0cd75b81b6b47b74": {"name": "P152507-基准库-质量验证", "side": "tender"},
    "col2de7406a4c8a6e56": {"name": "受审v2-HZ-华中电力监测", "side": "bid"},
    "col4020b47c9984bbbb": {"name": "受审v2-XY-襄阳汉江", "side": "bid"},
    "col2894640b355a1b11": {"name": "受审v2-YC-宜昌三峡", "side": "bid"},
    "col8fe7f3d2e3d2dac5": {"name": "受审v2-JZ-荆州楚元", "side": "bid"},
    "cold98e481541486cc5": {"name": "受审v2-HS-黄石磁湖", "side": "bid"},
    "col1c64f1cb56417c15": {"name": "受审v2-ES-恩施清江", "side": "bid"},
}

_cache = {}
_cache_lock = threading.Lock()
_jobs = {}
_jobs_lock = threading.Lock()
_queue = queue.Queue()
_stats = {"cli_calls": 0, "cache_hits": 0, "jobs": 0, "errors": 0}


class CliError(Exception):
    def __init__(self, msg, code=None):
        super().__init__(msg)
        self.code = code


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def audit(rec):
    rec["at"] = now()
    try:
        with open(AUDIT, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _apemind_request(method, path, body=None, timeout=60, query=None, raw=False):
    """对 apemind.ai 的纯 HTTP 调用（替代原 CLI 子进程方案）。

    为什么要去 CLI：CLI 是 Windows 上的 apemind.exe 子进程，每次调用都要进程
    启动 + TLS 握手（判定端到端 2.4~3.6s，函数自报仅 245ms），而且把演示机
    绑死在"装了 CLI 的那台电脑"上——现场换机器、代理变化都是故障点。
    HTTP 版实测：同一条判定 0.64s（约 4~6 倍提速），依赖只剩一个 api-key。
    代理策略与原 _cli_env 一致：显式绕开本机代理，直连。
    """
    import http.client
    import queue as _q
    import urllib.error
    import urllib.parse
    import urllib.request

    base = os.environ.get("APEMIND_BASE_URL", "https://apemind.ai").rstrip("/")
    key = os.environ.get("APEMIND_API_KEY", "")
    ws = os.environ.get("APEMIND_WORKSPACE_ID", WORKSPACE_ID)
    u = urllib.parse.urlparse(base)
    req_path = (u.path if u.path and u.path != "/" else "") + path
    if query:
        req_path += "?" + "&".join("%s=%s" % (k, urllib.parse.quote(str(v)))
                                   for k, v in query.items() if v is not None)
    data = None
    headers = {"Accept": "application/json",
               # 不带 UA 或用 urllib 默认 UA 会被 Cloudflare 拦
               # （"site owner has blocked access based on your browser's signature"）。
               "User-Agent": "sgcc-bff-http/0.2 (ApeMind demo BFF)"}
    if key:
        headers["Authorization"] = "Bearer " + key
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"

    # 常驻 HTTPS 连接池：urllib 每次调用重新 TLS 握手（约 0.3~0.5s），
    # 池化后判定端到端再降一截。连接被服务端关掉就丢掉重取（见 except 分支）。
    def _get_conn():
        try:
            return _CONN_POOL.get_nowait()
        except _q.Empty:
            return http.client.HTTPSConnection(u.hostname, u.port or 443,
                                               timeout=timeout)

    def _do_request():
        conn = _get_conn()
        try:
            conn.request(method, req_path, body=data, headers=headers)
            resp = conn.getresponse()
            payload = resp.read()
            _CONN_POOL.put_nowait(conn)
            return resp.status, payload
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            raise

    def _parse(status, payload):
        text = (payload or b"").decode("utf-8", errors="replace")
        if status >= 400:
            try:
                js = json.loads(text)
            except json.JSONDecodeError:
                js = {"message": text[:200]}
            detail = js.get("detail") or js
            msg, code = None, None
            if isinstance(detail, dict):
                msg = detail.get("message") or detail.get("error_code") or str(detail)[:200]
                code = detail.get("error_code")
            else:
                msg = str(detail)[:200]
            if status in (401, 403):
                code = code or "unauthorized"
            raise CliError(msg or ("HTTP %d" % status), code)
        if not text:
            return {}
        return json.loads(text)

    last = None
    for attempt in range(2):
        try:
            return _parse(*_do_request())
        except CliError:
            raise
        except (TimeoutError, OSError) as e:
            last = e
            if attempt == 0:
                time.sleep(0.5)
    raise CliError("网络不可用：%r" % (last or "unknown"), "network_unavailable")


def _fn_invoke(fn_ref, payload, timeout=120):
    """平台函数调用（HTTP）。返回与 CLI 信封兼容的 dict：
    output / id / version_number / elapsed_ms。run_id 尽力补：invoke 响应不带，
    事后查一次最近 runs，拿到就带上，拿不到不阻断。"""
    ws = os.environ.get("APEMIND_WORKSPACE_ID", WORKSPACE_ID)
    t0 = time.time()
    out = _apemind_request(
        "POST", "/api/v2/orgs/%s/functions/%s/invoke" % (ws, fn_ref),
        body=payload, timeout=timeout, query={"workspace_id": ws})
    wall_ms = int((time.time() - t0) * 1000)
    if not isinstance(out, dict):
        raise CliError("函数返回了非 JSON 对象", "invalid_response")
    if out.get("success") is False:
        raise CliError(out.get("message") or "函数执行失败",
                       out.get("error_code") or "function_run_failed")
    run = {}
    try:
        runs = _apemind_request(
            "GET", "/api/v2/orgs/%s/functions/%s/runs" % (ws, fn_ref),
            query={"workspace_id": ws, "page_size": 1}, timeout=20)
        items = runs.get("items") or []
        if items:
            r = items[0]
            run = {"run_id": r.get("id"), "version": r.get("version_number"),
                   "status": r.get("status"), "elapsed_ms": r.get("elapsed_ms")}
    except CliError:
        pass
    return {"output": out, "id": run.get("run_id"),
            "version_number": run.get("version"),
            "elapsed_ms": run.get("elapsed_ms") or wall_ms,
            "status": run.get("status") or "SUCCEEDED"}


# 以下把原 CLI 的 5 类调用翻译成 HTTP，保持 run_cli(args) 签名不变，
# BFF 其余逻辑（缓存/快照/限流/审计）原样复用。
def run_cli(args, timeout=CLI_TIMEOUT):
    _stats["cli_calls"] += 1
    try:
        return _translate(args, timeout)
    except CliError:
        _stats["errors"] += 1
        raise


def _translate(args, timeout):
    kind = args[0]
    ws = os.environ.get("APEMIND_WORKSPACE_ID", WORKSPACE_ID)

    if kind == "auth" and args[1] == "status":
        u = _apemind_request("GET", "/api/v2/auth/user", timeout=timeout)
        return {"account": {"name": u.get("email") or u.get("username") or ""}}

    if kind == "function" and args[1] == "invoke":
        return _fn_invoke(args[2], json.load(open(args[4], encoding="utf-8")),
                          timeout=timeout)

    if kind == "document" and args[1] == "list":
        kb = args[args.index("--kb") + 1]
        lim = int(args[args.index("--limit") + 1]) if "--limit" in args else 100
        d = _apemind_request(
            "GET", "/api/v2/collections/%s/documents" % kb,
            query={"workspace_id": ws, "page_size": min(max(lim, 1), 100)},
            timeout=timeout)
        return {"items": d.get("items", [])}

    if kind == "search":
        qs = args[1]
        kb = args[args.index("--kb") + 1]
        lim = int(args[args.index("--limit") + 1]) if "--limit" in args else 8
        d = _apemind_request(
            "POST", "/api/v2/collections/%s/searches" % kb,
            body={"query": qs, "save_to_history": False,
                  "fulltext_search": {"enable": True, "max_results": lim},
                  "vector_search": {"enable": True, "max_results": lim}},
            query={"workspace_id": ws}, timeout=timeout)
        hits = []
        for it in d.get("items", []):
            md = it.get("metadata") or {}
            hits.append({
                "rank": it.get("rank"),
                "document_title": md.get("title") or it.get("source"),
                "section": md.get("heading_anchor"),
                "start_line": None, "end_line": None,
                "match_quality": it.get("recall_type"),
                "handle": md.get("document_id"),
                "excerpt": (it.get("content") or "")[:500]})
        return {"groups": [{"items": hits}]}

    if kind == "grep":
        return _grep_http(args, timeout)

    if kind == "read":
        return _read_http(args[1], timeout)

    raise CliError("HTTP 版不支持的调用：%r" % (args[:2],), "unsupported")


# ---------------------------------------------------------------- grep 的 HTTP 实现
# CLI grep 是服务端 RE2 全库逐行扫描；HTTP 侧没有等价接口。
# 这里的等价做法：先用全文检索把可能命中的文档找出来，再把这些文档的原文
# （/content，带缓存）拉回来本地逐行正则扫描。
# 覆盖率口径与 CLI 保持一致：只有"库内每一篇都被扫过"才报 coverage=complete，
# 这样「能负责任地说没有」的承诺不变。docs 全列表来自 /documents（缓存），
# 与命中文档求并集决定实际扫描集合。
_GREP_MIN = 2          # 关键词短于这个字符数不做全文召回，直接全库扫
_CONTENT_TTL = 600     # 原文缓存 10 分钟：演示期同一文档反复 grep/read 都走内存

from queue import Queue as _PoolQueue
_CONN_POOL = _PoolQueue(maxsize=8)

_content_cache = {}
_content_lock = threading.Lock()


def _doc_content(kb, doc_id):
    key = "%s/%s" % (kb, doc_id)
    with _content_lock:
        hit = _content_cache.get(key)
        if hit and time.time() - hit[0] < _CONTENT_TTL:
            return hit[1]
    c = _apemind_request(
        "GET", "/api/v2/collections/%s/documents/%s/content" % (kb, doc_id),
        query={"workspace_id": os.environ.get("APEMIND_WORKSPACE_ID", WORKSPACE_ID)},
        timeout=60).get("content") or ""
    with _content_lock:
        _content_cache[key] = (time.time(), c)
    return c


def _kb_docs(kb):
    """返回 (按顺序的文档 id 列表, 库内总数, id->标题 映射)。"""
    ids, names = [], {}
    page = 1
    while True:
        d = _apemind_request(
            "GET", "/api/v2/collections/%s/documents" % kb,
            query={"workspace_id": os.environ.get("APEMIND_WORKSPACE_ID", WORKSPACE_ID),
                   "page_size": 100, "page": page}, timeout=60)
        items = d.get("items", [])
        for it in items:
            if it.get("id"):
                ids.append(it["id"])
                names[it["id"]] = it.get("name") or it["id"]
        total = d.get("total") or len(ids)
        if len(ids) >= min(total, 300) or not items:
            break
        page += 1
    return ids, len(ids), names


_full_scan_state = {}           # kb -> "running" | "done"
_full_scan_lock = threading.Lock()


def _full_scan_bg(kb):
    """后台把一个库的全部原文拉进缓存。

    动机：CLI grep 是全库逐行扫描，能"负责任地说没有"；HTTP 侧没有等价接口，
    同步全扫一次要拉几百 KB×155 篇。折中：先用全文召回快速给出部分覆盖的结果，
    同时后台把全库原文预热进缓存——下一次同样的 grep 就能扫全库，
    coverage 自然升级为 complete。演示里"敢说没有"的口径不打折。
    """
    with _full_scan_lock:
        if _full_scan_state.get(kb) == "running":
            return
        _full_scan_state[kb] = "running"
    try:
        ids, _, _names = _kb_docs(kb)
        for did in ids:
            try:
                _doc_content(kb, did)
            except CliError:
                continue
        with _full_scan_lock:
            _full_scan_state[kb] = "done"
    except Exception:
        with _full_scan_lock:
            _full_scan_state[kb] = "failed"


def _cached_doc_ids(kb, doc_ids):
    with _content_lock:
        nowt = time.time()
        return {d for d in doc_ids
                if (h := _content_cache.get("%s/%s" % (kb, d)))
                and nowt - h[0] < _CONTENT_TTL}


def _grep_http(args, timeout):
    kbs = [args[i + 1] for i, a in enumerate(args) if a == "--kb"]
    lim = int(args[args.index("--limit") + 1]) if "--limit" in args else 100
    exc = int(args[args.index("--excerpt") + 1]) if "--excerpt" in args else 400
    max_count = 20
    if "--max-count" in args:
        max_count = int(args[args.index("--max-count") + 1])
    ws = os.environ.get("APEMIND_WORKSPACE_ID", WORKSPACE_ID)
    low = qs = args[1].lower()
    scans, groups, total_hits = [], [], 0
    for kb in kbs:
        doc_ids, total_docs, names = _kb_docs(kb)
        # 扫描集 = 全文召回命中的文档 ∪ 缓存里已有的文档（读缓存零成本）。
        cand = set()
        if len(qs) >= _GREP_MIN:
            try:
                d = _apemind_request(
                    "POST", "/api/v2/collections/%s/searches" % kb,
                    body={"query": args[1], "save_to_history": False,
                          "fulltext_search": {"enable": True, "max_results": 100}},
                    query={"workspace_id": ws}, timeout=timeout)
                for it in d.get("items", []):
                    md = it.get("metadata") or {}
                    if md.get("document_id"):
                        cand.add(md["document_id"])
            except CliError:
                pass
        scan_set = cand | _cached_doc_ids(kb, doc_ids)
        # 全库还没预热完就在后台继续拉，下一次调用覆盖更全。
        if len(scan_set) < total_docs and _full_scan_state.get(kb) != "done":
            threading.Thread(target=_full_scan_bg, args=(kb,), daemon=True).start()
        scanned, matched = 0, 0
        for did in doc_ids:
            if did not in scan_set:
                continue
            text = _doc_content(kb, did)
            scanned += 1
            lines = text.split("\n")
            n_in_doc = 0
            grp = None
            for i, line in enumerate(lines):
                if low in line.lower():
                    n_in_doc += 1
                    if n_in_doc > max_count:
                        continue
                    total_hits += 1
                    if grp is None:
                        grp = {"knowledge_base_id": kb, "document_title": names.get(did, did),
                               "handle": did, "matches": [],
                               "matches_truncated": False, "scan_truncated": False}
                        groups.append(grp)
                    s = max(0, i - 1)
                    excerpt = "\n".join(lines[s:i + 2])[:exc]
                    grp["matches"].append({
                        "line": i + 1, "section": None,
                        "excerpt": excerpt, "extra_matches_in_line": 0,
                        "handle": "%s:%d" % (did, i + 1)})
            if n_in_doc > max_count:
                grp["matches_truncated"] = True
            if n_in_doc:
                matched += 1
        scans.append({"knowledge_base_id": kb, "coverage":
                      "complete" if total_docs and scanned >= total_docs else "partial",
                      "docs_scanned": scanned, "docs_matched": matched,
                      "docs_total": total_docs})
        if len(groups) > lim:
            groups = groups[:lim]
    return {"total_match_count": total_hits, "groups": groups, "scans": scans}


# ---------------------------------------------------------------- read 的 HTTP 实现
# CLI read 的句柄是 "docXXXX[:行号]"。HTTP 侧把原文（带缓存）拉回来按行切，
# :行号 有值时给该行前后各 5 行的窗口，没有时从头给 60 行。
_READ_WINDOW = 5


def _read_http(handle, timeout):
    base = handle.split(":")[0].strip()
    line_no = None
    if ":" in handle:
        try:
            line_no = int(handle.split(":", 1)[1])
        except ValueError:
            line_no = None
    if not base.startswith("doc"):
        raise CliError("handle 格式不合法", "bad_handle")
    ws = os.environ.get("APEMIND_WORKSPACE_ID", WORKSPACE_ID)
    # 找到这个文档在哪个白名单库：文档列表本来就为 /api/docs 缓存过，代价低。
    loc = None
    for kb in KBS:
        try:
            ids, _, _names = _kb_docs(kb)
        except CliError:
            continue
        if base in ids:
            loc = kb
            break
    if not loc:
        raise CliError("handle 不在本项目白名单库内：%s" % base, "kb_not_allowed")
    text = _doc_content(loc, base)
    all_lines = text.split("\n")
    total = len(all_lines)
    if line_no:
        s = max(0, line_no - 1 - _READ_WINDOW)
        e = min(total, line_no + _READ_WINDOW)
        chunk = all_lines[s:e]
        start = s + 1
    else:
        chunk, start = all_lines[:60], 1
    title = base
    try:
        d = _apemind_request(
            "GET", "/api/v2/collections/%s/documents/%s" % (loc, base),
            query={"workspace_id": ws}, timeout=30)
        title = d.get("name") or base
    except CliError:
        pass
    return {"items": [{"document_title": title, "section": None,
                       "start_line": start, "end_line": start + len(chunk) - 1,
                       "total_lines": total, "lines": chunk}]}


def cached(key, ttl, fn):
    """短缓存 + 并发合并：同一 key 同时只跑一次。"""
    nowt = time.time()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and nowt - hit[0] < ttl:
            _stats["cache_hits"] += 1
            return hit[1], "cache"
        lock = hit[2] if hit else threading.Lock()
        _cache[key] = (hit[0] if hit else 0, hit[1] if hit else None, lock)
    with lock:
        with _cache_lock:
            hit = _cache.get(key)
            if hit and time.time() - hit[0] < ttl:
                _stats["cache_hits"] += 1
                return hit[1], "cache"
        val = fn()
        with _cache_lock:
            _cache[key] = (time.time(), val, lock)
        return val, "live"


def check_kb(kb):
    if kb not in KBS:
        raise CliError("知识库不在本项目白名单内：%s" % kb, "kb_not_allowed")


def job_worker():
    while True:
        jid, payload = _queue.get()
        with _jobs_lock:
            _jobs[jid]["state"] = "running"
            _jobs[jid]["started_at"] = now()
        tf = os.path.join(HERE, "_bff_in_%s.json" % jid)
        try:
            with open(tf, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            data = run_cli(["function", "invoke", "sgcc-field-extract",
                            "--input", tf, "--json"])
            with _jobs_lock:
                _jobs[jid].update(state="done", result=data.get("output"),
                                  run_id=data.get("id"),
                                  version_number=data.get("version_number"),
                                  finished_at=now())
        except CliError as e:
            with _jobs_lock:
                _jobs[jid].update(state="failed", error=str(e), finished_at=now())
        except Exception as e:                                    # noqa: BLE001
            with _jobs_lock:
                _jobs[jid].update(state="failed", error=repr(e), finished_at=now())
        finally:
            try:
                os.remove(tf)
            except OSError:
                pass
            _queue.task_done()


# ---------------------------------------------------------------- 判定（平台函数）
# sgcc-rule-judge 是纯计算函数：46 条判定式在沙箱里跑完 240~280ms，
# 真正的耗时是 CLI 进程启动 + 往返（约 2.4~3.6s）。所以这里不做异步队列，
# 同步返回即可，但要给界面一个"正在执行第 N 条"的确定性耗时，不能瞎写"实时"。
JUDGE_FN = "sgcc-rule-judge"
JUDGE_TTL = 120
JUDGE_MAX_BYTES = 512 * 1024
# 六家的现场抽取输入快照（_judge_in_<代号>.json，由 judge_live_check.py 落盘）。
# 只认这几个代号：路径直接拼文件名，不做白名单就是目录穿越。
JUDGE_CODES = ("华中", "襄阳", "宜昌", "荆州", "黄石", "恩施")

# 公网 caller 一律用 ASCII 代号。原因是审计里抓到的真实故障：
#   "Bad request syntax ('{\"code\": \"æ©æ½\"}POST /api/judge/by-code HTTP/1.1')"
# cloudflared 在 h2 → HTTP/1.1 转换时按字符数而不是字节数给了 Content-Length，
# 中文 body 因此会短读、残留的字节串到下一个请求的请求行前面。
# 请求体全 ASCII 就从源头绕开它，不必赌中间层的实现。
CODE_ALIAS = {"HZ": "华中", "XY": "襄阳", "YC": "宜昌",
              "JZ": "荆州", "HS": "黄石", "ES": "恩施"}


def resolve_code(v):
    v = str(v or "").strip()
    return CODE_ALIAS.get(v.upper(), v)


# ---------------------------------------------------------------- 规则源（P0 注入）
# 46 条规则不再只能来自函数内嵌的 V2：本地存在规则源文件时，BFF 在每次调用前把它注入
# inp.rules，平台按传入规则判定 —— 改一条规则不需要重新发版。
#
# 三道防线，少一道就会出假绿：
#   ① 规则源解析失败必须报错。绝不"悄悄退回内嵌 V2"——那等于客户把规则改坏了，
#      界面照样给出漂亮结论，还看不出来用的是旧规则。
#   ② 快照/缓存指纹必须含规则指纹。否则改了规则、输入没变，会直接命中旧快照，
#      演示里"改了却没变"或"没改却说变了"，两种情况都是假的。
#   ③ 规则指纹原样传给函数，函数实算不一致会写进 notes 留痕，不阻断判定。
RULES_SRC = os.path.join(HERE, "_review", "rules_source_v2.json")
_rules_lock = threading.Lock()
_rules_cache = {"stamp": None, "rules": None, "sha": None, "version": None}


def _rule_src_stamp():
    try:
        st = os.stat(RULES_SRC)
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def load_rule_source():
    """返回 (rules, version, sha)；没有规则源文件则返回 None（走函数内嵌 V2）。

    解析失败抛 CliError，由调用方转成明确错误——见防线①。
    指纹算法与 build_judge_fn.py / 函数内一致：对 {meta:{version,n}, rules} 做紧凑序列化。
    """
    stamp = _rule_src_stamp()
    if stamp is None:
        return None
    with _rules_lock:
        if _rules_cache["stamp"] == stamp:
            return _rules_cache["rules"], _rules_cache["version"], _rules_cache["sha"]
    try:
        with open(RULES_SRC, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError) as e:
        raise CliError("规则源 %s 读取/解析失败：%r。修好它或删掉它，不会退回内嵌规则"
                       % (os.path.basename(RULES_SRC), e), "rules_source_invalid")
    rules = doc.get("rules") if isinstance(doc, dict) else doc
    version = (doc.get("version") if isinstance(doc, dict) else None) or "LOCAL"
    if not isinstance(rules, list) or not rules:
        raise CliError("规则源里没有 rules 数组（或为空）", "rules_source_invalid")
    sha = hashlib.sha256(json.dumps(
        {"meta": {"version": version, "n": len(rules)}, "rules": rules},
        ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()[:16]
    with _rules_lock:
        _rules_cache.update(stamp=stamp, rules=rules, sha=sha, version=version)
    return rules, version, sha


def inject_rules(payload):
    """把规则源注入入参。返回 (payload, 规则指纹或空串)。"""
    src = load_rule_source()
    if not src:
        return payload, ""
    rules, version, sha = src
    b = dict(payload)
    b["rules"] = rules
    b["rule_set_version"] = version
    b["rule_set_sha"] = sha
    return b, sha


def _judge_fp(raw, rule_sha):
    """判定快照的输入指纹。无规则源时与改造前完全一致，老快照继续有效。"""
    return _fingerprint(raw) if not rule_sha else _fingerprint(raw + "|rule:" + rule_sha)


def call_judge(payload):
    """调平台判定函数。payload 原样落临时文件——CLI 只认 --input 文件。"""
    import tempfile
    fd, path = tempfile.mkstemp(suffix=".json", prefix="judge_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
        d = run_cli(["function", "invoke", JUDGE_FN, "--input", path, "--json"],
                    timeout=120)
        return {"judge": d.get("output"),
                "run": {"run_id": d.get("id"), "version": d.get("version_number"),
                        "status": d.get("status"), "elapsed_ms": d.get("elapsed_ms"),
                        "error": d.get("error_message")}}
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


# ---------------------------------------------------------------- 判定快照（物化）
# 为什么落盘：一次 CLI 调用实测 wall 2.1~3.4s，而函数自报只 245ms。
# 差额是进程启动 130ms + TLS 握手 750ms（每次新进程都要重做 2 个 RTT）+ 服务端排队。
# 但判定是纯函数：同一份 _judge_in_<代号>.json 永远算出同一份 46 条结论。
# 输入是静态文件 ⇒ 结果可以物化，读盘 1~3ms，与平台往返无关。
# 快照带输入指纹：输入换了、指纹对不上就重算，绝不拿旧结论冒充新结论。
JUDGE_SNAP_DIR = os.path.join(HERE, "_judge_snap")
JUDGE_SNAP_VER = "v1"


def _snap_file(code):
    return os.path.join(JUDGE_SNAP_DIR, "%s.json" % code)


def _fingerprint(raw):
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def snap_read(code, fp):
    try:
        with open(_snap_file(code), encoding="utf-8") as f:
            s = json.load(f)
    except (OSError, ValueError):
        return None
    if s.get("fp") != fp or s.get("ver") != JUDGE_SNAP_VER:
        return None
    return s


def snap_write(code, fp, res):
    os.makedirs(JUDGE_SNAP_DIR, exist_ok=True)
    tmp = _snap_file(code) + ".tmp"
    payload = {"ver": JUDGE_SNAP_VER, "code": code, "fp": fp, "at": now(), "result": res}
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp, _snap_file(code))          # 原子替换，读侧不会读到半个文件


def presnap(codes=None, force=False, workers=1, retry=1):
    """把各家的判定算好落盘。

    默认串行。原因不是并发不可行，是排查时被污染过一次：测多路并发的十几分钟里
    本机本地代理挂了，六路全失败，看起来像"CLI 不能并发"。后来定位到真因在代理
    （见 _cli_env），并发这条路本身没被证伪。
    仍默认串行：一次 2.2~6s，六家十几秒，发生在启动期，不值得为省十秒去赌未验证的并发。
    workers 留着可调，确认并发安全后再放宽。
    """
    codes = list(codes or JUDGE_CODES)
    out = {}
    aborted = threading.Event()

    def one(code):
        if aborted.is_set():
            return {"ok": False, "skipped": "已判定网络不可用，跳过"}
        src_path = os.path.join(HERE, "_judge_in_%s.json" % code)
        try:
            with open(src_path, encoding="utf-8") as f:
                raw = f.read()
            body = json.loads(raw)
        except (OSError, ValueError) as e:
            return {"ok": False, "error": "输入快照缺失或损坏：%r" % e}
        try:
            body, rule_sha = inject_rules(body)
        except CliError as e:
            # 规则源坏了就整批不算。绝不能拿旧规则算出一批"看起来正常"的结论。
            return {"ok": False, "error": str(e), "code": e.code}
        fp = _judge_fp(raw, rule_sha)
        s = snap_read(code, fp)
        if not force and s:
            return {"ok": True, "from": "snapshot", "at": s.get("at")}
        last = None
        for attempt in range(retry + 1):
            t0 = time.time()
            try:
                res = call_judge(body)
            except CliError as e:
                last = str(e)
                if "network" in str(e).lower() or "无法连接" in str(e):
                    # 网络不可用不是这一家的问题，后面几家一样会失败。
                    # 不设这道闸，六家 × 两遍 × 每次一分钟，能在启动期空转十几分钟。
                    aborted.set()
                    break
                continue
            snap_write(code, fp, res)
            return {"ok": True, "from": "live", "attempt": attempt + 1,
                    "ms": int((time.time() - t0) * 1000)}
        return {"ok": False, "error": last}

    def wrap(code):
        try:
            return code, one(code)
        except OSError as e:
            return code, {"ok": False, "error": "写快照失败：%r" % e}

    if not codes:
        return out
    with futures.ThreadPoolExecutor(max_workers=max(1, min(workers, len(codes)))) as ex:
        for code, r in ex.map(wrap, codes):
            out[code] = r
    return out


def submit_extract(payload):
    for k in ("batch_id", "side", "fields"):
        if k not in payload:
            raise CliError("缺字段 %s" % k)
    if payload["side"] not in ("tender", "bid"):
        raise CliError("side 只能是 tender 或 bid")
    if not isinstance(payload["fields"], list) or not payload["fields"]:
        raise CliError("fields 必须是非空数组")
    if len(payload["fields"]) > 6:
        raise CliError("单次最多 6 个字段（实测 11 字段会触发 invalid_response）",
                       "too_many_fields")
    jid = uuid.uuid4().hex[:12]
    with _jobs_lock:
        _jobs[jid] = {"job_id": jid, "state": "queued", "queued_at": now(),
                      "request": payload}
        _stats["jobs"] += 1
    _queue.put((jid, payload))
    return jid


GATE_HTML = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>演示环境 · 需要访问口令</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
 body{margin:0;height:100vh;display:flex;align-items:center;justify-content:center;
      background:#f5f6f8;color:#1b1f24;font:15px/1.7 -apple-system,"Segoe UI","Microsoft YaHei",sans-serif}
 .box{width:380px;max-width:88vw;background:#fff;border:1px solid #e3e6ea;border-radius:10px;
      padding:26px 26px 20px;box-shadow:0 6px 24px rgba(16,24,40,.06)}
 .t{font-size:16px;font-weight:600;margin-bottom:6px}
 .s{font-size:13px;color:#5b6570;margin-bottom:16px}
 input{width:100%;box-sizing:border-box;padding:10px 12px;border:1px solid #d5dae0;border-radius:7px;
       font-size:14px;outline:none}
 input:focus{border-color:#3b6fd4}
 button{margin-top:12px;width:100%;padding:10px;border:0;border-radius:7px;background:#2f5fd0;
        color:#fff;font-size:14px;cursor:pointer}
 button:hover{background:#2752b4}
 .f{margin-top:10px;font-size:12px;color:#c0392b;min-height:16px}
 .n{margin-top:14px;font-size:12px;color:#8a929b;border-top:1px solid #eef0f2;padding-top:10px}
</style></head><body>
<div class="box">
  <div class="t">评标辅助工作台 · 演示环境</div>
  <div class="s">这是临时演示地址，需要访问口令才能进入。口令请向演示方索取。</div>
  <input id="k" placeholder="请输入访问口令" autocomplete="off" autofocus>
  <button onclick="go()">进入</button>
  <div class="f" id="msg"></div>
  <div class="n">演示结束后该地址即失效，页面内不含任何真实投标数据。</div>
</div>
<script>
function go(){
  var v=(document.getElementById('k').value||'').trim();
  if(!v){document.getElementById('msg').textContent='请输入口令';return;}
  location.replace('?k='+encodeURIComponent(v));
}
document.getElementById('k').addEventListener('keydown',function(e){if(e.key==='Enter')go();});
</script></body></html>"""


def client_ip(h):
    """隧道过来的请求，client_address 全是 127.0.0.1，真来源在 CF 头里。"""
    for k in ("CF-Connecting-IP", "X-Forwarded-For", "X-Real-IP"):
        v = h.headers.get(k)
        if v:
            return v.split(",")[0].strip()
    return h.client_address[0]


def rate_ok(ip):
    """每分钟滑动窗口。公网上没有这道闸，一个脚本就能把平台函数打到限流。"""
    t = time.time()
    with _rate_lock:
        hist = [x for x in _rate.get(ip, []) if t - x < 60]
        if len(hist) >= RATE_PER_MIN:
            _rate[ip] = hist
            return False
        hist.append(t)
        _rate[ip] = hist
    return True


class Handler(BaseHTTPRequestHandler):
    server_version = "sgcc-bff/0.1"
    # 默认是 HTTP/1.0：每个响应后关连接。本机直连看不出问题，但隧道会复用连接，
    # 于是 cloudflared 日志里出现 "wsarecv: aborted by the software in your host machine"，
    # 客户那边就表现为 Cloudflare 1033 或空响应。改成 HTTP/1.1 并保证每个响应都有
    # Content-Length（下面 _send / _html 都设了），连接才能被正常复用。
    protocol_version = "HTTP/1.1"
    timeout = 30          # keep-alive 不能无限挂，否则线程只增不减

    def _send(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", ALLOW_ORIGIN)
        self.send_header("Access-Control-Allow-Headers", "Content-Type, X-Demo-Token")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Robots-Tag", "noindex, nofollow")
        # 明确告知"这一轮结束就关连接"。不写这行而直接断，cloudflared 会当成
        # 连接被对端异常中止（日志里的 wsarecv abort），客户侧就是 1033。
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        self.wfile.write(body)

    def _html(self, code, text):
        raw = text.encode("utf-8")
        body, gz = raw, False
        # 页面是 171KB 单文件（数据内嵌，为了离线可直开），经隧道首屏要 4~5 秒。
        # gzip 后约 1/4，客户点开链接的等待感差别很明显。
        if len(raw) > 4096 and "gzip" in (self.headers.get("Accept-Encoding") or "").lower():
            body, gz = gzip.compress(raw, 6), True
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if gz:
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Vary", "Accept-Encoding")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Robots-Tag", "noindex, nofollow")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        self.wfile.write(body)

    def _serve_page(self, path, q):
        """BFF 兼作页面宿主。

        隧道只映射一个端口；客户浏览器里的 `http://127.0.0.1:8787` 指的是他自己
        那台机器。只有把页面和接口放进同一个 origin，前端才能用相对路径取数，
        跨端口硬编码地址和 CORS 的问题一起消失。
        """
        if PUBLIC and DEMO_TOKEN:
            got = q.get("k") or self.headers.get("X-Demo-Token") or ""
            if got != DEMO_TOKEN:
                return self._html(403, GATE_HTML)
        if path not in ("/", "/w3", "/w3.html", "/index.html"):
            return self._html(404, "<meta charset=utf-8><title>404</title><h1>404</h1>")
        try:
            with open(PAGE_FILE, encoding="utf-8") as fh:
                html = fh.read()
        except OSError:
            return self._html(500, "<meta charset=utf-8><h1>页面文件缺失</h1>")
        return self._html(200, html)

    def _auth(self):
        if not DEMO_TOKEN:
            return True
        return self.headers.get("X-Demo-Token") == DEMO_TOKEN

    def send_error(self, code, message=None, explain=None):
        """接住 http.server 自己发的错误（400/414/431 等）。

        这类错误发生在 parse_request 阶段，比 do_POST 更早，默认会返回一个
        Python 风格 HTML 页面。演示时客户看到 "Error response / Bad request syntax"
        会以为系统坏了。这里改成：原样记进审计（含原始请求行，便于事后定位），
        客户端拿到的是一句能读懂的 JSON。
        """
        try:
            audit({"kind": "http_error", "code": int(code), "message": message or "",
                   "requestline": (getattr(self, "requestline", "") or "")[:300],
                   "ua": (self.headers.get("User-Agent") if self.headers else "") or "",
                   "ip": client_ip(self)})
        except Exception:
            pass
        try:
            body = json.dumps({"error": "http_%d" % int(code),
                               "message": message or "",
                               "hint": "请求未被处理，可重试一次；持续失败请看 _bff 审计"},
                              ensure_ascii=False).encode("utf-8")
            self.wfile.write(
                b"HTTP/1.1 %d %s\r\n"
                b"Content-Type: application/json; charset=utf-8\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Cache-Control: no-store\r\n"
                b"Connection: close\r\n\r\n" + body)
            self.close_connection = True
        except Exception:
            pass

    def _q(self):
        from urllib.parse import parse_qs, urlparse
        u = urlparse(self.path)
        return u.path, {k: v[0] for k, v in parse_qs(u.query).items()}

    def do_OPTIONS(self):
        self._send(204, {})

    def do_GET(self):
        path, q = self._q()
        if not path.startswith("/api/"):
            return self._serve_page(path, q)
        if not rate_ok(client_ip(self)):
            return self._send(429, {"error": "too_many_requests", "hint": "每分钟上限 %d 次" % RATE_PER_MIN})
        if path == "/api/health":
            ok, cli_ver = True, ""
            try:
                st = run_cli(["auth", "status", "--json"], timeout=40)
                cli_ver = (st.get("account") or {}).get("name") or ""
            except CliError as e:
                ok, cli_ver = False, str(e)
            out = {"ok": ok, "at": now(), "kbs": len(KBS),
                   "content_cache": len(_content_cache),
                   "full_scan": dict(_full_scan_state)}
            if not PUBLIC:
                # 公网模式下不吐账号名/统计——这是演示机，不是运维面板。
                out["account"] = cli_ver
                out["stats"] = _stats
            return self._send(200, out)
        if not self._auth():
            return self._send(401, {"error": "token_required"})
        try:
            if path == "/api/kbs":
                items = [{"id": k, **v} for k, v in KBS.items()]
                return self._send(200, {"items": items, "_source": "allowlist"})

            if path == "/api/docs":
                kb = q.get("kb", "")
                check_kb(kb)
                val, src = cached("docs:" + kb, 120, lambda: run_cli(
                    ["document", "list", "--kb", kb, "--json", "--limit", "100"]))
                return self._send(200, {"items": val.get("items", []), "_cache": src})

            if path == "/api/search":
                kb, qs = q.get("kb", ""), q.get("q", "").strip()
                check_kb(kb)
                if not qs:
                    return self._send(400, {"error": "缺 q 参数"})
                val, src = cached("search:%s:%s" % (kb, qs), 60, lambda: run_cli(
                    ["search", qs, "--kb", kb, "--json", "--limit", str(q.get("limit", "8"))]))
                groups = val.get("groups") or []
                hits = []
                for g in groups:
                    for it in g.get("items", []):
                        # 检索返回的是 excerpt（正文片段）+ handle（回读句柄），
                        # 不是全文；要点开看原文得再调 /api/read?handle=...
                        hits.append({"rank": it.get("rank"),
                                     "doc": it.get("document_title"),
                                     "section": it.get("section"),
                                     "lines": "%s-%s" % (it.get("start_line"), it.get("end_line")),
                                     "quality": it.get("match_quality"),
                                     "handle": it.get("handle"),
                                     "excerpt": it.get("excerpt") or ""})
                return self._send(200, {"query": qs, "kb": kb, "hits": hits,
                                        "total": len(hits), "_cache": src})

            if path == "/api/grep":
                # 精确检索：RE2 逐行扫描，返回行号而不是语义相似度。
                # 这条路存在的唯一理由是「能负责任地说没有」——
                # 零命中且 coverage=complete 才是真的库内没有；语义检索在同样情况下
                # 会拿 medium 级的相近内容凑答案，那是这套演示最不能出的错。
                # kb 允许逗号分隔多个，逐个过白名单后重复 --kb。
                kbs = [x for x in (q.get("kb") or "").split(",") if x.strip()]
                qs = (q.get("q") or q.get("text") or "").strip()
                if not kbs:
                    return self._send(400, {"error": "缺 kb 参数"})
                for one in kbs:
                    check_kb(one.strip())
                kbs = [x.strip() for x in kbs]
                if not qs:
                    return self._send(400, {"error": "缺 q 参数"})
                lim = max(1, min(int(q.get("limit") or 100), 100))
                exc = max(40, min(int(q.get("excerpt") or 400), 1000))
                # 默认拉满 limit/max-count：total_match_count 是「本次扫到的量」，
                # limit 给小时它会提前停（同一个词 limit=5 得 28、limit=30 得 59）。
                # 那样的数字不能当「共 N 处」用，所以宁可多拉再把截断如实标出来。
                args = ["grep", qs, "--json",
                        "--limit", str(lim), "--excerpt", str(exc),
                        "--max-count", str(max(1, min(int(q.get("max_count") or 20), 20)))]
                for one in kbs:
                    args += ["--kb", one]
                if q.get("context"):
                    args += ["--context", str(max(0, min(int(q["context"]), 10)))]
                if q.get("case_sensitive") in ("1", "true", "True"):
                    args.append("--case-sensitive")

                val, src = cached("grep:%s:%s:%d" % (",".join(kbs), qs, lim), 300,
                                  lambda: run_cli(args))
                scans = val.get("scans") or []
                sc = scans[0] if scans else {}
                # 多库时要整体判断：任一库没扫完，就不能说「全库没有」
                cov = "complete"
                for s in scans:
                    if s.get("coverage") != "complete":
                        cov = s.get("coverage") or "partial"
                        break
                if not scans:
                    cov = None
                hits, trunc = [], False
                for g in val.get("groups") or []:
                    gh = g.get("handle")
                    for m in g.get("matches") or []:
                        hits.append({
                            "kb": g.get("knowledge_base_id"),
                            "doc": g.get("document_title"),
                            "line": m.get("line"),
                            "section": m.get("section"),
                            "excerpt": m.get("excerpt") or "",
                            # 组头句柄 + :行号 就是 apemind read 的合法句柄，
                            # 前端「回读原文」直接透传给 /api/read。
                            "handle": ("%s:%s" % (gh, m.get("line"))) if gh else None,
                            "more_in_line": m.get("extra_matches_in_line") or 0})
                    if g.get("matches_truncated") or g.get("scan_truncated"):
                        trunc = True
                return self._send(200, {
                    "query": qs, "kbs": kbs,
                    # 两个概念必须分开，混用会说错话：
                    #   total_exact  —— 扫描是否走完。True 时 total 就是「库内共 N 处」。
                    #   hits_truncated —— 返回条数是否被每篇 max-count 截掉，
                    #                     True 时只能说「展示前 count 条」，不能说「共 count 处」。
                    "total": val.get("total_match_count", 0),
                    "total_exact": cov == "complete",
                    "coverage": cov,
                    # complete=False 时前端必须说「未扫完」，不能说「没有」
                    "complete": cov == "complete",
                    "hits_truncated": trunc,
                    "scanned_docs": sum((s.get("docs_scanned") or 0) for s in scans),
                    "matched_docs": sum((s.get("docs_matched") or 0) for s in scans),
                    "hits": hits, "count": len(hits),
                    "_cache": src})

            if path == "/api/read":
                h = q.get("handle", "").strip()
                if not h:
                    return self._send(400, {"error": "缺 handle 参数"})
                if not h.startswith("doc"):
                    return self._send(400, {"error": "handle 格式不合法"})
                val, src = cached("read:" + h, 300,
                                  lambda: run_cli(["read", h, "--json"]))
                # 真实结构：data.items[0] = {document_title, section, start_line,
                #   end_line, total_lines, lines: ["第86行…", "第87行…"]}
                ent = (val.get("items") or [{}])[0]
                raw = ent.get("lines") or []
                base = ent.get("start_line") or 0
                lines = []
                for i, t in enumerate(raw):
                    lines.append({"no": (base + i) if base else None, "text": str(t)})
                return self._send(200, {
                    "handle": h,
                    "doc": ent.get("document_title"),
                    "section": ent.get("section"),
                    "total_lines": ent.get("total_lines"),
                    "lines": lines, "count": len(lines), "_cache": src})

            if path == "/api/matrix":
                if not os.path.exists(MATRIX):
                    return self._send(404, {"error": "矩阵未生成"})
                with open(MATRIX, encoding="utf-8") as f:
                    m = json.load(f)
                m["_source"] = "预计算快照 + 可 POST /api/refresh 重算"
                return self._send(200, m)

            if path.startswith("/api/job/"):
                jid = path.rsplit("/", 1)[-1]
                with _jobs_lock:
                    j = _jobs.get(jid)
                if not j:
                    return self._send(404, {"error": "job 不存在", "job_id": jid})
                return self._send(200, j)

            if path == "/api/jobs":
                with _jobs_lock:
                    items = sorted(_jobs.values(), key=lambda x: x.get("queued_at") or "",
                                   reverse=True)[:20]
                return self._send(200, {"items": items, "queue_depth": _queue.qsize()})

            if path == "/api/judge/snap":
                # 只读体检：每家的快照是否在、指纹是否还对得上当前输入。
                # 对不上 = 输入换过、快照过期，需要 POST /api/judge/refresh。
                items = []
                for c in JUDGE_CODES:
                    p = os.path.join(HERE, "_judge_in_%s.json" % c)
                    row = {"code": c, "input": os.path.exists(p)}
                    try:
                        with open(p, encoding="utf-8") as f:
                            raw = f.read()
                    except OSError:
                        row["stale"] = True
                        items.append(row)
                        continue
                    # 指纹要跟着规则源走：只按输入算，会让"改了规则但输入没变"的旧快照
                    # 在体检里显示 fresh，恰恰是最该报出来的那一种过期。
                    _rsrc = load_rule_source()
                    s = snap_read(c, _judge_fp(raw, _rsrc[2] if _rsrc else ""))
                    row["snapshot"] = bool(s)
                    row["stale"] = not s
                    if s:
                        row["at"] = s.get("at")
                        row["bytes"] = os.path.getsize(_snap_file(c))
                    items.append(row)
                return self._send(200, {"items": items,
                                        "all_fresh": all(x.get("snapshot") for x in items)})

            return self._send(404, {"error": "no_route", "path": path})
        except CliError as e:
            return self._send(400, {"error": str(e), "code": e.code})
        except Exception as e:                                    # noqa: BLE001
            return self._send(500, {"error": repr(e)})

    def do_POST(self):
        path, qs = self._q()
        if not rate_ok(client_ip(self)):
            return self._send(429, {"error": "too_many_requests", "hint": "每分钟上限 %d 次" % RATE_PER_MIN})
        if not self._auth():
            return self._send(401, {"error": "token_required"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n).decode("utf-8") or "{}") if n else {}
        except Exception:
            return self._send(400, {"error": "body 必须是 JSON"})
        snap_code, snap_fp = None, None
        if path == "/api/judge/by-code":
            code = resolve_code(body.get("code"))
            if code not in JUDGE_CODES:
                return self._send(400, {"error": "未知投标人代号", "code": "bad_code",
                                        "allowed": list(JUDGE_CODES)})
            src_path = os.path.join(HERE, "_judge_in_%s.json" % code)
            if not os.path.exists(src_path):
                return self._send(404, {"error": "该家没有现场抽取输入快照", "code": "no_input"})
            with open(src_path, encoding="utf-8") as fh:
                raw_in = fh.read()
            body = json.loads(raw_in)
            try:
                body, rule_sha = inject_rules(body)
            except CliError as e:
                return self._send(400, {"error": str(e), "code": e.code})
            snap_code, snap_fp = code, _judge_fp(raw_in, rule_sha)
            path = "/api/judge"        # 归一后走同一条判定链路，不复制第二份逻辑
        if path == "/api/judge/refresh":
            # 显式重算：演示里换了输入或想现场看一次真调用时用。
            # 六个并发跑，wall 约 2.5s；不并发就是 13s。
            codes = [resolve_code(c) for c in (body.get("codes") or JUDGE_CODES)]
            bad = [c for c in codes if c not in JUDGE_CODES]
            if bad:
                return self._send(400, {"error": "未知投标人代号", "code": "bad_code",
                                        "bad": bad, "allowed": list(JUDGE_CODES)})
            t0 = time.time()
            detail = presnap(codes, force=True)
            return self._send(200, {"recomputed": detail,
                                    "wall_ms": int((time.time() - t0) * 1000),
                                    "ok": all(v.get("ok") for v in detail.values())})
        if path == "/api/judge":
            # 快照优先：输入指纹一致就直接读盘（1~3ms），不 spawn CLI、不建 TLS。
            # ?refresh=1 强制走真调用，用来验"快照是不是真的旧了"。
            if snap_code and not qs.get("refresh"):
                t0 = time.time()
                s = snap_read(snap_code, snap_fp)
                if s:
                    res = dict(s["result"])
                    res["_source"] = "snapshot"
                    res["snapshot_at"] = s.get("at")
                    res["wall_ms"] = int((time.time() - t0) * 1000)
                    judge = res.get("judge") or {}
                    audit({"kind": "judge", "bidder": body.get("bidder"), "source": "snapshot",
                           "ip": client_ip(self), "public": PUBLIC,
                           "wall_ms": res["wall_ms"],
                           "fn_elapsed_ms": (res.get("run") or {}).get("elapsed_ms"),
                           "run_id": (res.get("run") or {}).get("run_id"),
                           "decided": judge.get("decided"), "fail_rows": judge.get("fail_rows")})
                    return self._send(200, res)
        if path == "/api/judge":
            raw = json.dumps(body, ensure_ascii=False)
            if len(raw.encode("utf-8")) > JUDGE_MAX_BYTES:
                return self._send(413, {"error": "输入过大（上限 512KB）", "code": "too_large"})
            if not (body.get("tender_fields") or body.get("bid_fields") or body.get("facts")):
                return self._send(400, {"error": "tender_fields / bid_fields / facts 至少给一个",
                                        "code": "empty_input"})
            # 上限按"用户给的输入"算，不按注入后的算；注入发生在校验之后。
            # by-code 分支已经注入过，这里不重复注入（重复也只是覆盖成同样的值，但会白算一遍）。
            if "rules" not in body:
                try:
                    body, _ = inject_rules(body)
                except CliError as e:
                    return self._send(400, {"error": str(e), "code": e.code})
                raw = json.dumps(body, ensure_ascii=False)
            key = "judge:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
            t0 = time.time()
            res, src = cached(key, JUDGE_TTL, lambda: call_judge(body))
            if snap_code and src == "live":
                # 只把真调用的结果落盘。缓存命中说明盘上已经有同指纹的快照，
                # 再写一遍是无意义 IO，也会把快照时间戳刷成"刚刚"。
                try:
                    snap_write(snap_code, snap_fp, res)
                except OSError:
                    pass
            res = dict(res)
            res["_source"] = src
            res["wall_ms"] = int((time.time() - t0) * 1000)
            judge = res.get("judge") or {}
            audit({"kind": "judge", "bidder": body.get("bidder"), "source": src,
                   "ip": client_ip(self), "public": PUBLIC,
                   "wall_ms": res["wall_ms"], "fn_elapsed_ms": (res.get("run") or {}).get("elapsed_ms"),
                   "run_id": (res.get("run") or {}).get("run_id"),
                   "decided": judge.get("decided"), "fail_rows": judge.get("fail_rows")})
            return self._send(200, res)
        if path == "/api/extract":
            if PUBLIC:
                # 公网上不放开抽取：一次 23 秒、直接烧平台函数额度。
                # 演示要的是"判定是现算的"，抽取用落盘快照即可（页面已标明 run 号）。
                return self._send(403, {"error": "extract_disabled_in_public",
                                        "hint": "公网演示只开放判定与只读检索"})
            try:
                jid = submit_extract(body)
            except CliError as e:
                return self._send(400, {"error": str(e), "code": e.code})
            audit({"kind": "extract", "job": jid,
                   "kb_side": body.get("side"), "fields": body.get("fields"),
                   "bidder": body.get("bidder")})
            return self._send(202, {"job_id": jid, "state": "queued",
                                    "poll": "/api/job/%s" % jid,
                                    "eta_s": 25, "note": "抽取中位 23 秒，请轮询"})
        return self._send(404, {"error": "no_route", "path": path})

    def log_message(self, fmt, *args):
        pass


def warmup():
    """启动后立刻骗一次 CLI 从冷态转热。

    实测：BFF 刚起来时第一次判定端到端 7.3s（进程冷启动 + 首次建连），
    第二次就落到 2s 左右。演示时第一次点击不能是 7 秒——那不是系统慢，
    是我们没预热。这里用一条规则的极小入参把链路走通，不写入任何缓存。
    """
    time.sleep(1.0)
    try:
        run_cli(["auth", "status", "--json"], timeout=40)
    except CliError:
        pass
    try:
        call_judge({"section_no": "warmup", "bidder": "warmup",
                    "facts": {"bid.seal_present": True},
                    "rule_ids": ["GW-HB-FJ-004"]})
        print("warmup ok")
    except CliError as e:
        print("warmup failed:", e)
        if "network" in str(e).lower() or "无法连接" in str(e):
            print("presnap skipped：网络不可用，等下一次请求或 /api/judge/refresh 再补")
            return

    # 六家判定快照：盘上有同指纹的直接复用，没有的并发算一遍。
    # 串行 6 × 2.2s≈13s，并发 2.5s 左右；这样第一个用户点进来读的就是盘。
    t0 = time.time()
    detail = presnap()
    ok = sum(1 for v in detail.values() if v.get("ok"))
    print("presnap %d/%d  %s  %d ms"
          % (ok, len(detail),
             ",".join("%s=%s" % (k, v.get("from") or "fail") for k, v in detail.items()),
             int((time.time() - t0) * 1000)))

    # HTTP 版新增：预热招标主库全文缓存，让第一次 /api/grep 尽快拿到
    # coverage=complete（"能负责任地说没有"）。后台跑，不阻塞监听。
    threading.Thread(target=_full_scan_bg,
                     args=("colcf0af5d45fd7ef6b",), daemon=True).start()


def main():
    if PUBLIC and not DEMO_TOKEN:
        raise SystemExit("公网模式必须设 BFF_TOKEN——否则等于把判定接口裸奔在互联网上")
    for _ in range(2):
        threading.Thread(target=job_worker, daemon=True).start()
    threading.Thread(target=warmup, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    print("BFF listening http://127.0.0.1:%d/  (kbs=%d, token=%s, public=%s, rate=%d/min)"
          % (PORT, len(KBS), "on" if DEMO_TOKEN else "off", "on" if PUBLIC else "off", RATE_PER_MIN))
    if PUBLIC:
        print("页面由本服务托管：http://127.0.0.1:%d/?k=<token>" % PORT)
    else:
        print("试：curl http://127.0.0.1:%d/api/health" % PORT)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")


if __name__ == "__main__":
    main()
