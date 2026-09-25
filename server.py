"""
arena2api - Arena.ai to OpenAI API Proxy
=========================================

极简设计：Chrome 扩展提供 reCAPTCHA token 和 cookies，
本服务器负责 OpenAI 格式转换和 arena.ai API 调用。

使用方式：
  1. pip install -r requirements.txt
  2. python server.py
  3. 安装 Chrome 扩展，打开 arena.ai
  4. 在 OpenAI 客户端中配置 http://localhost:9090/v1

=============================================================================
阅读导引（本文件按以下顺序组织，想直接看某块可以按标题搜索）
=============================================================================
  1) 配置区          —— 端口 / API_KEY / arena.ai 各接口地址 / reCAPTCHA sitekey
  2) uuid7()         —— 生成 arena.ai 需要的 UUIDv7 时间有序 ID
  3) Store 类        —— 进程内状态仓库，缓存扩展推来的 cookies / token / 模型表
  4) parse_access_token() —— 从 arena-auth cookie 里解出真正的 Bearer JWT
  5) FastAPI 应用 + CORS + 可选 API Key 鉴权
  6) 扩展端点        —— /v1/extension/push（收数据）、/v1/extension/status（看状态）
  7) OpenAI 端点     —— /v1/models、/v1/chat/completions（请求预处理 + 会话管理）
  8) stream_response()     —— arena SSE → OpenAI SSE 的“流式”转换器
  9) non_stream_response() —— 同一套解析逻辑，但把全部结果攒成一条完整响应
 10) /health 健康检查 与 启动入口

=============================================================================
核心数据流（为什么需要浏览器扩展）
=============================================================================
  OpenAI 客户端 ──POST /v1/chat/completions──▶ 本服务器
        扩展（arena.ai 页面内） ──POST /v1/extension/push──▶ 本服务器
        本服务器 ──携带 cookie + JWT + reCAPTCHA token──▶ arena.ai
        arena.ai ──自定义 SSE（a0:/ag:/ad:/a2:/a3:）──▶ 本服务器 ──OpenAI 格式──▶ 客户端

  关键点：arena.ai 的接口有 reCAPTCHA 风控，纯服务端无法伪造高分 token，
  所以 reCAPTCHA token 与 cookies 都由真实浏览器里的扩展抓取后“喂”给本服务器。
  服务器自己不保存任何账号信息，只做格式转换和状态转发。

  因此有一个硬约束：**必须保持 arena.ai 标签页打开**，
  扩展超过 120 秒没推送数据，Store.active 即为 False，接口会返回 503。
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import random
import re
import secrets
import time
import uuid
from typing import Optional
from urllib.parse import unquote

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import StreamingResponse, JSONResponse

# ============================================================
# 日志
# ============================================================
# 环境变量 DEBUG 非空时输出 DEBUG 级别日志（例如 DEBUG=1 python server.py）
logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG") else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("arena2api")


# 扩展心跳很勤（push 约 30s，弹窗开着时 status 约 2s），access log 刷屏容易
# 让人点选 Windows 终端触发 Quick Edit，整进程卡死。静默这两条即可。
class _QuietExtensionAccess(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "/v1/extension/push" not in msg and "/v1/extension/status" not in msg


logging.getLogger("uvicorn.access").addFilter(_QuietExtensionAccess())

# ============================================================
# 配置
# ============================================================
# PORT：监听端口，默认 9090；容器/多实例部署时用环境变量覆盖
PORT = int(os.environ.get("PORT", "9090"))

# API_KEY：可选。为空表示本服务不鉴权（默认，适合本机使用）；
# 一旦设置，所有 OpenAI 兼容端点都要求请求头 `Authorization: Bearer <API_KEY>`。
API_KEY = os.environ.get("API_KEY", "").strip()

# arena.ai 的接口地址（Next.js 的 Route Handler 路径）
ARENA_BASE = "https://arena.ai"
# 创建一次新的评测（= 新建一个对话/会话），首次请求走这里
ARENA_CREATE_EVAL = f"{ARENA_BASE}/nextjs-api/stream/create-evaluation"
# 往已有评测里追加消息（= 继续同一段对话），后续多轮走这里
ARENA_POST_EVAL = f"{ARENA_BASE}/nextjs-api/stream/post-to-evaluation"  # + /{id}

# reCAPTCHA（Google reCAPTCHA Enterprise）站点公钥。
# 站点公钥本身就是公开值，写在页面 JS 里，扩展用它在浏览器上下文中执行 grecaptcha.enterprise.execute()
# 拿到 token，再推给本服务器；这里只是记录一份，供对照/将来服务端复用。
RECAPTCHA_V3_SITEKEY = "6LeTGMcsAAAAALuIlkVwIxaAuZA8VledA6d3Nnb0"

# ============================================================
# UUIDv7
# ============================================================
def uuid7() -> str:
    """
    生成符合 RFC 9562 的 UUIDv7（前 48 位为毫秒时间戳，剩余为随机位）。

    为什么不用 uuid.uuid4()？arena.ai 侧用这些 ID 做消息/会话主键，
    时间有序的 v7 既能全局唯一，又能让服务端按 ID 天然排序、便于分库分表，
    并且和浏览器端 crypto.randomUUID() 之外的自研实现保持同构。

    位布局（共 128 bit）：
        [ 48 bit 毫秒时间戳 ][ 4 bit 版本号=7 ][ 12 bit 随机 ][ 2 bit 变体=10 ][ 62 bit 随机 ]
    """
    ts = int(time.time() * 1000)          # 48 位毫秒时间戳
    ra = secrets.randbits(12)             # 版本号之后的 12 位随机
    rb = secrets.randbits(62)             # 变体位之后的 62 位随机
    # 用大整数把三段拼起来：时间戳左移 80 位 | (0x7xxx) 左移 64 位 | (0b10xx…)
    u = ts << 80 | (0x7000 | ra) << 64 | (0x8000000000000000 | rb)
    h = f"{u:032x}"                       # 补零成 32 位十六进制
    # 按 8-4-4-4-12 切成标准 UUID 字符串格式
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


# ============================================================
# Token / Cookie Store（从扩展接收）
# ============================================================
class Store:
    """
    进程内全局状态（单例 store），保存扩展推送上来的一切运行时凭据。

    生命周期：完全由扩展驱动——扩展每 30 秒（以及拿到新 token 时）POST 一次
    /v1/extension/push，这里就把最新数据覆盖/追加进内存。

    注意：这是**内存态**，进程重启后全部丢失，需要扩展重新推送一次。
    数据不落盘，也不做持久化，避免凭据残留在磁盘上。
    """

    def __init__(self):
        self.cookies: dict = {}        # 扩展读到的 document.cookie（键值对形式）
        self.auth_token: str = ""      # 扩展单独提取的 auth token（可能是 JWT 或 base64-JSON）
        self.cf_clearance: str = ""    # Cloudflare 通行 cookie，仅作状态展示
        self.v3_tokens: list = []      # reCAPTCHA V3 token 池：[{token, action, ts}]，FIFO 队列
        self.v2_token: Optional[dict] = None   # reCAPTCHA V2 token（点击式验证，兜底用），一次性
        self.last_push: float = 0      # 最近一次接收推送的时间戳，用于判定“扩展是否在线”
        self.models: list = []         # 原始模型列表（扩展从页面 Next.js 数据里抠出来的）
        self.text_models: dict = {}    # publicName -> id，纯文本输出模型
        self.image_models: dict = {}   # publicName -> id，可输出图片的模型
        self.vision_models: list = []  # 支持图片输入的模型名（当前仅记录，未在请求里使用）
        self.next_actions: dict = {}   # 页面里的 Next.js Server Action 名 -> hash（备用）
        self.sessions: dict = {}       # 逻辑会话键 -> arena 评测 ID（eval_id），用来续接多轮

    @property
    def active(self) -> bool:
        """
        扩展是否“在线”：只要 120 秒内收到过推送就算在线。

        120 秒这个阈值 ≈ 扩展推送间隔（30s）的 4 倍，允许丢几次推送；
        离线时 /v1/chat/completions 会直接返回 503（见下方判断），
        以免拿着过期 cookie/token 去撞 arena.ai。
        """
        return self.last_push > 0 and (time.time() - self.last_push < 120)

    def push(self, data: dict):
        """
        处理扩展 POST 上来的一批数据（字段都是可选的，只更新带了的那部分）。

        入参 data 典型结构：
            {
              "cookies": {"arena-auth-prod-v1": "...", ...},
              "auth_token": "...",
              "cf_clearance": "...",
              "v3_tokens": [{"token": "...", "action": "chat_submit", "age_ms": 1234}],
              "v2_token": {"token": "...", "age_ms": 5000},
              "models": [{"publicName": "...", "id": "...", "capabilities": {...}}],
              "next_actions": {"xxx": "hash"}
            }
        """
        self.last_push = time.time()   # 先刷新心跳，哪怕本批数据都是空的
        if data.get("cookies"):
            self.cookies = data["cookies"]
        if data.get("auth_token"):
            self.auth_token = data["auth_token"]
        if data.get("cf_clearance"):
            self.cf_clearance = data["cf_clearance"]

        # ---- reCAPTCHA V3 token 入池（滚动池，最多 10 个）----
        if data.get("v3_tokens"):
            for t in data["v3_tokens"]:
                tok = t.get("token", "")
                if not tok or len(tok) < 20:
                    continue      # 明显不是真 token（空串/占位），丢弃
                age = t.get("age_ms", 0)
                if age > 120000:
                    continue      # 已经被扩展放了两分钟以上，视为过期，丢弃
                if any(x["token"] == tok for x in self.v3_tokens):
                    continue      # 去重：同一个 token 反复推送只保留一份
                self.v3_tokens.append({
                    "token": tok,
                    "action": t.get("action", "chat_submit"),   # token 对应的 reCAPTCHA action 名
                    # 用扩展给出的 age_ms 反推 token 的“生成时刻”，保证过期判断基于真实时间
                    "ts": time.time() - age / 1000,
                })
            # 超出池容量时丢最旧的（FIFO）
            while len(self.v3_tokens) > 10:
                self.v3_tokens.pop(0)

        # ---- reCAPTCHA V2 token（一次性，仅当仍是新鲜的才接受）----
        if data.get("v2_token"):
            v2 = data["v2_token"]
            if v2.get("token") and v2.get("age_ms", 0) < 120000:
                self.v2_token = {
                    "token": v2["token"],
                    "ts": time.time() - v2.get("age_ms", 0) / 1000,
                }

        # ---- 模型清单（每次全量覆盖）----
        if data.get("models"):
            self._update_models(data["models"])

        # ---- Next.js Server Action 映射表（增量合并，当前未直接使用）----
        if data.get("next_actions"):
            self.next_actions.update(data["next_actions"])

    def _update_models(self, models: list):
        """
        把扩展抓到的原始模型数组，按能力拆成三张表，供 /v1/models 和模型解析使用。

        判定依据是 arena.ai 模型对象的 capabilities：
          outputCapabilities 含 "text"  → 文本输出模型（text_models）
          outputCapabilities 含 "image" → 图片输出模型（image_models）
          inputCapabilities  含 "image" → 支持图片输入（vision_models，仅记录）

        key 用 publicName（就是客户端要填的 model 名，如 "GPT-4o"），
        value 用 id（arena.ai 内部的模型 ID），真正发给 arena.ai 的是后者。
        """
        self.models = models
        self.text_models = {}
        self.image_models = {}
        self.vision_models = []
        for m in models:
            name = m.get("publicName", "")
            mid = m.get("id", "")
            caps = m.get("capabilities", {})
            out_caps = caps.get("outputCapabilities", [])
            in_caps = caps.get("inputCapabilities", [])
            if "text" in out_caps:
                self.text_models[name] = mid
            if "image" in out_caps:
                self.image_models[name] = mid
            if "image" in in_caps:
                self.vision_models.append(name)

    def pop_v3_token(self) -> Optional[str]:
        """
        从池中取一个可用的 V3 token（FIFO，取出即删除，避免重复使用被风控）。

        每次调用先做一遍惰性清理：把生成超过 120 秒的 token 全部剔除
        （reCAPTCHA token 有效期约 2 分钟，过期的 token 送过去只会被拒绝）。
        池空了返回 None，调用方会退化为使用 V2 token 或不带 token 直接请求。
        """
        now = time.time()
        self.v3_tokens = [t for t in self.v3_tokens if now - t["ts"] < 120]
        if not self.v3_tokens:
            return None
        return self.v3_tokens.pop(0)["token"]

    def pop_v2_token(self) -> Optional[str]:
        """取出 V2 token（一次性：取完置 None）。已过期则返回 None。"""
        if not self.v2_token:
            return None
        if time.time() - self.v2_token["ts"] > 120:
            self.v2_token = None
            return None
        tok = self.v2_token["token"]
        self.v2_token = None
        return tok

    def build_cookie_header(self) -> str:
        """
        组装发给 arena.ai 的 Cookie 请求头。

        难点：arena.ai 用 Supabase SSR 存会话，认证 cookie 太大时浏览器会把它拆成
        `arena-auth-prod-v1.0`、`arena-auth-prod-v1.1` … 多个分片。
        各分片必须按序号拼接才能还原出完整的 `arena-auth-prod-v1`，
        所以这里：没有完整 cookie 时，就从 .0 开始顺序收集分片再拼起来。

        注意：拼接依赖序号连续（遇到第一个缺失的 .N 就停止），
        如果浏览器写入的分片不连续，这里会拼出错误的 cookie 值。
        """
        cookies = dict(self.cookies)
        # Supabase SSR 分片：拼回 arena-auth-prod-v1，浏览器 Cookie 里两种都可能有
        if not cookies.get("arena-auth-prod-v1"):
            chunks = []
            i = 0
            while True:
                part = cookies.get(f"arena-auth-prod-v1.{i}")
                if not part:
                    break
                chunks.append(part)
                i += 1
            if chunks:
                cookies["arena-auth-prod-v1"] = "".join(chunks)
        # 拼成标准的 "k=v; k=v" 形式
        return "; ".join(f"{k}={v}" for k, v in cookies.items())

    def access_token(self) -> str:
        """从 arena-auth cookie 取出 JWT，不要把整段 base64-JSON 当 Bearer。"""
        # 优先用扩展单独提取的 auth_token；没有就回退到 cookie 里的 arena-auth-prod-v1
        raw = self.auth_token or self.cookies.get("arena-auth-prod-v1", "")
        if not raw:
            # 同样要处理分片情况：把 arena-auth-prod-v1.0/.1/... 拼回整段
            chunks = []
            i = 0
            while True:
                part = self.cookies.get(f"arena-auth-prod-v1.{i}")
                if not part:
                    break
                chunks.append(part)
                i += 1
            raw = "".join(chunks)
        return parse_access_token(raw)

    def status(self) -> dict:
        """
        给 /v1/extension/status 和 /health 用的诊断快照（不含任何凭据明文）。
        扩展弹窗 / 排障时靠这些字段判断“扩展有没有连上、token 够不够、模型有没有抓到”。
        """
        now = time.time()
        valid_v3 = [t for t in self.v3_tokens if now - t["ts"] < 120]
        return {
            "active": self.active,                                    # 扩展是否在线
            "last_push_ago": round(now - self.last_push, 1) if self.last_push else None,
            "v3_tokens": len(valid_v3),                               # 当前可用 V3 token 数
            "has_v2": bool(self.v2_token and now - self.v2_token["ts"] < 120),
            "has_auth": bool(self.auth_token),                        # 有没有拿到 auth token
            "has_cf": bool(self.cf_clearance),
            "text_models": len(self.text_models),
            "image_models": len(self.image_models),
            "next_actions": list(self.next_actions.keys()),
            "cookies": list(self.cookies.keys()),                     # 只列 cookie 名，不列值
        }


# 全局唯一状态实例：所有请求共用（多 worker 部署时每个进程各有一份，会各自需要扩展推送）
store = Store()


def parse_access_token(raw: str) -> str:
    """
    从 cookie/扩展给的一坨字符串里，解析出真正能放进 `Authorization: Bearer` 的 access_token。

    arena.ai 的 Supabase 会话 cookie 是加密/编码后的结构，本函数兼容两种形态：

    1) `base64-<base64url(JSON)>`：
       JSON 里含 `access_token` 或 `accessToken` 字段，取出该字段作为 JWT。
       （先尝试 urlsafe 解码，再退回标准 base64，兼容不同的 padding/字符集写法）
    2) 直接就是 JWT：以 `eyJ` 开头（base64url 的 `{"` 特征）且至少含 2 个 `.`。

    解析失败一律返回空串，调用方会记录 "No access_token parsed" 警告（请求仍会发出）。
    """
    if not raw:
        return ""
    s = unquote(raw.strip())     # cookie 值可能是 URL-encode 过的，先解码
    if s.startswith("base64-"):
        blob = s[7:]
        pad = "=" * ((4 - len(blob) % 4) % 4)   # base64 长度需为 4 的倍数，补齐 padding
        data = None
        for decoder in (base64.urlsafe_b64decode, base64.b64decode):
            try:
                data = json.loads(decoder(blob + pad))
                break
            except Exception:
                continue
        if not isinstance(data, dict):
            return ""
        # 兼容 camelCase / snake_case 两种字段命名
        return data.get("access_token") or data.get("accessToken") or ""
    if s.count(".") >= 2 and s.startswith("eyJ"):
        return s
    return ""


# ============================================================
# FastAPI
# ============================================================
app = FastAPI(title="arena2api", version="1.0.0")

# 全开 CORS：本服务面向本机/局域网自用，需要让浏览器里的扩展和各类 Web 客户端直连。
# 注意：allow_origins=["*"] 与 allow_credentials=True 同时出现时，浏览器实际不会带凭据；
# 要暴露到公网请同时收紧 CORS 并设置 API_KEY。
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def verify_api_key(request: Request):
    """Optional API key auth for OpenAI endpoints.

    If API_KEY is set, require: Authorization: Bearer <API_KEY>
    """
    # 未配置 API_KEY 时完全跳过（本机自用最省事）
    if not API_KEY:
        return

    auth_header = request.headers.get("authorization", "")
    expected = f"Bearer {API_KEY}"
    # 整串精确比对：客户端把 API Key 填成任意非空值是不行的，必须与服务端一致
    if auth_header != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ============================================================
# 扩展端点
# ============================================================

# 说明：这两个端点**不做 API_KEY 鉴权**（扩展自身不带密钥）。
# 也就是说，任何能访问本端口的进程都能往 Store 里灌 cookies/token。
# 因此不要把本服务直接暴露到公网，或在前面加一层反向代理做访问控制。
@app.post("/v1/extension/push")
async def extension_push(request: Request):
    """接收扩展推送的 token、cookies、models"""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    store.push(data)
    # 告诉扩展“池里可用 token 不足 3 个”，扩展据此决定是否立刻再取一批 token
    need = len([t for t in store.v3_tokens if time.time() - t["ts"] < 120]) < 3
    return {
        "status": "ok",
        "need_tokens": need,          # 扩展根据这个字段触发主动补 token
        "v3_count": len(store.v3_tokens),
    }


@app.get("/v1/extension/status")
async def extension_status():
    """返回 Store 诊断快照，供扩展弹窗展示连接状态 / token 池 / 模型数量"""
    return store.status()


# ============================================================
# OpenAI 兼容端点
# ============================================================
@app.get("/v1/models")
async def list_models(request: Request):
    """列出可用模型"""
    # 请求日志：记录来源 IP 与 UA，方便区分是哪个客户端在拉模型列表
    log.info("GET /v1/models from %s ua=%s", request.client.host if request.client else "?", request.headers.get("user-agent", "")[:80])
    verify_api_key(request)
    # 文本模型 + 图片模型合并成一张表（模型名可能重名，后者覆盖前者，实际几乎不冲突）
    all_models = {}
    all_models.update(store.text_models)
    all_models.update(store.image_models)
    data = []
    # 排序输出，保证同一份模型清单每次顺序一致（便于客户端缓存/对比）
    for name in sorted(all_models.keys()):
        data.append({
            "id": name,               # 注意：对外暴露的是 publicName，不是 arena 内部 id
            "object": "model",
            "created": 0,             # 无真实创建时间，填 0 占位
            "owned_by": "arena.ai",
        })
    if not data:
        # 返回一个占位模型：此时通常是扩展还没连上/模型还没抓到，
        # 返回占位项而不是空列表，避免部分客户端因空列表报错退出
        data.append({
            "id": "waiting-for-extension",
            "object": "model",
            "created": 0,
            "owned_by": "arena.ai",
        })
    log.info("GET /v1/models -> %d models", len(data))
    return {"object": "list", "data": data}


def session_key(request: Request, model_name: str) -> str:
    """
    解析用于归并多轮对话的会话键。

    兼容主流客户端会使用的会话头（按优先级）：
      - X-Session-Id       （OpenAI 兼容客户端通用写法）
      - X-Conversation-Id  （WorkBuddy / CodeBuddy 实际使用的头）
      - X-Chat-Id          （部分客户端使用）

    都没有时退化为「模型名哈希」：同一模型的所有请求落进同一个 arena 会话。
    注意该兜底策略有副作用——同一模型下不同客户端/不同轮次会共用同一段
    arena 上下文（会互相串话）；需要隔离请在客户端显式传上面任一头部。
    """
    for h in ("x-session-id", "x-conversation-id", "x-chat-id"):
        sid = request.headers.get(h, "").strip()
        if sid:
            return sid
    return hashlib.sha256(model_name.encode()).hexdigest()[:16]


def detect_client(request: Request) -> str:
    """
    通过 User-Agent 粗略识别客户端类型，仅用于个别响应格式微调
    （目前只有 "claude" 分支会额外塞 Anthropic 风格字段）。
    """
    ua = request.headers.get("user-agent", "").lower()
    if "claude" in ua or "anthropic" in ua:
        return "claude"
    if "gemini" in ua or "google" in ua:
        return "gemini"
    if "codex" in ua:
        return "codex"
    if "opencode" in ua:
        return "opencode"
    # NewAPI/OneAPI 通常使用标准 OpenAI 格式
    return "openai"


def _build_arena_payload(eval_id: str, model_id: str, prompt: str, modality: str,
                          v3_token: Optional[str], v2_token: Optional[str]) -> dict:
    """
    构造发给 arena.ai 的请求体。

    mode=direct-battle：单模型直聊（扩展打开的是 ?mode=direct）。
    不要带 modelBMessageId——那是 Battle/Side-by-side 双模型投票模式的字段。
    """
    payload = {
        "id": eval_id,
        "mode": "direct-battle",
        "modelAId": model_id,
        "userMessageId": uuid7(),
        "modelAMessageId": uuid7(),
        "userMessage": {
            "content": prompt,
            "experimental_attachments": [],   # 附件能力未实现（图片输入未透传），留空数组
            "metadata": {},
        },
        "modality": modality,
    }
    # 风控凭据二选一：带 V2 时显式把 V3 置 None（服务端会按此选择校验方式）
    if v2_token:
        payload["recaptchaV2Token"] = v2_token
        payload["recaptchaV3Token"] = None
    else:
        payload["recaptchaV3Token"] = v3_token
    return payload


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """
    核心端点：OpenAI 格式请求 → arena.ai 请求 → OpenAI 格式响应（流式或非流式）。

    处理顺序（也是本函数的分段依据）：
      1. 鉴权 → 解析 JSON → 识别客户端 / 提取 model、messages、stream
      2. 打日志（Authorization 与 Cookie 脱敏，只留前后缀/长度）
      3. 校验 messages、校验扩展是否在线（离线 503）
      4. 模型名 → arena 内部 model_id（精确匹配失败则模糊匹配，再失败 404）
      5. 组装 prompt：取最后一条 user 消息；新会话时才把 system 提示词拼进去
      6. 会话续接：命中 store.sessions 就 POST 到已有 eval，否则新建 eval（走 create-evaluation）
      7. 取 reCAPTCHA token（优先 V3，退而 V2，都没有就裸发并告警）
      8. 组装 arena 请求体 + 请求头（cookie、Bearer JWT、伪装 UA/origin/referer）
      9. 按 stream 分流到 stream_response() / non_stream_response()

    注意：会话的注册（store.sessions[skey] = eval_id）**不再在这里发生**，
    而是推迟到 arena.ai 真正返回 200 之后、在响应函数内部完成。这样一旦
    create-evaluation 中途失败/被取消，不会留下一个脏 eval_id 把后续请求带进 404。
    """
    verify_api_key(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    client_type = detect_client(request)
    model_name = body.get("model", "")
    messages = body.get("messages", [])
    stream = body.get("stream", False)

    # 打印客户端请求详情（Authorization 只留前后缀）
    # 脱敏原因：这些日志会打到控制台/日志文件，不能落明文密钥与完整 Cookie
    hdrs = {}
    for k, v in request.headers.items():
        lk = k.lower()
        if lk == "authorization" and v:
            # 形如 "Bearer sk-abc...xyz"，只保留前 12 和后 6 个字符
            hdrs[k] = v[:12] + "..." + v[-6:] if len(v) > 24 else "***"
        elif lk == "cookie" and v:
            hdrs[k] = f"<{len(v)} chars>"     # 只留长度，绝不落 cookie 内容
        else:
            hdrs[k] = v
    log.info(
        "Client request: client=%s model=%s stream=%s msgs=%d x-session-id=%r headers=%s",
        client_type,
        model_name,
        stream,
        len(messages),
        request.headers.get("x-session-id"),
        hdrs,
    )
    # 逐条打印消息摘要：多模态消息只取 text 片段，超过 120 字符截断
    for i, msg in enumerate(messages):
        content = msg.get("content", "")
        if isinstance(content, list):
            # OpenAI 多模态 content 形如 [{"type":"text","text":...}, {"type":"image_url",...}]
            preview = "\n".join(p.get("text", "") for p in content if p.get("type") == "text")
        else:
            preview = str(content or "")
        if len(preview) > 120:
            preview = preview[:120] + "..."
        log.info("  message[%d] role=%s content=%r", i, msg.get("role"), preview)

    if not messages:
        raise HTTPException(400, "messages is required")

    # 检查扩展是否连接
    # 拿不到新鲜 cookie/token 时直接失败，免得用过期凭据去打 arena.ai（会触发风控/无意义报错）
    if not store.active:
        raise HTTPException(503, "Extension not connected. Please open arena.ai in Chrome with the extension installed.")

    # 解析模型：先按 publicName 精确查 text/image 两张表
    model_id = store.text_models.get(model_name) or store.image_models.get(model_name)
    if not model_id:
        # 尝试模糊匹配：双向包含，兼容 "gpt-4o" vs "GPT-4o-2024-xx" 之类的写法差异
        for name, mid in {**store.text_models, **store.image_models}.items():
            if model_name.lower() in name.lower() or name.lower() in model_name.lower():
                model_id = mid
                model_name = name     # 回写为规范化后的真实名称，后续响应用的也是它
                break
    if not model_id:
        available = list(store.text_models.keys()) + list(store.image_models.keys())
        raise HTTPException(404, f"Model '{model_name}' not found. Available: {available[:20]}")

    # 提取最后一条 user 消息内容（arena 会话已保存历史，续接时只发最新一句）
    user_prompt = ""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, list):
                # 多模态消息
                text_parts = [p.get("text", "") for p in content if p.get("type") == "text"]
                user_prompt = "\n".join(text_parts)
            else:
                user_prompt = content
            break
    if not user_prompt:
        # 没有 user 消息（例如只有 system）时，退化为取最后一条消息的内容
        user_prompt = messages[-1].get("content", "")

    # 提取所有 system 消息（新会话 / 回退到 create 时才需要）
    system_parts = []
    for m in messages:
        if m.get("role") != "system":
            continue
        content = m.get("content", "")
        if isinstance(content, list):
            # 兼容 content 为数组的 system 消息（Anthropic 风格客户端会这样发）
            content = "\n".join(p.get("text", "") for p in content if p.get("type") == "text")
        if content:
            system_parts.append(content)
    system_prompt = "\n".join(system_parts)

    # 会话续接判断：命中 store.sessions 就 post 到已有 eval，否则新建
    skey = session_key(request, model_name)
    continuing = skey in store.sessions
    log.info("Session resolve: key=%r continue=%s eval_id=%s", skey, continuing, store.sessions.get(skey))

    # 获取 reCAPTCHA token：优先 V3（一次性，出池即删），没有则退回 V2
    v3_token = store.pop_v3_token()
    v2_token = store.pop_v2_token() if not v3_token else None
    # 裸发会被 arena 打成 429 {"error":"prompt failed"}；网页端总能带上新 token
    if not v3_token and not v2_token:
        raise HTTPException(
            503,
            "No reCAPTCHA token. Keep arena.ai tab open and wait for the extension to refill tokens.",
        )

    # 图片模型走 modality="image"，普通对话是 "chat"
    is_image = model_name in store.image_models
    modality = "image" if is_image else "chat"

    if continuing:
        # 续接：POST 到已有评测 ID，system 提示词不再重复发送（首轮已发过）
        eval_id = store.sessions[skey]
        url = f"{ARENA_POST_EVAL}/{eval_id}"
        arena_payload = _build_arena_payload(eval_id, model_id, user_prompt, modality, v3_token, v2_token)
        register_session = False

        # 404 回退工厂：如果 post 到已有会话发现它在 arena 侧不存在，
        # 就把这段对话当一个全新会话重新发起（带 system + 当前 user）。
        # 这里刻意重新弹一次 token——之前弹的可能已经在回退时消耗掉了。
        def make_fallback():
            new_eval_id = uuid7()
            full_prompt = (system_prompt + "\n\n" + user_prompt) if system_prompt else user_prompt
            new_v3 = store.pop_v3_token()
            new_v2 = store.pop_v2_token() if not new_v3 else None
            new_payload = _build_arena_payload(new_eval_id, model_id, full_prompt, modality, new_v3, new_v2)
            return new_eval_id, new_payload
        fallback_fn = make_fallback
    else:
        # 新会话：把所有 system 消息按顺序拼到 prompt 前面
        full_prompt = (system_prompt + "\n\n" + user_prompt) if system_prompt else user_prompt
        # 自己生成一个 UUIDv7 作为 arena 评测 ID——create-evaluation 接受客户端指定 ID
        eval_id = uuid7()
        url = ARENA_CREATE_EVAL
        arena_payload = _build_arena_payload(eval_id, model_id, full_prompt, modality, v3_token, v2_token)
        register_session = True
        fallback_fn = None

    # 请求头尽量与浏览器一致：origin/referer 指向该评测页面。
    # content-type 特意用 text/plain——arena.ai 前端就是这样发的（避免触发 preflight/校验）。
    headers = {
        "accept": "*/*",
        "content-type": "text/plain;charset=UTF-8",
        "origin": ARENA_BASE,
        "referer": f"{ARENA_BASE}/c/{eval_id}",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36",
    }
    cookie_header = store.build_cookie_header()
    if cookie_header:
        headers["cookie"] = cookie_header
    access = store.access_token()
    if access:
        headers["authorization"] = f"Bearer {access}"
    else:
        log.warning("No access_token parsed from arena-auth cookie")

    log.info(f"Sending to arena.ai: model={model_name}, eval_id={eval_id}, continue={continuing}, has_v3={bool(v3_token)}, has_v2={bool(v2_token)}")

    if stream:
        # 流式：立刻返回 SSE 响应头，内容由生成器边收边转（首字节延迟最低）
        return StreamingResponse(
            stream_response(url, arena_payload, headers, model_name, eval_id,
                            client_type, skey, register_session, fallback_fn),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",   # 让 Nginx 等反代不要缓冲 SSE
            },
        )
    else:
        # 非流式：先把 arena 的流全部读完再拼成一条完整响应（客户端只需等一次）
        return await non_stream_response(url, arena_payload, headers, model_name, eval_id,
                                         client_type, skey, register_session, fallback_fn)


async def stream_response(url, payload, headers, model_name, eval_id,
                          client_type="openai", skey=None,
                          register_session=False, fallback_fn=None):
    """
    【流式】arena.ai SSE → OpenAI SSE 的转换生成器。

    arena.ai 返回的行格式是 `前缀:JSON`（不是标准 SSE 的 data:），前缀含义：
        a0:  正文文本片段（字符串；特殊值 "hasArenaError" 表示对端出错）
        ag:  推理/思考内容片段（DeepSeek-R1、o 系列等模型会输出）
        ad:  结束信号，JSON 里可带 finishReason（可选 usage）
        a2:  心跳，或图片结果（数组，元素含 image 字段，值是图片 URL）
        a3:  错误信息
    其余行（含空行）一律忽略。

    OpenAI 侧输出：
        data: {"object":"chat.completion.chunk","choices":[{"delta":{"content":...}}]}
        ...
        data: [DONE]
    推理内容放在 delta.reasoning_content（OpenAI 官方无此字段，但被主流客户端/网关识别）。

    重试/回退策略（本函数带 while 循环的原因）：
      - 429 Too Many Requests：指数退避后重试同一请求，最多 3 次；
      - post-to-evaluation 返回 404：说明 arena 侧会话被回收，立即以
        回退工厂构造的全新 create-evaluation 请求重发一次；
      - 其余非 200：直接把错误写成 data: 帧（HTTP 头已发出，没法再改状态码）。
    """
    # 先让出事件循环，避免建连 arena 前其它请求饿死
    await asyncio.sleep(0)
    created = int(time.time())

    # 当前请求的可变状态（404 回退 / 429 重试时会更新）
    current_url = url
    current_payload = payload
    current_headers = dict(headers)
    current_eval_id = eval_id
    chat_id = f"chatcmpl-{current_eval_id}"

    fallback_used = False
    retry_count = 0
    max_retries = 3

    try:
        while True:
            # timeout=300：大模型长回答可能很久，给足 5 分钟
            # follow_redirects=True：arena.ai 可能 302 到带地区/实验参数的同路径
            async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
                body = json.dumps(current_payload, ensure_ascii=False)   # 保留中文原文，不要 \uXXXX
                # 用 stream() + aiter_lines() 逐行读，避免整段响应驻留内存
                async with client.stream("POST", current_url, content=body, headers=current_headers) as resp:
                    # ---- 429：指数退避重试（同一个请求，原样重发） ----
                    if resp.status_code == 429 and retry_count < max_retries:
                        await resp.aread()
                        wait = (2 ** retry_count) + random.random()
                        log.warning("Arena returned 429; backing off %.1fs (attempt %d/%d)",
                                    wait, retry_count + 1, max_retries)
                        retry_count += 1
                        await asyncio.sleep(wait)
                        continue

                    # ---- 404 且目标是 post-to-evaluation：会话在 arena 侧不存在，回退到 create ----
                    if (resp.status_code == 404
                            and current_url.startswith(ARENA_POST_EVAL)
                            and not fallback_used
                            and fallback_fn is not None):
                        await resp.aread()
                        log.warning("Eval session %s not found on arena.ai; falling back to create-evaluation",
                                    current_eval_id)
                        if skey:
                            store.sessions.pop(skey, None)
                        new_eval_id, new_payload = fallback_fn()
                        current_eval_id = new_eval_id
                        current_url = ARENA_CREATE_EVAL
                        current_payload = new_payload
                        current_headers = dict(current_headers)
                        current_headers["referer"] = f"{ARENA_BASE}/c/{new_eval_id}"
                        chat_id = f"chatcmpl-{new_eval_id}"
                        fallback_used = True
                        register_session = True     # 回退成功后要登记新会话
                        retry_count = 0
                        continue

                    # ---- 其它非 200：无法重试，直接以 data: 帧报错 ----
                    if resp.status_code != 200:
                        err = await resp.aread()
                        log.error(f"Arena API error: {resp.status_code} {err[:500]}")
                        if skey:
                            store.sessions.pop(skey, None)
                        # 关键：HTTP 头已经发出去了（SSE 已开始），只能用 data: 帧报错，不能用 HTTP 状态码
                        error_chunk = {
                            "id": chat_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model_name,
                            "choices": [{
                                "index": 0,
                                "delta": {"content": f"[Error: Arena API returned {resp.status_code}]"},
                                "finish_reason": "stop",
                            }],
                        }
                        yield f"data: {json.dumps(error_chunk)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

                    # ---- 200：会话在 arena 侧确实建立/存在了，此刻才登记 ----
                    if register_session and skey:
                        store.sessions[skey] = current_eval_id
                        log.info("Registered session %r -> %s", skey, current_eval_id)

                    got_finish = False
                    async for line in resp.aiter_lines():
                        if not line.strip():
                            continue

                        content = None      # 本行要输出的正文增量
                        reasoning = None    # 本行要输出的推理增量
                        finish = None       # 非空表示本行之后结束

                        if line.startswith("a0:"):
                            # 文本内容
                            try:
                                content = json.loads(line[3:])
                                if content == "hasArenaError":
                                    # 对端返回了错误标记，转成可见文本并立即结束
                                    content = "[Arena Error]"
                                    finish = "stop"
                            except json.JSONDecodeError:
                                continue
                        elif line.startswith("ag:"):
                            # 推理内容
                            try:
                                reasoning = json.loads(line[3:])
                            except json.JSONDecodeError:
                                continue
                        elif line.startswith("ad:"):
                            # 完成
                            finish = "stop"
                            try:
                                data = json.loads(line[3:])
                                if data.get("finishReason"):
                                    finish = data["finishReason"]   # 例如 length / stop
                            except json.JSONDecodeError:
                                pass
                        elif line.startswith("a2:"):
                            # heartbeat 或图片
                            # 心跳只是保活信号，直接跳过（不要把 "heartbeat" 当内容发出去）
                            if "heartbeat" in line:
                                continue
                            try:
                                data = json.loads(line[3:])
                                # 文生图模型：把图片数组渲染成 Markdown 图片链接作为内容
                                images = [img.get("image") for img in data if img.get("image")]
                                if images:
                                    content = "\n".join(f"![image]({u})" for u in images)
                            except json.JSONDecodeError:
                                continue
                        elif line.startswith("a3:"):
                            # 错误
                            try:
                                content = f"[Error: {json.loads(line[3:])}]"
                            except Exception:
                                # 兜底：JSON 解析失败就原样把载荷贴出来
                                content = f"[Error: {line[3:]}]"
                            finish = "stop"
                        else:
                            continue    # 不认识的前缀（如元数据行）直接忽略

                        if content is not None:
                            chunk = {
                                "id": chat_id,
                                "object": "chat.completion.chunk",
                                "created": created,
                                "model": model_name,
                                "choices": [{
                                    "index": 0,
                                    "delta": {"content": content},
                                    "finish_reason": None,
                                }],
                            }
                            # Claude/Anthropic 格式兼容
                            # 说明：这只是给 Anthropic 风格客户端“看起来像”的近似处理，
                            # 并不是完整的 Messages API 协议（字段结构并不完全等价）
                            if client_type == "claude":
                                chunk["type"] = "content_block_delta"
                            yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

                        if reasoning is not None:
                            # 将推理内容通过独立字段反馈，避免思考过程混进正文影响客户端渲染
                            chunk = {
                                "id": chat_id,
                                "object": "chat.completion.chunk",
                                "created": created,
                                "model": model_name,
                                "choices": [{
                                    "index": 0,
                                    "delta": {"reasoning_content": reasoning},
                                    "finish_reason": None,
                                }],
                            }
                            yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

                        if finish:
                            # 收尾帧：delta 为空、只带 finish_reason，然后补一个 [DONE]
                            # 注意这里没有回传 usage（流式模式下 token 统计缺失，部分客户端会显示 0）
                            chunk = {
                                "id": chat_id,
                                "object": "chat.completion.chunk",
                                "created": created,
                                "model": model_name,
                                "choices": [{
                                    "index": 0,
                                    "delta": {},
                                    "finish_reason": finish if finish != "stop" else "stop",  # 恒等于 finish
                                }],
                            }
                            yield f"data: {json.dumps(chunk)}\n\n"
                            yield "data: [DONE]\n\n"
                            got_finish = True
                            return

                    # 流结束但 arena 没发 ad:（偶发）——补一个安全收尾，避免客户端一直挂等
                    if not got_finish:
                        log.warning("arena stream ended without finish frame; emitting synthetic [DONE]")
                        final_chunk = {
                            "id": chat_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model_name,
                            "choices": [{
                                "index": 0,
                                "delta": {},
                                "finish_reason": "stop",
                            }],
                        }
                        yield f"data: {json.dumps(final_chunk)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

    except Exception as e:
        # 网络/解析等异常同样只能以 data: 帧上报（响应头已发出）
        log.error(f"Stream error: {e}")
        error_chunk = {
            "id": chat_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model_name,
            "choices": [{
                "index": 0,
                "delta": {"content": f"[Stream Error: {e}]"},
                "finish_reason": "stop",
            }],
        }
        yield f"data: {json.dumps(error_chunk)}\n\n"
        yield "data: [DONE]\n\n"


async def non_stream_response(url, payload, headers, model_name, eval_id,
                              client_type="openai", skey=None,
                              register_session=False, fallback_fn=None):
    """
    【非流式】响应：内部仍然按 SSE 逐行读 arena.ai（上游只有流式接口），
    但把 a0:/ag:/a2: 的内容全部缓存下来，最后拼成一条标准 OpenAI 响应返回。

    与 stream_response 的差异：
      - 富余信息更多：ad: 行里的 finishReason 与 usage 都能取到并回填；
      - 错误可以用真正的 HTTP 状态码返回（不是 data: 帧）；
      - 首字节延迟高：必须等 arena 全部输出完。

    重试/回退策略与 stream_response 相同：429 退避重试，post-to-evaluation 遇
    404 时回退到 create-evaluation 并重发。
    """
    content_parts = []
    reasoning_parts = []
    finish_reason = "stop"
    usage = {}

    current_url = url
    current_payload = payload
    current_headers = dict(headers)
    current_eval_id = eval_id

    fallback_used = False
    retry_count = 0
    max_retries = 3

    try:
        while True:
            async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
                body = json.dumps(current_payload, ensure_ascii=False)
                async with client.stream("POST", current_url, content=body, headers=current_headers) as resp:
                    # ---- 429：指数退避后重试同一请求 ----
                    if resp.status_code == 429 and retry_count < max_retries:
                        await resp.aread()
                        wait = (2 ** retry_count) + random.random()
                        log.warning("Arena returned 429; backing off %.1fs (attempt %d/%d)",
                                    wait, retry_count + 1, max_retries)
                        retry_count += 1
                        await asyncio.sleep(wait)
                        continue

                    # ---- 404 post-to-evaluation：回退到 create ----
                    if (resp.status_code == 404
                            and current_url.startswith(ARENA_POST_EVAL)
                            and not fallback_used
                            and fallback_fn is not None):
                        await resp.aread()
                        log.warning("Eval session %s not found on arena.ai; falling back to create-evaluation",
                                    current_eval_id)
                        if skey:
                            store.sessions.pop(skey, None)
                        new_eval_id, new_payload = fallback_fn()
                        current_eval_id = new_eval_id
                        current_url = ARENA_CREATE_EVAL
                        current_payload = new_payload
                        current_headers = dict(current_headers)
                        current_headers["referer"] = f"{ARENA_BASE}/c/{new_eval_id}"
                        fallback_used = True
                        register_session = True
                        retry_count = 0
                        continue

                    # ---- 其它非 200：记录日志、清会话缓存、透传状态码 ----
                    if resp.status_code != 200:
                        err = await resp.aread()
                        log.error(f"Arena API error: {resp.status_code} {err[:500]}")
                        if skey:
                            store.sessions.pop(skey, None)
                        raise HTTPException(resp.status_code, f"Arena API error: {err[:200]}")

                    # ---- 200：会话在 arena 侧确实建立/存在了，登记 ----
                    if register_session and skey:
                        store.sessions[skey] = current_eval_id
                        log.info("Registered session %r -> %s", skey, current_eval_id)

                    # 与 stream 版同样的前缀解析，区别只是“累积”而不是“逐条下发”
                    async for line in resp.aiter_lines():
                        if not line.strip():
                            continue
                        if line.startswith("a0:"):
                            try:
                                text = json.loads(line[3:])
                                # "hasArenaError" 是错误标记，不作为正文
                                if isinstance(text, str) and text != "hasArenaError":
                                    content_parts.append(text)
                            except json.JSONDecodeError:
                                pass
                        elif line.startswith("ag:"):
                            try:
                                text = json.loads(line[3:])
                                if isinstance(text, str):
                                    reasoning_parts.append(text)
                            except json.JSONDecodeError:
                                pass
                        elif line.startswith("ad:"):
                            try:
                                data = json.loads(line[3:])
                                if data.get("finishReason"):
                                    finish_reason = data["finishReason"]
                                if data.get("usage"):
                                    usage = data["usage"]      # 有就回填，没有就用下面的零值占位
                            except json.JSONDecodeError:
                                pass
                        elif line.startswith("a2:"):
                            if "heartbeat" in line:
                                continue
                            try:
                                data = json.loads(line[3:])
                                images = [img.get("image") for img in data if img.get("image")]
                                for img_url in images:
                                    content_parts.append(f"![image]({img_url})")   # 图片转 Markdown
                            except json.JSONDecodeError:
                                pass
                        elif line.startswith("a3:"):
                            try:
                                content_parts.append(f"[Error: {json.loads(line[3:])}]")
                            except Exception:
                                content_parts.append(f"[Error: {line[3:]}]")

                    # 读完了一整段，跳出 while
                    break

    except HTTPException:
        raise            # 上面主动抛的 HTTPException 原样上抛，不要被下面的 except 吞掉变 500
    except Exception as e:
        log.error(f"Non-stream error: {e}")
        raise HTTPException(500, str(e))

    full_content = "".join(content_parts)
    full_reasoning = "".join(reasoning_parts)

    # 组装 OpenAI 的 message 对象；有推理内容才附加 reasoning_content 字段
    message = {"role": "assistant", "content": full_content}
    if full_reasoning:
        message["reasoning_content"] = full_reasoning

    response = {
        "id": f"chatcmpl-{current_eval_id}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model_name,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
        }],
        # usage 缺失时给零值占位（部分客户端会因缺字段报错）
        "usage": usage or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
    }

    # Claude 格式兼容：额外补一套 Anthropic 风格的顶层字段
    # （同样只是近似兼容，不是完整 Messages API 协议）
    if client_type == "claude":
        response["type"] = "message"
        response["role"] = "assistant"
        response["content"] = [{"type": "text", "text": full_content}]

    return response


# ============================================================
# 健康检查
# ============================================================
@app.get("/health")
@app.get("/")
async def health():
    """存活探针 + 扩展状态一览（无需鉴权，方便脚本/监控直接 curl）"""
    return {
        "status": "ok",
        "version": "1.0.0",
        "extension": store.status(),
    }


# ============================================================
# 启动
# ============================================================
if __name__ == "__main__":
    log.info(f"Starting arena2api on port {PORT}")
    log.info(f"OpenAI API: http://localhost:{PORT}/v1")
    log.info("Waiting for Chrome extension to connect...")
    # host=0.0.0.0：允许容器/局域网访问；仅本机使用可改成 127.0.0.1 更安全
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
