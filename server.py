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
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
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
logging.basicConfig(
    level=logging.DEBUG if os.environ.get("DEBUG") else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("arena2api")

# ============================================================
# 配置
# ============================================================
PORT = int(os.environ.get("PORT", "9090"))
API_KEY = os.environ.get("API_KEY", "").strip()
ARENA_BASE = "https://arena.ai"
ARENA_CREATE_EVAL = f"{ARENA_BASE}/nextjs-api/stream/create-evaluation"
ARENA_POST_EVAL = f"{ARENA_BASE}/nextjs-api/stream/post-to-evaluation"  # + /{id}

# reCAPTCHA
RECAPTCHA_V3_SITEKEY = "6LeTGMcsAAAAALuIlkVwIxaAuZA8VledA6d3Nnb0"

# ============================================================
# UUIDv7
# ============================================================
def uuid7() -> str:
    ts = int(time.time() * 1000)
    ra = secrets.randbits(12)
    rb = secrets.randbits(62)
    u = ts << 80 | (0x7000 | ra) << 64 | (0x8000000000000000 | rb)
    h = f"{u:032x}"
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"


# ============================================================
# Token / Cookie Store（从扩展接收）
# ============================================================
class Store:
    def __init__(self):
        self.cookies: dict = {}
        self.auth_token: str = ""
        self.cf_clearance: str = ""
        self.v3_tokens: list = []  # [{token, action, ts}]
        self.v2_token: Optional[dict] = None
        self.last_push: float = 0
        self.models: list = []
        self.text_models: dict = {}  # publicName -> id
        self.image_models: dict = {}
        self.vision_models: list = []
        self.next_actions: dict = {}  # action name -> hash
        self.sessions: dict = {}  # session key -> arena eval id

    @property
    def active(self) -> bool:
        return self.last_push > 0 and (time.time() - self.last_push < 120)

    def push(self, data: dict):
        self.last_push = time.time()
        if data.get("cookies"):
            self.cookies = data["cookies"]
        if data.get("auth_token"):
            self.auth_token = data["auth_token"]
        if data.get("cf_clearance"):
            self.cf_clearance = data["cf_clearance"]
        # V3 tokens
        if data.get("v3_tokens"):
            for t in data["v3_tokens"]:
                tok = t.get("token", "")
                if not tok or len(tok) < 20:
                    continue
                age = t.get("age_ms", 0)
                if age > 120000:
                    continue
                if any(x["token"] == tok for x in self.v3_tokens):
                    continue
                self.v3_tokens.append({
                    "token": tok,
                    "action": t.get("action", "chat_submit"),
                    "ts": time.time() - age / 1000,
                })
            while len(self.v3_tokens) > 10:
                self.v3_tokens.pop(0)
        # V2 token
        if data.get("v2_token"):
            v2 = data["v2_token"]
            if v2.get("token") and v2.get("age_ms", 0) < 120000:
                self.v2_token = {
                    "token": v2["token"],
                    "ts": time.time() - v2.get("age_ms", 0) / 1000,
                }
        # Models
        if data.get("models"):
            self._update_models(data["models"])
        # Next actions
        if data.get("next_actions"):
            self.next_actions.update(data["next_actions"])

    def _update_models(self, models: list):
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
        now = time.time()
        self.v3_tokens = [t for t in self.v3_tokens if now - t["ts"] < 120]
        if not self.v3_tokens:
            return None
        return self.v3_tokens.pop(0)["token"]

    def pop_v2_token(self) -> Optional[str]:
        if not self.v2_token:
            return None
        if time.time() - self.v2_token["ts"] > 120:
            self.v2_token = None
            return None
        tok = self.v2_token["token"]
        self.v2_token = None
        return tok

    def build_cookie_header(self) -> str:
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
        return "; ".join(f"{k}={v}" for k, v in cookies.items())

    def access_token(self) -> str:
        """从 arena-auth cookie 取出 JWT，不要把整段 base64-JSON 当 Bearer。"""
        raw = self.auth_token or self.cookies.get("arena-auth-prod-v1", "")
        if not raw:
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
        now = time.time()
        valid_v3 = [t for t in self.v3_tokens if now - t["ts"] < 120]
        return {
            "active": self.active,
            "last_push_ago": round(now - self.last_push, 1) if self.last_push else None,
            "v3_tokens": len(valid_v3),
            "has_v2": bool(self.v2_token and now - self.v2_token["ts"] < 120),
            "has_auth": bool(self.auth_token),
            "has_cf": bool(self.cf_clearance),
            "text_models": len(self.text_models),
            "image_models": len(self.image_models),
            "next_actions": list(self.next_actions.keys()),
            "cookies": list(self.cookies.keys()),
        }


store = Store()


def parse_access_token(raw: str) -> str:
    if not raw:
        return ""
    s = unquote(raw.strip())
    if s.startswith("base64-"):
        blob = s[7:]
        pad = "=" * ((4 - len(blob) % 4) % 4)
        data = None
        for decoder in (base64.urlsafe_b64decode, base64.b64decode):
            try:
                data = json.loads(decoder(blob + pad))
                break
            except Exception:
                continue
        if not isinstance(data, dict):
            return ""
        return data.get("access_token") or data.get("accessToken") or ""
    if s.count(".") >= 2 and s.startswith("eyJ"):
        return s
    return ""


# ============================================================
# FastAPI
# ============================================================
app = FastAPI(title="arena2api", version="1.0.0")
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
    if not API_KEY:
        return

    auth_header = request.headers.get("authorization", "")
    expected = f"Bearer {API_KEY}"
    if auth_header != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ============================================================
# 扩展端点
# ============================================================
@app.post("/v1/extension/push")
async def extension_push(request: Request):
    """接收扩展推送的 token、cookies、models"""
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")
    store.push(data)
    need = len([t for t in store.v3_tokens if time.time() - t["ts"] < 120]) < 3
    return {
        "status": "ok",
        "need_tokens": need,
        "v3_count": len(store.v3_tokens),
    }


@app.get("/v1/extension/status")
async def extension_status():
    return store.status()


# ============================================================
# OpenAI 兼容端点
# ============================================================
@app.get("/v1/models")
async def list_models(request: Request):
    """列出可用模型"""
    log.info("GET /v1/models from %s ua=%s", request.client.host if request.client else "?", request.headers.get("user-agent", "")[:80])
    verify_api_key(request)
    all_models = {}
    all_models.update(store.text_models)
    all_models.update(store.image_models)
    data = []
    for name in sorted(all_models.keys()):
        data.append({
            "id": name,
            "object": "model",
            "created": 0,
            "owned_by": "arena.ai",
        })
    if not data:
        # 返回一个占位模型
        data.append({
            "id": "waiting-for-extension",
            "object": "model",
            "created": 0,
            "owned_by": "arena.ai",
        })
    log.info("GET /v1/models -> %d models", len(data))
    return {"object": "list", "data": data}


def session_key(request: Request, model_name: str) -> str:
    """X-Session-Id 优先；没有则用模型名哈希，换模型即新会话。"""
    sid = request.headers.get("x-session-id", "").strip()
    if sid:
        return sid
    return hashlib.sha256(model_name.encode()).hexdigest()[:16]


def detect_client(request: Request) -> str:
    """检测客户端类型"""
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


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """OpenAI 兼容的聊天补全"""
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
    hdrs = {}
    for k, v in request.headers.items():
        lk = k.lower()
        if lk == "authorization" and v:
            hdrs[k] = v[:12] + "..." + v[-6:] if len(v) > 24 else "***"
        elif lk == "cookie" and v:
            hdrs[k] = f"<{len(v)} chars>"
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
    for i, msg in enumerate(messages):
        content = msg.get("content", "")
        if isinstance(content, list):
            preview = "\n".join(p.get("text", "") for p in content if p.get("type") == "text")
        else:
            preview = str(content or "")
        if len(preview) > 120:
            preview = preview[:120] + "..."
        log.info("  message[%d] role=%s content=%r", i, msg.get("role"), preview)

    if not messages:
        raise HTTPException(400, "messages is required")

    # 检查扩展是否连接
    if not store.active:
        raise HTTPException(503, "Extension not connected. Please open arena.ai in Chrome with the extension installed.")

    # 解析模型
    model_id = store.text_models.get(model_name) or store.image_models.get(model_name)
    if not model_id:
        # 尝试模糊匹配
        for name, mid in {**store.text_models, **store.image_models}.items():
            if model_name.lower() in name.lower() or name.lower() in model_name.lower():
                model_id = mid
                model_name = name
                break
    if not model_id:
        available = list(store.text_models.keys()) + list(store.image_models.keys())
        raise HTTPException(404, f"Model '{model_name}' not found. Available: {available[:20]}")

    # 构建 prompt（取最后一条 user 消息）
    prompt = ""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, list):
                # 多模态消息
                text_parts = [p.get("text", "") for p in content if p.get("type") == "text"]
                prompt = "\n".join(text_parts)
            else:
                prompt = content
            break
    if not prompt:
        prompt = messages[-1].get("content", "")

    # 同一 session 只把最新一句发给已有 arena 会话；新会话才带上 system
    skey = session_key(request, model_name)
    continuing = skey in store.sessions
    log.info("Session resolve: key=%r continue=%s eval_id=%s", skey, continuing, store.sessions.get(skey))
    if continuing:
        eval_id = store.sessions[skey]
        url = f"{ARENA_POST_EVAL}/{eval_id}"
    else:
        system_parts = []
        for m in messages:
            if m.get("role") != "system":
                continue
            content = m.get("content", "")
            if isinstance(content, list):
                content = "\n".join(p.get("text", "") for p in content if p.get("type") == "text")
            if content:
                system_parts.append(content)
        if system_parts:
            prompt = "\n".join(system_parts) + "\n\n" + prompt
        eval_id = uuid7()
        store.sessions[skey] = eval_id
        url = ARENA_CREATE_EVAL

    # 获取 reCAPTCHA token
    v3_token = store.pop_v3_token()
    v2_token = store.pop_v2_token() if not v3_token else None

    is_image = model_name in store.image_models
    modality = "image" if is_image else "chat"

    # 构建 arena.ai 请求：Cookie + 解析出的 JWT；body 对齐浏览器 text/plain
    user_msg_id = uuid7()
    model_a_msg_id = uuid7()

    arena_payload = {
        "id": eval_id,
        "mode": "direct-battle",
        "modelAId": model_id,
        "userMessageId": user_msg_id,
        "modelAMessageId": model_a_msg_id,
        "userMessage": {
            "content": prompt,
            "experimental_attachments": [],
            "metadata": {},
        },
        "modality": modality,
    }

    if v2_token:
        arena_payload["recaptchaV2Token"] = v2_token
        arena_payload["recaptchaV3Token"] = None
    elif v3_token:
        arena_payload["recaptchaV3Token"] = v3_token
    else:
        log.warning("No reCAPTCHA token available, sending without token")

    headers = {
        "accept": "*/*",
        "content-type": "text/plain;charset=UTF-8",
        "origin": ARENA_BASE,
        "referer": f"{ARENA_BASE}/c/{eval_id}",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
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
        return StreamingResponse(
            stream_response(url, arena_payload, headers, model_name, eval_id, client_type, skey),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
    else:
        return await non_stream_response(url, arena_payload, headers, model_name, eval_id, client_type, skey)


async def stream_response(url, payload, headers, model_name, eval_id, client_type="openai", session_key=None):
    """流式响应生成器"""
    # 先让出事件循环，避免建连 arena 前其它请求饿死
    await asyncio.sleep(0)
    chat_id = f"chatcmpl-{eval_id}"
    created = int(time.time())

    try:
        async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
            body = json.dumps(payload, ensure_ascii=False)
            async with client.stream("POST", url, content=body, headers=headers) as resp:
                if resp.status_code != 200:
                    err = await resp.aread()
                    log.error(f"Arena API error: {resp.status_code} {err[:500]}")
                    if session_key:
                        store.sessions.pop(session_key, None)
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

                async for line in resp.aiter_lines():
                    if not line.strip():
                        continue

                    content = None
                    reasoning = None
                    finish = None

                    if line.startswith("a0:"):
                        # 文本内容
                        try:
                            content = json.loads(line[3:])
                            if content == "hasArenaError":
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
                                finish = data["finishReason"]
                        except json.JSONDecodeError:
                            pass
                    elif line.startswith("a2:"):
                        # heartbeat 或图片
                        if "heartbeat" in line:
                            continue
                        try:
                            data = json.loads(line[3:])
                            images = [img.get("image") for img in data if img.get("image")]
                            if images:
                                content = "\n".join(f"![image]({url})" for url in images)
                        except json.JSONDecodeError:
                            continue
                    elif line.startswith("a3:"):
                        # 错误
                        try:
                            content = f"[Error: {json.loads(line[3:])}]"
                        except:
                            content = f"[Error: {line[3:]}]"
                        finish = "stop"
                    else:
                        continue

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
                        if client_type == "claude":
                            chunk["type"] = "content_block_delta"
                        yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

                    if reasoning is not None:
                        # 将推理内容作为普通内容输出（或可以用 reasoning_content）
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
                        chunk = {
                            "id": chat_id,
                            "object": "chat.completion.chunk",
                            "created": created,
                            "model": model_name,
                            "choices": [{
                                "index": 0,
                                "delta": {},
                                "finish_reason": finish if finish != "stop" else "stop",
                            }],
                        }
                        yield f"data: {json.dumps(chunk)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

    except Exception as e:
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


async def non_stream_response(url, payload, headers, model_name, eval_id, client_type="openai", session_key=None):
    """非流式响应"""
    content_parts = []
    reasoning_parts = []
    finish_reason = "stop"
    usage = {}

    try:
        async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
            body = json.dumps(payload, ensure_ascii=False)
            async with client.stream("POST", url, content=body, headers=headers) as resp:
                if resp.status_code != 200:
                    err = await resp.aread()
                    log.error(f"Arena API error: {resp.status_code} {err[:500]}")
                    if session_key:
                        store.sessions.pop(session_key, None)
                    raise HTTPException(resp.status_code, f"Arena API error: {err[:200]}")

                async for line in resp.aiter_lines():
                    if not line.strip():
                        continue
                    if line.startswith("a0:"):
                        try:
                            text = json.loads(line[3:])
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
                                usage = data["usage"]
                        except json.JSONDecodeError:
                            pass
                    elif line.startswith("a2:"):
                        if "heartbeat" in line:
                            continue
                        try:
                            data = json.loads(line[3:])
                            images = [img.get("image") for img in data if img.get("image")]
                            for img_url in images:
                                content_parts.append(f"![image]({img_url})")
                        except json.JSONDecodeError:
                            pass
                    elif line.startswith("a3:"):
                        try:
                            content_parts.append(f"[Error: {json.loads(line[3:])}]")
                        except:
                            content_parts.append(f"[Error: {line[3:]}]")

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Non-stream error: {e}")
        raise HTTPException(500, str(e))

    full_content = "".join(content_parts)
    full_reasoning = "".join(reasoning_parts)

    message = {"role": "assistant", "content": full_content}
    if full_reasoning:
        message["reasoning_content"] = full_reasoning

    response = {
        "id": f"chatcmpl-{eval_id}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model_name,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": finish_reason,
        }],
        "usage": usage or {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
    }

    # Claude 格式兼容
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
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
