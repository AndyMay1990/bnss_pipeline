import re
import time
import html
import json
import uuid
from datetime import datetime, timezone

import requests
from Crypto.Cipher import AES
from flask import Flask, request, jsonify, Response, stream_with_context

app = Flask(__name__)

MODELS = [
    "DeepSeek-V1", "DeepSeek-V2", "DeepSeek-V2.5", "DeepSeek-V3", "DeepSeek-V3-0324",
    "DeepSeek-V3.1", "DeepSeek-V3.2", "DeepSeek-R1", "DeepSeek-R1-0528", "DeepSeek-R1-Distill",
    "DeepSeek-Prover-V1", "DeepSeek-Prover-V1.5", "DeepSeek-Prover-V2", "DeepSeek-VL",
    "DeepSeek-Coder", "DeepSeek-Coder-V2", "DeepSeek-Coder-6.7B-base", "DeepSeek-Coder-6.7B-instruct"
]

TAG_RE = re.compile(r"<[^>]+>")

def now_unix() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def clean_html_to_text(raw: str) -> str:
    if not raw:
        return ""
    text = html.unescape(raw)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p\s*>", "\n\n", text)
    text = re.sub(r"(?i)</div\s*>", "\n", text)
    text = TAG_RE.sub("", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text

def extract_response_content(page_html: str) -> str:
    m = re.search(r'<div class="response-content">(.*?)</div>', page_html, re.DOTALL)
    raw = m.group(1) if m else ""
    return clean_html_to_text(raw) if raw else ""

_session = None

def get_session() -> requests.Session:
    global _session
    if _session is not None:
        return _session

    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Android)"})

    r = s.get("https://asmodeus.free.nf/", timeout=30)
    nums = re.findall(r'toNumbers\("([a-f0-9]+)"\)', r.text)
    if len(nums) < 3:
        raise RuntimeError("Challenge parse failed (toNumbers not found).")

    key, iv, data = [bytes.fromhex(n) for n in nums[:3]]
    cookie_value = AES.new(key, AES.MODE_CBC, iv).decrypt(data).hex()
    s.cookies.set("__test", cookie_value, domain="asmodeus.free.nf")

    s.get("https://asmodeus.free.nf/index.php?i=1", timeout=30)
    time.sleep(0.3)

    _session = s
    return s

def messages_to_prompt(messages) -> str:
    prompt_lines = []
    for msg in messages or []:
        role = (msg.get("role") or "user").lower()
        content = msg.get("content") or ""
        if isinstance(content, list):
            content = "\n".join(
                [p.get("text", "") for p in content if p.get("type") == "text"]
            )
        prompt_lines.append(f"{role}: {content}".strip())
    return "\n".join([l for l in prompt_lines if l]).strip()

@app.get("/")
def home():
    return {"status": "ok", "openai_compat": True, "base": "/v1"}

@app.get("/v1/models")
def list_models():
    return jsonify({
        "object": "list",
        "data": [{"id": m, "object": "model"} for m in MODELS]
    })

@app.post("/v1/chat/completions")
def chat_completions():
    body = request.get_json(force=True) or {}

    model = body.get("model") or "DeepSeek-Coder-V2"
    if model not in MODELS:
        return jsonify({
            "error": {"message": f"Unknown model '{model}'", "type": "invalid_request_error"}
        }), 400

    stream = bool(body.get("stream", False))
    prompt = messages_to_prompt(body.get("messages"))

    s = get_session()
    resp = s.post(
        "https://asmodeus.free.nf/deepseek.php",
        params={"i": "1"},
        data={"model": model, "question": prompt},
        timeout=90,
    )
    text = extract_response_content(resp.text) or "No response."

    chatcmpl_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = now_unix()

    # Non-streaming (works with curl/PowerShell)
    if not stream:
        return jsonify({
            "id": chatcmpl_id,
            "object": "chat.completion",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop"
            }],
            "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
        })

    # Streaming (SSE): send delta chunks then [DONE]
    def sse():
        # First chunk: role
        first = {
            "id": chatcmpl_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
        }
        yield f"data: {json.dumps(first)}\n\n"

        # Stream content in chunks
        chunk_size = 200
        for i in range(0, len(text), chunk_size):
            part = text[i:i + chunk_size]
            chunk = {
                "id": chatcmpl_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{"index": 0, "delta": {"content": part}, "finish_reason": None}],
            }
            yield f"data: {json.dumps(chunk)}\n\n"

        # Final chunk
        final = {
            "id": chatcmpl_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
        }
        yield f"data: {json.dumps(final)}\n\n"
        yield "data: [DONE]\n\n"

    return Response(stream_with_context(sse()), mimetype="text/event-stream")

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8787)
