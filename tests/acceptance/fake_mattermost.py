"""Ağsız değil ama dış sunucusuz: gerçek HTTP + gerçek websocket konuşan sahte
Mattermost. Kabul testleri adaptörü gerçek süreç olarak (CLI) buna bağlar;
böylece iddialar bir kullanıcının/plugin'in göreceği sonuç üzerinde kurulur."""
from __future__ import annotations

import base64
import hashlib
import json
import queue
import re
import socket
import struct
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BOT_USER_ID = "bot-user-id"
BOT_USERNAME = "assistant-bot"
VALID_TOKEN = "tok-SECRET-1234567890"
_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def make_post(post_id, *, channel_id="c1", root_id="", user_id="u1", message="hello",
              create_at=None, post_type="", file_ids=None, metadata=None):
    post = {
        "id": post_id,
        "channel_id": channel_id,
        "root_id": root_id,
        "user_id": user_id,
        "message": message,
        "create_at": create_at,
        "type": post_type,
        "props": {},
    }
    if file_ids is not None:
        post["file_ids"] = file_ids
    if metadata is not None:
        post["metadata"] = metadata
    return post


class FakeMattermost:
    def __init__(self):
        self.lock = threading.Lock()
        self.channel_posts: dict[str, list[dict]] = {}
        self.created_posts: list[dict] = []
        self.requests: list[tuple[str, str]] = []
        self.files: dict[str, tuple[dict, bytes]] = {}
        self.fail_create_post_status: int | None = None
        self.echo_auth_in_errors = True
        self.ws_connected = threading.Event()
        # Her websocket bağlantısının kendi kuyruğu vardır; yeni bağlantı eskisini
        # emekliye ayırır. Böylece kopmuş eski bağlantı yeni olayları yutamaz.
        self._ws_active: "queue.Queue[dict | None] | None" = None
        self._seq = 0
        self._clock = 1789380000000
        fake = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def _json(self, status, body):
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def _authorized(self):
                return self.headers.get("Authorization") == f"Bearer {VALID_TOKEN}"

            def _deny(self):
                msg = "Invalid or expired session, please login again."
                if fake.echo_auth_in_errors:
                    msg += f" (received Authorization: {self.headers.get('Authorization')})"
                self._json(401, {"id": "api.context.session_expired.app_error", "message": msg,
                                 "status_code": 401})

            def do_GET(self):
                parsed = urlparse(self.path)
                with fake.lock:
                    fake.requests.append(("GET", parsed.path))
                if parsed.path == "/api/v4/websocket":
                    return fake._serve_websocket(self)
                if not self._authorized():
                    return self._deny()
                if parsed.path == "/api/v4/users/me":
                    return self._json(200, {"id": BOT_USER_ID, "username": BOT_USERNAME})
                m = re.fullmatch(r"/api/v4/channels/([^/]+)/posts", parsed.path)
                if m:
                    params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                    return self._json(200, fake._post_list(m.group(1), params))
                m = re.fullmatch(r"/api/v4/files/([^/]+)/info", parsed.path)
                if m and m.group(1) in fake.files:
                    return self._json(200, fake.files[m.group(1)][0])
                m = re.fullmatch(r"/api/v4/files/([^/]+)", parsed.path)
                if m and m.group(1) in fake.files:
                    raw = fake.files[m.group(1)][1]
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)
                    return
                return self._json(404, {"message": "not found", "status_code": 404})

            def do_POST(self):
                parsed = urlparse(self.path)
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                with fake.lock:
                    fake.requests.append(("POST", parsed.path))
                if not self._authorized():
                    return self._deny()
                if parsed.path != "/api/v4/posts":
                    return self._json(404, {"message": "not found"})
                if fake.fail_create_post_status is not None:
                    msg = "internal failure"
                    if fake.echo_auth_in_errors:
                        msg += f" Authorization: {self.headers.get('Authorization')}"
                    return self._json(fake.fail_create_post_status, {"message": msg})
                with fake.lock:
                    fake.created_posts.append(body)
                    post_id = f"created-{len(fake.created_posts)}"
                return self._json(201, {
                    "id": post_id,
                    "channel_id": body.get("channel_id"),
                    "root_id": body.get("root_id", ""),
                    "message": body.get("message"),
                    "create_at": 1789380000123,
                    "props": body.get("props") or {},
                })

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    # ------------------------------------------------------------ lifecycle
    def start(self):
        self._thread.start()
        return self

    def stop(self):
        with self.lock:
            active, self._ws_active = self._ws_active, None
        if active is not None:
            active.put(None)
        self.server.shutdown()
        self.server.server_close()

    def env(self, token=VALID_TOKEN):
        env = {"MM_URL": "127.0.0.1", "MM_PORT": str(self.port), "MM_SCHEME": "http"}
        if token is not None:
            env["MM_TOKEN"] = token
        return env

    # ------------------------------------------------------------ data
    def _next_create_at(self):
        with self.lock:
            self._clock += 1000
            return self._clock

    def add_offline_post(self, post):
        """Websocket olayı olmadan kanala düşen post (adaptör kapalıyken)."""
        if post.get("create_at") is None:
            post["create_at"] = self._next_create_at()
        with self.lock:
            self.channel_posts.setdefault(post["channel_id"], []).append(post)
        return post

    def push_post(self, post, *, sender_name=None, channel_type="O", team_id="t1"):
        self.add_offline_post(post)
        with self.lock:
            self._seq += 1
            seq = self._seq
            active = self._ws_active
        data = {"post": json.dumps(post), "channel_type": channel_type, "team_id": team_id}
        if sender_name is not None:
            data["sender_name"] = sender_name
        if active is not None:  # bağlı istemci yoksa olay kaybolur; post replay ile bulunur
            active.put({"event": "posted", "data": data,
                        "broadcast": {"channel_id": post["channel_id"]}, "seq": seq})
        return post

    def _post_list(self, channel_id, params):
        with self.lock:
            posts = list(self.channel_posts.get(channel_id, []))
        after = params.get("after")
        if after:
            ids = [p["id"] for p in posts]
            posts = posts[ids.index(after) + 1:] if after in ids else []
        per_page = int(params.get("per_page", 60))
        selected = posts[:per_page] if after else posts[-per_page:]
        more = len(posts) > per_page
        return {
            "order": [p["id"] for p in reversed(selected)],
            "posts": {p["id"]: p for p in selected},
            "next_post_id": "more" if more and after else "",
            "prev_post_id": "",
        }

    # ------------------------------------------------------------ websocket
    def _serve_websocket(self, handler):
        key = handler.headers.get("Sec-WebSocket-Key", "")
        accept = base64.b64encode(hashlib.sha1((key + _WS_GUID).encode()).digest()).decode()
        handler.send_response(101)
        handler.send_header("Upgrade", "websocket")
        handler.send_header("Connection", "Upgrade")
        handler.send_header("Sec-WebSocket-Accept", accept)
        handler.end_headers()
        handler.wfile.flush()
        sock = handler.connection
        rfile = handler.rfile
        handler.close_connection = True
        own_queue = None
        try:
            challenge = json.loads(_read_frame(rfile))
            if challenge.get("data", {}).get("token") != VALID_TOKEN:
                _send_frame(sock, json.dumps({"status": "FAIL", "seq_reply": 1}))
                return
            _send_frame(sock, json.dumps({"event": "hello", "data": {"server_version": "10"},
                                          "broadcast": {}, "seq": 0}))
            _send_frame(sock, json.dumps({"status": "OK", "seq_reply": 1}))
            own_queue: "queue.Queue[dict | None]" = queue.Queue()
            with self.lock:
                retired, self._ws_active = self._ws_active, own_queue
                self.ws_connected.set()
            if retired is not None:
                retired.put(None)
            while True:
                try:
                    event = own_queue.get(timeout=0.2)
                except queue.Empty:
                    continue
                if event is None:
                    return
                _send_frame(sock, json.dumps(event))
        except (OSError, ValueError, ConnectionError):
            return
        finally:
            with self.lock:
                if own_queue is not None and self._ws_active is own_queue:
                    self._ws_active = None
                    self.ws_connected.clear()


def _read_exact(rfile, n):
    data = rfile.read(n)
    if len(data) != n:
        raise ConnectionError("websocket closed")
    return data


def _read_frame(rfile):
    while True:
        b1, b2 = _read_exact(rfile, 2)
        opcode = b1 & 0x0F
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack(">H", _read_exact(rfile, 2))[0]
        elif length == 127:
            length = struct.unpack(">Q", _read_exact(rfile, 8))[0]
        mask = _read_exact(rfile, 4) if b2 & 0x80 else b"\0\0\0\0"
        payload = bytes(c ^ mask[i % 4] for i, c in enumerate(_read_exact(rfile, length)))
        if opcode == 0x8:
            raise ConnectionError("websocket closed")
        if opcode == 0x1:
            return payload.decode()


def _send_frame(sock: socket.socket, text: str):
    payload = text.encode()
    header = bytes([0x81])
    if len(payload) < 126:
        header += bytes([len(payload)])
    elif len(payload) < 65536:
        header += bytes([126]) + struct.pack(">H", len(payload))
    else:
        header += bytes([127]) + struct.pack(">Q", len(payload))
    sock.sendall(header + payload)
