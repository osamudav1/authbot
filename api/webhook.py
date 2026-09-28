import asyncio
import json
import os
from http.server import BaseHTTPRequestHandler

from telegram import Update

from auction_bot.bot import AuctionBot
from auction_bot.config import Config


_application = None
_loop = asyncio.new_event_loop()


def _get_application():
    global _application
    if _application is None:
        asyncio.set_event_loop(_loop)
        service = AuctionBot(Config.from_env())
        _application = service.application()
        _loop.run_until_complete(_application.initialize())
        _loop.run_until_complete(_application.start())
    return _application


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        expected_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
        received_secret = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if expected_secret and received_secret != expected_secret:
            self.send_response(403)
            self.end_headers()
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length))
            application = _get_application()
            update = Update.de_json(payload, application.bot)
            _loop.run_until_complete(application.process_update(update))
        except Exception:
            self.send_response(500)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true,"service":"authbot-webhook"}')

    def log_message(self, format, *args):
        return
