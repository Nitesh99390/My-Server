"""Offline check: every command / reply-keyboard button is matched by exactly ONE message handler.

Run:  python tests_bot/test_routing.py
(no network, no Telegram credentials needed - uses a throw-away sqlite db in /tmp)
"""
import asyncio
import os
import sys

os.environ.update(API_ID="12345", API_HASH="x" * 32, BOT_TOKEN="123:abc", OWNER_ID="1",
                  DB_PATH="/tmp/abp_route.db", JOBS_DIR="/tmp/abp_route_jobs", SESSION_NAME="/tmp/abp_route_s",
                  LOG_FILE="/tmp/abp_route.log")
for f in ("/tmp/abp_route.db", "/tmp/abp_route.db-wal", "/tmp/abp_route.db-shm"):
    try:
        os.remove(f)
    except OSError:
        pass
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import bot as B  # noqa: E402
from pyrogram.enums import ChatType, MessageEntityType  # noqa: E402
from pyrogram.handlers import MessageHandler  # noqa: E402
from pyrogram.types import Chat, Message, MessageEntity, User  # noqa: E402

# Client.add_handler() schedules the registration on the loop, so tick it once.
B.LOOP.run_until_complete(asyncio.sleep(0))
HANDLERS = [h for grp in B.bot.dispatcher.groups.values() for h in grp if isinstance(h, MessageHandler)]
B.bot.me = User(id=99, is_bot=True, first_name="bot", username="Fast_tts_bot")


def mk(text: str, uid: int = 1) -> Message:
    ents = None
    if text.startswith("/"):
        ents = [MessageEntity(type=MessageEntityType.BOT_COMMAND, offset=0, length=len(text.split()[0]), client=B.bot)]
    return Message(id=1, text=text, from_user=User(id=uid, is_bot=False, first_name="F", username="u"),
                   chat=Chat(id=uid, type=ChatType.PRIVATE), entities=ents, client=B.bot)


async def route(text: str, uid: int = 1):
    m = mk(text, uid)
    return [h.callback.__name__ for h in HANDLERS if await h.filters(B.bot, m)]


CASES = ["/start", "/menu", "/help", "/status", "/account", "/settings", "/preview", "/cancel", "/resume", "/queue",
         "/history", "/admin", "/servers", "/users", "/stats", "/jobs", "/addserver https://a.b.workers.dev",
         B.BTN_CREATE, B.BTN_SETTINGS, B.BTN_STATUS, B.BTN_ADMIN, B.BTN_HELP, B.BTN_REQUEST,
         "🖥 Server Status", "🔙 Back", "➕ Add Server", "📜 History", "👤 My Account",
         "hello this is a long text to narrate", "/unknowncmd"]


async def main() -> int:
    B.db.ensure_user(1, "o", "O")
    bad = 0
    print(f"{len(HANDLERS)} message handlers registered")
    for c in CASES:
        hit = await route(c)
        flag = "" if len(hit) == 1 else "   <-- PROBLEM"
        print(f"{c!r:45} -> {hit}{flag}")
        if len(hit) != 1:
            bad += 1
    print("ROUTING OK" if not bad else f"ROUTING PROBLEMS: {bad}")
    return bad


if __name__ == "__main__":
    rc = B.LOOP.run_until_complete(main())
    sys.exit(1 if rc else 0)
