#!/usr/bin/env python3
"""
Sabong Saga Bot - Web Dashboard Backend
FastAPI server with bot management, WebSocket live logs, SQLite config
"""

import os, sys, json, time, asyncio, re, hashlib, secrets
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from contextlib import asynccontextmanager

import sqlite3
import requests
import websockets
import jwt as jwt_lib

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# =========================
# Database
# =========================
DB_PATH = "sabong.db"

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nickname TEXT NOT NULL,
            address TEXT NOT NULL,
            jwt TEXT NOT NULL,
            csrf_secret TEXT NOT NULL DEFAULT 'VeTXMPhBzWCRRpSJe8Yz7VFm',
            refresh_token TEXT NOT NULL DEFAULT '',
            primary_chicken INTEGER NOT NULL,
            backup_chickens TEXT NOT NULL DEFAULT '[]',
            battle_items TEXT NOT NULL DEFAULT '[54, 97, 90, 47, 45, 98]',
            check_affection INTEGER NOT NULL DEFAULT 0,
            use_backup_chickens INTEGER NOT NULL DEFAULT 1,
            require_immortal INTEGER NOT NULL DEFAULT 1,
            min_affection INTEGER NOT NULL DEFAULT 90,
            min_boosters INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        
        CREATE TABLE IF NOT EXISTS match_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            chicken_id INTEGER NOT NULL,
            outcome TEXT NOT NULL,
            mmr_change INTEGER NOT NULL DEFAULT 0,
            battle_items TEXT,
            is_backup INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        );
    """)
    
    # Set default password if not exists
    cursor = conn.execute("SELECT value FROM app_config WHERE key = 'password_hash'")
    if not cursor.fetchone():
        # Default password: "sabong123" - user should change this
        default_hash = hashlib.sha256("sabong123".encode()).hexdigest()
        conn.execute("INSERT INTO app_config (key, value) VALUES ('password_hash', ?)", (default_hash,))
    
    conn.commit()
    conn.close()

# =========================
# Game Constants
# =========================
BASE_URL = "https://game.sabongsaga-services.com"
APP_URL = "https://app.sabongsaga.com"
HP_API = "https://chicken-api-ivory.vercel.app/api/game/{token_id}"
MATCHES_API = f"{APP_URL}/api/proxy/game/matches?tokenId={{token_id}}"
HEAL_API = f"{APP_URL}/api/proxy/heal"
CSRF_TOKEN_URL = f"{APP_URL}/csrf-token"
DAILY_RUB_URL = f"{APP_URL}/api/chickens/daily-rub"
MATCHMAKE_URL = f"{BASE_URL}/matchmake/joinOrCreate/matchmaking"
CHICKEN_API = "https://chicken-animation.vercel.app/api/proxy/game?tokenId={token_id}"

QUEUE_DELAY = 60
MATCH_TIMEOUT = 900
POLL_INTERVAL = 10
DAILY_RUB_HOUR = 8  # 8am PHT (UTC+8 = 0 UTC)

# =========================
# Bot Manager
# =========================
class BotInstance:
    def __init__(self, account_id: int, config: dict):
        self.account_id = account_id
        self.config = config
        self.task: Optional[asyncio.Task] = None
        self.running = False
        self.stats = {"wins": 0, "losses": 0, "draws": 0, "total_mmr": 0, "matches": 0}
        self.logs: List[str] = []
        self.current_chicken: Optional[int] = None
        self.current_round: int = 0
        self.session_start = datetime.now()
    
    def log(self, msg: str, level: str = "info"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        entry = {"time": timestamp, "msg": msg, "level": level}
        self.logs.append(json.dumps(entry))
        # Keep last 500 logs
        if len(self.logs) > 500:
            self.logs = self.logs[-500:]
        # Broadcast to connected websockets
        asyncio.ensure_future(broadcast_log(self.account_id, entry))

# Global bot instances
bot_instances: Dict[int, BotInstance] = {}
ws_connections: Dict[int, List[WebSocket]] = {}

async def broadcast_log(account_id: int, entry: dict):
    if account_id in ws_connections:
        dead = []
        for ws in ws_connections[account_id]:
            try:
                await ws.send_json({"type": "log", "data": entry})
            except:
                dead.append(ws)
        for ws in dead:
            ws_connections[account_id].remove(ws)

async def broadcast_stats(account_id: int):
    if account_id in bot_instances:
        bot = bot_instances[account_id]
        stats_data = {
            "type": "stats",
            "data": {
                **bot.stats,
                "current_chicken": bot.current_chicken,
                "current_round": bot.current_round,
                "running": bot.running,
                "session_start": bot.session_start.isoformat(),
            }
        }
        if account_id in ws_connections:
            dead = []
            for ws in ws_connections[account_id]:
                try:
                    await ws.send_json(stats_data)
                except:
                    dead.append(ws)
            for ws in dead:
                ws_connections[account_id].remove(ws)

# =========================
# Bot Logic (adapted from bot.py)
# =========================
def jwt_hours_left(token: str) -> float:
    try:
        data = jwt_lib.decode(token, options={"verify_signature": False})
        exp = data.get("exp")
        if not exp: return 9999.0
        return max(0.0, (exp - time.time()) / 3600.0)
    except:
        return 9999.0

def refresh_jwt_via_page_load(jwt_token: str, refresh_token: str, csrf_secret: str) -> Optional[str]:
    """
    Refresh JWT by mimicking a full browser page load.
    The server middleware reads the refresh_token cookie and sets a new jwt cookie.
    Tries both chickensaga.com and sabongsaga.com domains.
    Returns the new JWT or None if refresh failed.
    """
    domains = [
        "https://app.chickensaga.com",
        "https://app.sabongsaga.com",
    ]
    
    for domain in domains:
        try:
            s = requests.Session()
            s.cookies.update({
                "jwt": jwt_token,
                "refresh_token": refresh_token,
                "_csrfSecret": csrf_secret,
            })
            s.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
            })
            
            # Try homepage
            r = s.get(domain + "/", timeout=30, allow_redirects=True)
            new_jwt = s.cookies.get("jwt")
            if new_jwt and new_jwt != jwt_token:
                hrs = jwt_hours_left(new_jwt)
                if hrs > 1:
                    # Also check for new refresh token
                    new_rt = s.cookies.get("refresh_token")
                    s.close()
                    return new_jwt
            
            # Try inventory page
            r = s.get(domain + "/inventory", timeout=30, allow_redirects=True)
            new_jwt = s.cookies.get("jwt")
            if new_jwt and new_jwt != jwt_token:
                hrs = jwt_hours_left(new_jwt)
                if hrs > 1:
                    s.close()
                    return new_jwt
            
            # Try auth/me endpoint
            r = s.get(domain + "/api/auth/me", timeout=30)
            new_jwt = s.cookies.get("jwt")
            if new_jwt and new_jwt != jwt_token:
                hrs = jwt_hours_left(new_jwt)
                if hrs > 1:
                    s.close()
                    return new_jwt
            
            s.close()
        except:
            pass
    return None

def update_account_jwt(account_id: int, new_jwt: str, new_refresh: str = None):
    """Save refreshed JWT (and optionally new refresh token) to database."""
    try:
        conn = get_db()
        if new_refresh:
            conn.execute("UPDATE accounts SET jwt = ?, refresh_token = ?, updated_at = datetime('now') WHERE id = ?",
                        (new_jwt, new_refresh, account_id))
        else:
            conn.execute("UPDATE accounts SET jwt = ?, updated_at = datetime('now') WHERE id = ?",
                        (new_jwt, account_id))
        conn.commit()
        conn.close()
    except: pass

def do_daily_rub(jwt_token: str, refresh_token: str, csrf_secret: str) -> Tuple[bool, str]:
    """
    Perform daily rub for all chickens.
    POST /api/chickens/daily-rub with CSRF token, no body.
    Returns (success, message).
    """
    try:
        s = requests.Session()
        s.cookies.update({
            "jwt": jwt_token,
            "refresh_token": refresh_token,
            "_csrfSecret": csrf_secret,
        })
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Origin": APP_URL,
            "Referer": APP_URL + "/rub",
        })
        
        # Get CSRF token
        csrf_token = None
        for attempt in range(3):
            try:
                r = s.get(CSRF_TOKEN_URL, timeout=120)
                if r.status_code == 200:
                    csrf_token = r.json().get("csrfToken")
                    if csrf_token: break
            except: pass
            time.sleep(2)
        
        if not csrf_token:
            s.close()
            return False, "Could not get CSRF token"
        
        if not csrf_token:
            s.close()
            return False, "Could not get CSRF token"
        
        s.cookies.update({"_csrf": csrf_token})
        
        # POST daily rub (empty body)
        headers = {
            "Content-Type": "application/json",
            "x-csrf-token": csrf_token,
        }
        r = s.post(DAILY_RUB_URL, headers=headers, timeout=120)
        s.close()
        
        if r.status_code == 200:
            data = r.json()
            if data.get("status"):
                # Extract feather info
                summary = data.get("data", {}).get("data", {}).get("summary", {})
                feathers = summary.get("totalFeathers", 0)
                chickens = summary.get("totalChickens", 0)
                return True, f"Rubbed {chickens} chickens, earned {feathers} feathers"
            return False, data.get("message", "Unknown error")
        else:
            return False, f"HTTP {r.status_code}"
    except Exception as e:
        return False, str(e)

# Daily rub scheduler state
daily_rub_last_date: Dict[int, str] = {}  # account_id -> last rub date "YYYY-MM-DD"

def get_hp(session: requests.Session, token_id: int) -> Tuple[Optional[int], Optional[int]]:
    for attempt in range(3):
        try:
            r = session.get(HP_API.format(token_id=token_id), timeout=30)
            if r.status_code != 200:
                if attempt < 2: time.sleep(2)
                continue
            data = r.json()
            if isinstance(data, list) and data:
                data = data[0]
            if isinstance(data, dict):
                cur = None
                max_hp = None
                for key in ("currentHP", "currentHp", "current_hp", "hpCurrent", "health"):
                    val = data.get(key)
                    if val is not None:
                        try: cur = int(val); break
                        except: continue
                for key in ("hp", "maxHP", "maxHp", "max_hp", "hpMax", "maxHealth"):
                    val = data.get(key)
                    if val is not None:
                        try: max_hp = int(val); break
                        except: continue
                if cur is None or max_hp is None:
                    for nk in ("data", "chicken", "fighter", "stats"):
                        nested = data.get(nk)
                        if isinstance(nested, dict):
                            if cur is None:
                                for key in ("currentHP", "currentHp", "hpCurrent", "health"):
                                    val = nested.get(key)
                                    if val is not None:
                                        try: cur = int(val); break
                                        except: continue
                            if max_hp is None:
                                for key in ("hp", "maxHP", "maxHp", "hpMax", "maxHealth"):
                                    val = nested.get(key)
                                    if val is not None:
                                        try: max_hp = int(val); break
                                        except: continue
                if cur is not None and max_hp is not None:
                    return cur, max_hp
            if attempt < 2: time.sleep(2)
        except:
            if attempt < 2: time.sleep(2)
    return None, None

def heal_chicken_once(token_id: int, address: str, jwt_token: str, csrf_secret: str) -> bool:
    heal_session = None
    try:
        heal_session = requests.Session()
        heal_session.timeout = 30
        heal_session.cookies.update({"jwt": jwt_token, "_csrfSecret": csrf_secret})
        heal_session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
            "Origin": APP_URL,
            "Referer": f"{APP_URL}/inventory/chickens/{token_id}",
        })
        csrf_token = None
        for attempt in range(3):
            try:
                r = heal_session.get(CSRF_TOKEN_URL, timeout=30)
                if r.status_code == 200:
                    try: csrf_token = r.json().get("csrfToken")
                    except:
                        m = re.search(r'\{"csrfToken"\s*:\s*"([^"]+)"\}', r.text.strip())
                        if m: csrf_token = m.group(1)
                    if csrf_token: break
            except: pass
            time.sleep(1.5)
        if not csrf_token:
            if heal_session: heal_session.close()
            return False
        heal_session.cookies.update({"_csrf": csrf_token})
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
            "x-csrf-token": csrf_token,
        }
        payload = {"id": token_id, "address": address}
        resp = heal_session.post(HEAL_API, json=payload, headers=headers, timeout=30)
        heal_session.close()
        return resp.status_code in (200, 201)
    except:
        if heal_session:
            try: heal_session.close()
            except: pass
        return False

def heal_to_full(bot: BotInstance, session: requests.Session, token_id: int, address: str, jwt_token: str, csrf_secret: str) -> bool:
    for attempt in range(5):
        cur, max_hp = get_hp(session, token_id)
        if cur is not None and max_hp is not None and cur >= max_hp:
            bot.log(f"✓ HP full ({cur}/{max_hp})", "success")
            return True
        bot.log(f"🩹 Healing #{token_id} (attempt {attempt+1}, HP: {cur}/{max_hp})")
        if not heal_chicken_once(token_id, address, jwt_token, csrf_secret):
            bot.log("✗ Heal failed", "error")
            return False
        bot.log("✓ Heal sent", "success")
        time.sleep(3)
    cur, max_hp = get_hp(session, token_id)
    if cur is not None and max_hp is not None and cur >= max_hp:
        bot.log(f"✓ HP full ({cur}/{max_hp})", "success")
        return True
    return False

def check_chicken_ready(session: requests.Session, token_id: int, config: dict) -> Tuple[bool, str]:
    try:
        r = session.get(CHICKEN_API.format(token_id=token_id), timeout=30)
        if r.status_code != 200:
            return False, "Could not check chicken status"
        data = r.json()
        
        if config.get("check_affection"):
            affection = data.get("affection")
            if affection is not None:
                try:
                    val = int(affection)
                    if val < config.get("min_affection", 90):
                        return False, f"Low affection ({val}/{config.get('min_affection', 90)})"
                except:
                    return False, "Cannot read affection"
            else:
                return False, "No affection data"
        
        if config.get("require_immortal", True):
            if not data.get("isImmortal", False):
                return False, "Not immortal"
        
        boosters = data.get("boosters", {})
        if isinstance(boosters, dict):
            active = sum(1 for v in boosters.values() if int(v) > 0)
            if active < config.get("min_boosters", 4):
                return False, f"Low boosters ({active}/{config.get('min_boosters', 4)})"
        
        return True, "Ready"
    except Exception as e:
        return False, f"Error: {e}"

def queue_for_match(session: requests.Session, token_id: int, jwt_token: str, battle_items: list) -> Tuple[bool, Optional[str], Optional[str], Optional[str]]:
    try:
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
            "Origin": APP_URL,
            "Referer": f"{APP_URL}/inventory/chickens/{token_id}"
        }
        payload = {"fighterId": token_id, "jwt": jwt_token, "rns": None}
        if battle_items:
            payload["battleItems"] = battle_items
        r = session.post(MATCHMAKE_URL, json=payload, headers=headers, timeout=30)
        if r.status_code not in (200, 201, 202):
            return False, None, None, None
        data = r.json()
        room = data.get("room", {})
        room_id = room.get("roomId") or data.get("roomId")
        process_id = room.get("processId")
        session_id = data.get("sessionId")
        return True, room_id, process_id, session_id
    except:
        return False, None, None, None

async def poll_for_result(session: requests.Session, token_id: int, jwt_token: str,
                          queued_at: datetime, timeout: int) -> Tuple[Optional[str], Optional[int]]:
    deadline = time.time() + timeout
    poll_count = 0
    while time.time() < deadline:
        try:
            poll_count += 1
            headers = {"Authorization": f"Bearer {jwt_token}"}
            url = MATCHES_API.format(token_id=token_id)
            r = await asyncio.to_thread(session.get, url, headers=headers, timeout=30)
            if r.status_code == 200:
                data = r.json()
                matches = data.get("matches", [])
                for match in matches:
                    date_str = match.get("date")
                    if not date_str: continue
                    try:
                        match_time = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    except: continue
                    if match_time <= queued_at: continue
                    outcome = (match.get("outcome") or "").lower()
                    if outcome in ("won", "lost", "draw", "tie"):
                        mmr = int(match.get("mmrChange") or 0)
                        return outcome, mmr
            await asyncio.sleep(POLL_INTERVAL)
        except:
            await asyncio.sleep(POLL_INTERVAL)
    return None, None

async def wait_for_match_result(bot: BotInstance, session: requests.Session, token_id: int, jwt_token: str,
                                room_id: str, process_id: str, session_id: str,
                                queued_at: datetime) -> Tuple[Optional[str], Optional[int]]:
    bot.log(f"⏳ Waiting for result (timeout: {MATCH_TIMEOUT//60}min)")
    
    async def websocket_monitor():
        if not (room_id and process_id and session_id):
            return None, None
        ws_url = f"wss://game.sabongsaga-services.com/{process_id}/{room_id}?sessionId={session_id}"
        try:
            import ssl
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
            async with websockets.connect(ws_url, ssl=ssl_ctx, ping_interval=20, ping_timeout=120) as ws:
                bot.log("[WS] Connected!", "success")
                try: await ws.send(b'\x0a')
                except: pass
                deadline = time.time() + MATCH_TIMEOUT
                while time.time() < deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=30.0)
                    except asyncio.TimeoutError:
                        try: await ws.send(b'\x0a')
                        except: pass
                        continue
                    except Exception as e:
                        err_str = str(e).lower()
                        if "locked" in err_str or "already" in err_str:
                            bot.log("[WS] Chicken locked - HTTP fallback", "warn")
                            return None, None
                        if "don't own" in err_str or "dont own" in err_str:
                            item_match = re.search(r'#(\d+)', str(e))
                            if item_match:
                                return "ITEM_ERROR", int(item_match.group(1))
                            return "ITEM_ERROR", 0
                        break
                    try:
                        payload = msg.decode("utf-8", errors="ignore") if isinstance(msg, (bytes, bytearray)) else str(msg)
                        if '"outcome"' in payload.lower():
                            outcome = None
                            if '"won"' in payload.lower(): outcome = "won"
                            elif '"lost"' in payload.lower(): outcome = "lost"
                            elif '"draw"' in payload.lower() or '"tie"' in payload.lower(): outcome = "draw"
                            if outcome:
                                mmr = 0
                                mmr_match = re.search(r'"mmrChange"\s*:\s*(-?\d+)', payload)
                                if mmr_match: mmr = int(mmr_match.group(1))
                                return outcome, mmr
                    except: pass
        except Exception as e:
            err_str = str(e).lower()
            if "don't own" in err_str or "dont own" in err_str:
                item_match = re.search(r'#(\d+)', str(e))
                if item_match:
                    return "ITEM_ERROR", int(item_match.group(1))
                return "ITEM_ERROR", 0
            if "locked" not in err_str and "already" not in err_str:
                bot.log(f"[WS] Error: {e}", "warn")
        return None, None
    
    async def http_monitor():
        return await poll_for_result(session, token_id, jwt_token, queued_at, MATCH_TIMEOUT)
    
    try:
        tasks = [asyncio.create_task(websocket_monitor()), asyncio.create_task(http_monitor())]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=MATCH_TIMEOUT)
        for task in done:
            outcome, mmr = task.result()
            if outcome:
                for p in pending: p.cancel()
                return outcome, mmr
        for p in pending: p.cancel()
    except:
        pass
    return None, None

# =========================
# Main Bot Loop
# =========================
async def run_bot_loop(bot: BotInstance):
    config = bot.config
    address = config["address"]
    jwt_token = config["jwt"]
    csrf_secret = config.get("csrf_secret", "VeTXMPhBzWCRRpSJe8Yz7VFm")
    primary_id = config["primary_chicken"]
    backups = json.loads(config.get("backup_chickens", "[]")) if isinstance(config.get("backup_chickens"), str) else config.get("backup_chickens", [])
    battle_items_priority = json.loads(config.get("battle_items", "[]")) if isinstance(config.get("battle_items"), str) else config.get("battle_items", [])
    
    session = requests.Session()
    session.timeout = 30
    last_match_time = datetime.now(timezone.utc) - timedelta(minutes=1)
    
    bot.log(f"🐔 Bot started for {config.get('nickname', address[:10])}")
    bot.log(f"Primary: #{primary_id} | Backups: {backups}")
    bot.log(f"Battle Items: {battle_items_priority}")
    
    # Track refresh token
    refresh_token = config.get("refresh_token", "")
    
    while bot.running:
        try:
            bot.current_round += 1
            round_num = bot.current_round
            bot.log(f"── ROUND {round_num} ──")
            await broadcast_stats(bot.account_id)
            
            # === AUTO JWT REFRESH ===
            hrs = jwt_hours_left(jwt_token)
            if hrs < 2 and refresh_token:
                bot.log(f"🔑 JWT expires in {hrs:.1f}h, refreshing...", "warn")
                new_jwt = await asyncio.to_thread(refresh_jwt_via_page_load, jwt_token, refresh_token, csrf_secret)
                if new_jwt:
                    jwt_token = new_jwt
                    update_account_jwt(bot.account_id, new_jwt)
                    bot.log(f"✓ JWT refreshed! New expiry: {jwt_hours_left(new_jwt):.1f}h", "success")
                else:
                    bot.log(f"✗ JWT refresh failed! {hrs:.1f}h remaining. Update JWT manually in dashboard.", "error")
            elif hrs < 1 and not refresh_token:
                bot.log(f"⚠ JWT expires in {int(hrs*60)}min! No refresh token set.", "error")
            
            # === DAILY RUB (8am PHT = 0:00 UTC) ===
            pht_now = datetime.now(timezone(timedelta(hours=8)))
            today_str = pht_now.strftime("%Y-%m-%d")
            rub_done_today = daily_rub_last_date.get(bot.account_id) == today_str
            
            if not rub_done_today and pht_now.hour >= DAILY_RUB_HOUR:
                bot.log(f"🫳 Daily rub time! ({pht_now.strftime('%I:%M %p')} PHT)")
                success, msg = await asyncio.to_thread(do_daily_rub, jwt_token, refresh_token, csrf_secret)
                if success:
                    bot.log(f"✓ {msg}", "success")
                    daily_rub_last_date[bot.account_id] = today_str
                else:
                    bot.log(f"✗ Rub failed: {msg}", "error")
            
            # Try primary chicken
            active_id = None
            use_items = True
            
            bot.log(f"📊 Checking primary #{primary_id}...")
            cur, max_hp = get_hp(session, primary_id)
            primary_ready = False
            
            if cur is not None and max_hp is not None:
                if cur < max_hp:
                    bot.log(f"❤️ HP: {cur}/{max_hp} (healing...)", "warn")
                    if heal_to_full(bot, session, primary_id, address, jwt_token, csrf_secret):
                        primary_ready = True
                    else:
                        bot.log("✗ Heal failed", "error")
                else:
                    bot.log(f"❤️ HP: {cur}/{max_hp} (full)", "success")
                    primary_ready = True
            else:
                bot.log("⚠ Could not check HP", "warn")
            
            if primary_ready:
                ready, reason = check_chicken_ready(session, primary_id, config)
                if ready:
                    active_id = primary_id
                    use_items = True
                    bot.log(f"✓ Primary #{primary_id} ready", "success")
                else:
                    bot.log(f"✗ Primary not ready: {reason}", "error")
                    primary_ready = False
            
            # Try backups
            if not primary_ready and config.get("use_backup_chickens") and backups:
                bot.log("🔄 Checking backups...")
                for bid in backups:
                    cur, max_hp = get_hp(session, bid)
                    if cur is not None and max_hp is not None and cur >= max_hp:
                        active_id = bid
                        use_items = False
                        bot.log(f"✓ Backup #{bid} ready (HP: {cur}/{max_hp})", "success")
                        break
                    else:
                        hp_str = f"{cur}/{max_hp}" if cur is not None else "unknown"
                        bot.log(f"  #{bid}: HP {hp_str} (skip)", "warn")
            
            if active_id is None:
                bot.log("✗ No chickens available, waiting 60s...", "error")
                await asyncio.sleep(60)
                continue
            
            bot.current_chicken = active_id
            await broadcast_stats(bot.account_id)
            
            # Queue
            items = list(battle_items_priority[:3]) if use_items else []
            bot.log(f"🎮 Queueing #{active_id} (items: {items or 'None'})")
            queued_at = datetime.now(timezone.utc)
            
            success, room_id, process_id, session_id = queue_for_match(session, active_id, jwt_token, items)
            
            # Item fallback for primary
            if not success and use_items:
                pool = list(battle_items_priority)
                while not success and len(pool) > 3:
                    dropped = pool.pop(0)
                    items = pool[:3]
                    bot.log(f"⚠ Dropping item #{dropped}, trying {items}", "warn")
                    success, room_id, process_id, session_id = queue_for_match(session, active_id, jwt_token, items)
                if not success:
                    bot.log("⚠ Trying without items...", "warn")
                    items = []
                    success, room_id, process_id, session_id = queue_for_match(session, active_id, jwt_token, [])
            
            if not success:
                bot.log("⚠ Queue failed, checking previous match...", "warn")
                result = await poll_for_result(session, active_id, jwt_token, last_match_time, 120)
                if result[0]:
                    outcome, mmr = result[0], result[1]
                    if outcome == "lost" and mmr > 0: mmr = -mmr
                    elif outcome == "won" and mmr < 0: mmr = abs(mmr)
                    bot.stats["matches"] += 1
                    bot.stats["total_mmr"] += mmr
                    bot.stats[f"{'wins' if outcome == 'won' else 'losses' if outcome == 'lost' else 'draws'}"] += 1
                    bot.log(f"{'🏆' if outcome=='won' else '💀' if outcome=='lost' else '🤝'} {outcome.upper()} {mmr:+d} MMR", "success" if outcome == "won" else "error")
                    last_match_time = datetime.now(timezone.utc)
                    save_match(bot.account_id, active_id, outcome, mmr, items, active_id != primary_id)
                    await broadcast_stats(bot.account_id)
                else:
                    bot.log("✗ Queue failed, retrying in 60s...", "error")
                await asyncio.sleep(60)
                continue
            
            is_backup = active_id != primary_id
            bot.log(f"✓ Queued! Room: {room_id} {'(BACKUP)' if is_backup else '(PRIMARY)'}", "success")
            
            # Wait for result
            outcome, mmr = await wait_for_match_result(bot, session, active_id, jwt_token, room_id, process_id, session_id, queued_at)
            
            if outcome == "ITEM_ERROR":
                bad_item = mmr
                if bad_item and bad_item in battle_items_priority:
                    battle_items_priority.remove(bad_item)
                    bot.log(f"🗑 Removed item #{bad_item}", "warn")
                bot.log("🔄 Re-queueing...", "info")
                continue
            elif outcome:
                if outcome == "lost" and mmr > 0: mmr = -mmr
                elif outcome == "won" and mmr < 0: mmr = abs(mmr)
                bot.stats["matches"] += 1
                bot.stats["total_mmr"] += mmr
                if outcome == "won": bot.stats["wins"] += 1
                elif outcome == "lost": bot.stats["losses"] += 1
                else: bot.stats["draws"] += 1
                emoji = "🏆" if outcome == "won" else "💀" if outcome == "lost" else "🤝"
                bot.log(f"{emoji} {outcome.upper()} {mmr:+d} MMR | Record: {bot.stats['wins']}W-{bot.stats['losses']}L-{bot.stats['draws']}D", "success" if outcome == "won" else "error" if outcome == "lost" else "warn")
                last_match_time = datetime.now(timezone.utc)
                save_match(bot.account_id, active_id, outcome, mmr, items, is_backup)
                await broadcast_stats(bot.account_id)
            else:
                bot.log("No result detected", "warn")
                last_match_time = datetime.now(timezone.utc)
            
            bot.log(f"⏱ Waiting {QUEUE_DELAY}s...")
            await asyncio.sleep(QUEUE_DELAY)
            
        except asyncio.CancelledError:
            bot.log("⏹ Bot stopped", "warn")
            break
        except Exception as e:
            bot.log(f"❌ Error: {e}", "error")
            await asyncio.sleep(30)
    
    bot.running = False
    await broadcast_stats(bot.account_id)

def save_match(account_id: int, chicken_id: int, outcome: str, mmr: int, items: list, is_backup: bool):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO match_history (account_id, chicken_id, outcome, mmr_change, battle_items, is_backup) VALUES (?, ?, ?, ?, ?, ?)",
            (account_id, chicken_id, outcome, mmr, json.dumps(items), int(is_backup))
        )
        conn.commit()
        conn.close()
    except: pass

# =========================
# FastAPI App
# =========================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield
    # Stop all bots on shutdown
    for bot in bot_instances.values():
        bot.running = False
        if bot.task:
            bot.task.cancel()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# =========================
# Auth
# =========================
AUTH_TOKENS: Dict[str, datetime] = {}

class LoginRequest(BaseModel):
    password: str

def verify_token(request: Request):
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    if not token or token not in AUTH_TOKENS:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if AUTH_TOKENS[token] < datetime.now():
        del AUTH_TOKENS[token]
        raise HTTPException(status_code=401, detail="Token expired")
    return token

@app.post("/api/login")
async def login(req: LoginRequest):
    conn = get_db()
    row = conn.execute("SELECT value FROM app_config WHERE key = 'password_hash'").fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=500, detail="No password configured")
    if hashlib.sha256(req.password.encode()).hexdigest() != row["value"]:
        raise HTTPException(status_code=401, detail="Wrong password")
    token = secrets.token_hex(32)
    AUTH_TOKENS[token] = datetime.now() + timedelta(hours=24)
    return {"token": token}

@app.post("/api/change-password")
async def change_password(request: Request, _=Depends(verify_token)):
    data = await request.json()
    new_pass = data.get("password")
    if not new_pass or len(new_pass) < 4:
        raise HTTPException(status_code=400, detail="Password too short")
    conn = get_db()
    conn.execute("UPDATE app_config SET value = ? WHERE key = 'password_hash'", (hashlib.sha256(new_pass.encode()).hexdigest(),))
    conn.commit()
    conn.close()
    return {"success": True}

# =========================
# Account CRUD
# =========================
class AccountCreate(BaseModel):
    nickname: str
    address: str
    jwt: str
    refresh_token: str = ""
    csrf_secret: str = "VeTXMPhBzWCRRpSJe8Yz7VFm"
    primary_chicken: int
    backup_chickens: List[int] = []
    battle_items: List[int] = [54, 97, 90, 47, 45, 98]
    check_affection: bool = False
    use_backup_chickens: bool = True
    require_immortal: bool = True
    min_affection: int = 90
    min_boosters: int = 0

@app.get("/api/accounts")
async def list_accounts(_=Depends(verify_token)):
    conn = get_db()
    rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
    conn.close()
    accounts = []
    for r in rows:
        acc = dict(r)
        acc["backup_chickens"] = json.loads(acc["backup_chickens"])
        acc["battle_items"] = json.loads(acc["battle_items"])
        acc["check_affection"] = bool(acc["check_affection"])
        acc["use_backup_chickens"] = bool(acc["use_backup_chickens"])
        acc["require_immortal"] = bool(acc["require_immortal"])
        acc["enabled"] = bool(acc["enabled"])
        # Add bot status
        bot = bot_instances.get(acc["id"])
        acc["bot_running"] = bot.running if bot else False
        acc["bot_stats"] = bot.stats if bot else None
        acc["jwt_hours_left"] = jwt_hours_left(acc["jwt"])
        accounts.append(acc)
    return accounts

@app.post("/api/accounts")
async def create_account(acc: AccountCreate, _=Depends(verify_token)):
    conn = get_db()
    conn.execute(
        """INSERT INTO accounts (nickname, address, jwt, refresh_token, csrf_secret, primary_chicken, backup_chickens, battle_items,
           check_affection, use_backup_chickens, require_immortal, min_affection, min_boosters)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (acc.nickname, acc.address, acc.jwt, acc.refresh_token, acc.csrf_secret, acc.primary_chicken,
         json.dumps(acc.backup_chickens), json.dumps(acc.battle_items),
         int(acc.check_affection), int(acc.use_backup_chickens), int(acc.require_immortal),
         acc.min_affection, acc.min_boosters)
    )
    conn.commit()
    account_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    return {"id": account_id, "success": True}

@app.put("/api/accounts/{account_id}")
async def update_account(account_id: int, request: Request, _=Depends(verify_token)):
    data = await request.json()
    conn = get_db()
    
    # Build update query dynamically
    allowed = ["nickname", "address", "jwt", "refresh_token", "csrf_secret", "primary_chicken", "backup_chickens",
               "battle_items", "check_affection", "use_backup_chickens", "require_immortal",
               "min_affection", "min_boosters", "enabled"]
    sets = []
    vals = []
    for key in allowed:
        if key in data:
            val = data[key]
            if key in ("backup_chickens", "battle_items"):
                val = json.dumps(val)
            elif key in ("check_affection", "use_backup_chickens", "require_immortal", "enabled"):
                val = int(val)
            sets.append(f"{key} = ?")
            vals.append(val)
    
    if sets:
        sets.append("updated_at = datetime('now')")
        vals.append(account_id)
        conn.execute(f"UPDATE accounts SET {', '.join(sets)} WHERE id = ?", vals)
        conn.commit()
    conn.close()
    return {"success": True}

@app.delete("/api/accounts/{account_id}")
async def delete_account(account_id: int, _=Depends(verify_token)):
    # Stop bot first
    if account_id in bot_instances:
        bot = bot_instances[account_id]
        bot.running = False
        if bot.task: bot.task.cancel()
        del bot_instances[account_id]
    conn = get_db()
    conn.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
    conn.execute("DELETE FROM match_history WHERE account_id = ?", (account_id,))
    conn.commit()
    conn.close()
    return {"success": True}

# =========================
# Bot Control
# =========================
@app.post("/api/bot/{account_id}/start")
async def start_bot(account_id: int, _=Depends(verify_token)):
    if account_id in bot_instances and bot_instances[account_id].running:
        raise HTTPException(status_code=400, detail="Bot already running")
    
    conn = get_db()
    row = conn.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail="Account not found")
    
    config = dict(row)
    config["backup_chickens"] = json.loads(config["backup_chickens"])
    config["battle_items"] = json.loads(config["battle_items"])
    config["check_affection"] = bool(config["check_affection"])
    config["use_backup_chickens"] = bool(config["use_backup_chickens"])
    config["require_immortal"] = bool(config["require_immortal"])
    
    bot = BotInstance(account_id, config)
    bot.running = True
    bot.task = asyncio.create_task(run_bot_loop(bot))
    bot_instances[account_id] = bot
    
    return {"success": True, "message": "Bot started"}

@app.post("/api/bot/{account_id}/stop")
async def stop_bot(account_id: int, _=Depends(verify_token)):
    if account_id not in bot_instances or not bot_instances[account_id].running:
        raise HTTPException(status_code=400, detail="Bot not running")
    
    bot = bot_instances[account_id]
    bot.running = False
    if bot.task:
        bot.task.cancel()
    bot.log("⏹ Bot stopped by user", "warn")
    
    return {"success": True, "message": "Bot stopped"}

# =========================
# Match History
# =========================
@app.get("/api/matches/{account_id}")
async def get_matches(account_id: int, limit: int = 50, _=Depends(verify_token)):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM match_history WHERE account_id = ? ORDER BY created_at DESC LIMIT ?",
        (account_id, limit)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

# =========================
# WebSocket for Live Logs
# =========================
@app.websocket("/ws/{account_id}")
async def websocket_endpoint(websocket: WebSocket, account_id: int):
    # Verify auth via query param
    token = websocket.query_params.get("token", "")
    if not token or token not in AUTH_TOKENS:
        await websocket.close(code=4001, reason="Unauthorized")
        return
    
    await websocket.accept()
    
    if account_id not in ws_connections:
        ws_connections[account_id] = []
    ws_connections[account_id].append(websocket)
    
    # Send existing logs
    if account_id in bot_instances:
        bot = bot_instances[account_id]
        for log_entry in bot.logs[-100:]:
            try:
                await websocket.send_json({"type": "log", "data": json.loads(log_entry)})
            except: break
        await broadcast_stats(account_id)
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        if account_id in ws_connections:
            ws_connections[account_id] = [ws for ws in ws_connections[account_id] if ws != websocket]

# =========================
# Serve Frontend
# =========================
@app.get("/")
async def serve_frontend():
    return FileResponse("static/index.html")

@app.get("/{path:path}")
async def catch_all(path: str):
    file_path = f"static/{path}"
    if os.path.exists(file_path):
        return FileResponse(file_path)
    return FileResponse("static/index.html")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
