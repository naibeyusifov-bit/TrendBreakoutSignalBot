"""
TREND BREAKOUT BOT (OPTIMIZED & TELEGRAM READY) - FIXED
===============================================
- TELEGRAM INTEGRATION: Həm Long Polling (getUpdates), həm də Webhook dəstəyi.
- NO REPAINT           : İndikatorlar və breakout tam bağlanmış şamlar (-2) üzrə.
- THREAD-SAFE          : Lock-lar ilə yaddaş və SQLite toqquşmasının qarşısı alınıb.
- TEST READY           : Günlük trade limiti test üçün limitsizdir (99999).

DÜZƏLİŞLƏR (bu versiyada):
1. update_trailing_stops: race condition düzəldildi - bütün oxuma/yazma eyni lock daxilində
2. fetch_klines / fetch_price: ardıcıl uğursuzluqlar sayılır, N dəfədən sonra Telegram xəbərdarlığı
3. Startup mesajı: yalnız faktiki ilk başlanğıcda göndərilir (env dəyişəni ilə söndürülə bilər)
4. PID lock faylı: proses bitəndə (normal çıxışda) təmizlənir
5. atexit ilə səliyyəli bağlanma
"""

import os
import sys
import time
import atexit
import sqlite3
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request


# ============================================================
# CONFIG (MƏLUMATLARINIZI BURAYA ƏLAVƏ EDİN)
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

# Long Polling rejimini aktiv saxlayın (Webhook qurmağa ehtiyac qalmır)
USE_POLLING = os.getenv("USE_POLLING", "True").lower() == "true"

# Restart zamanı "BOT AKTİVDİR" mesajının göndərilib-göndərilməyəcəyi
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "True").lower() == "true"

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
BYBIT_TICKER_URL = "https://api.bybit.com/v5/market/tickers"

# ------------------------------------------------------------
# PROXY (Bybit bəzi bulud provayderlərin IP-lərini blok edir)
# ------------------------------------------------------------
# Format: http://istifadeci:sifre@proxy-host:port  (və ya http://proxy-host:port)
# Boş saxlasan proxy istifadə olunmur (birbaşa qoşulur).
BYBIT_PROXY_URL = os.getenv("BYBIT_PROXY_URL", "").strip()
BYBIT_PROXIES = {"http": BYBIT_PROXY_URL, "https": BYBIT_PROXY_URL} if BYBIT_PROXY_URL else None

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

TRADE_TF = "15"
TREND_TF = "60"
MAX_CANDLES = 300

DONCHIAN_PERIOD = 20
ATR_PERIOD = 14
EMA_TREND_PERIOD = 200
CHANDELIER_ATR_MULT = 3.0

CANDLE_POLL_SECONDS = 60
PRICE_POLL_SECONDS = 10

ACCOUNT_BALANCE_USDT = float(os.getenv("ACCOUNT_BALANCE_USDT", "1000"))
RISK_PER_TRADE_PCT = 0.01

MAX_TRADES_PER_DAY = int(os.getenv("MAX_TRADES_PER_DAY", "99999"))
MAX_CONSECUTIVE_LOSSES = 3
COOLDOWN_HOURS_AFTER_LOSSES = 24

# Neçə ardıcıl uğursuz API sorğusundan sonra Telegram-a xəbərdarlıq göndərilsin
MAX_CONSECUTIVE_FETCH_FAILS = 5

# ------------------------------------------------------------
# SİQNAL PUANLAMASI (SCORING)
# ------------------------------------------------------------
# BTC bu qrupa daxil deyil - həmişə müstəqil işləyir (özəl filtr yoxdur).
# ETH və SOL eyni vaxtda breakout versə, yalnız ən yüksək balı olan açılır.
SCORE_GROUP_SYMBOLS = ["ETHUSDT", "SOLUSDT"]

# 100 üzərindən minimum keçid balı - bundan aşağı olan siqnal (qrup daxilində) açılmır
MIN_SIGNAL_SCORE = float(os.getenv("MIN_SIGNAL_SCORE", "50"))

# ------------------------------------------------------------
# SİQNAL FİLTRLƏRİ (hamısı ayrı-ayrı açılıb-bağlana bilər)
# ------------------------------------------------------------
# QEYD: Bu filtrlər yalnız breakout AŞKARLANANDA işə düşür (hər poll dövründə yox),
# ona görə əsas candle_worker dövrəsini yavaşlatmır.

# 1) Həcm təsdiqi: breakout şamının həcmi son 20 şamın ortalamasından
#    ən azı bu əmsal qədər yüksək olmalıdır
ENABLE_VOLUME_FILTER = os.getenv("ENABLE_VOLUME_FILTER", "True").lower() == "true"
MIN_VOLUME_RATIO = float(os.getenv("MIN_VOLUME_RATIO", "1.5"))

# 2) ADX filtri: trend gücü bu həddən aşağıdırsa (yastı bazar) siqnal rədd edilir
ENABLE_ADX_FILTER = os.getenv("ENABLE_ADX_FILTER", "True").lower() == "true"
MIN_ADX = float(os.getenv("MIN_ADX", "20"))

# 3) Minimum volatilite filtri: ATR qiymətin bu faizindən (%) az olarsa (sıxılmış
#    bazar) siqnal rədd edilir - whipsaw riskini azaldır
ENABLE_ATR_FILTER = os.getenv("ENABLE_ATR_FILTER", "True").lower() == "true"
MIN_ATR_PCT = float(os.getenv("MIN_ATR_PCT", "0.15"))  # məs: 0.15% = qiymətin 0.0015-i

# 4) Multi-timeframe təsdiqi: breakout həm TRADE_TF, həm də TREND_TF-də
#    (daha az dövrlə) təsdiqlənməlidir
ENABLE_MTF_FILTER = os.getenv("ENABLE_MTF_FILTER", "True").lower() == "true"
TREND_DONCHIAN_PERIOD = int(os.getenv("TREND_DONCHIAN_PERIOD", "10"))

# 5) Spread/likidlik filtri: siqnal anında bid-ask spread bu faizdən genişdirsə
#    (aşağı likidlik) siqnal rədd edilir. Yalnız breakout aşkarlananda 1 əlavə
#    API sorğusu edir - pollinq dövrəsini yavaşlatmır.
ENABLE_SPREAD_FILTER = os.getenv("ENABLE_SPREAD_FILTER", "True").lower() == "true"
MAX_SPREAD_PCT = float(os.getenv("MAX_SPREAD_PCT", "0.15"))

# 6) Correlation/exposure limiti: eyni istiqamətdə (LONG və ya SHORT) maksimum
#    neçə aktiv trade ola bilər (BTC, ETH, SOL çox vaxt birgə hərəkət edir)
MAX_SAME_DIRECTION_TRADES = int(os.getenv("MAX_SAME_DIRECTION_TRADES", "2"))

# 7) Partial take-profit: 1R (risk vahidi) qazanılanda pozisiyanın yarısı
#    "bağlanır" (bildiriş göndərilir), qalanı trailing stop ilə davam edir
ENABLE_PARTIAL_TP = os.getenv("ENABLE_PARTIAL_TP", "True").lower() == "true"
PARTIAL_TP_R_MULTIPLE = float(os.getenv("PARTIAL_TP_R_MULTIPLE", "1.0"))
PARTIAL_TP_CLOSE_PCT = float(os.getenv("PARTIAL_TP_CLOSE_PCT", "0.5"))

DB_FILE = "trend_breakout.db"
PID_FILE = "trend_bot.lock"


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE & LOCKS
# ============================================================

lock = threading.Lock()
db_lock = threading.Lock()

candles_trade_tf = {s: [] for s in SYMBOLS}
candles_trend_tf = {s: [] for s in SYMBOLS}

active_trades = {}
last_signal_candle = {s: None for s in SYMBOLS}

daily_trade_count = 0
daily_count_date = None

consecutive_losses = 0
cooldown_until = None

# Ardıcıl fetch uğursuzluqlarının sayğacı (simvol -> say)
fetch_fail_counts = {s: 0 for s in SYMBOLS}
fetch_fail_alerted = {s: False for s in SYMBOLS}

_startup_done = False
_startup_lock = threading.Lock()
_owns_pid_lock = False


# ============================================================
# SINGLE INSTANCE GUARD (PID LOCK)
# ============================================================

def check_single_instance():
    """Botun dublikat işə düşməsinin qarşısını alır."""
    global _owns_pid_lock
    pid = str(os.getpid())
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                old_pid = int(f.read().strip())
            if old_pid != os.getpid():
                try:
                    os.kill(old_pid, 0)
                    print(f"⚠️ [PID Lock] Bot artıq başqa prosesdə işləyir (PID: {old_pid}). Təkrarlanma dayandırıldı.")
                    return False
                except (OSError, ProcessLookupError):
                    # köhnə prosess artıq yoxdur - stale lock, üzərinə yazırıq
                    pass
        except (OSError, ValueError):
            pass

    try:
        with open(PID_FILE, "w") as f:
            f.write(pid)
        _owns_pid_lock = True
        return True
    except Exception as e:
        print(f"❌ PID lock faylı xətası: {e}")
        return True


def release_pid_lock():
    """Proses normal bağlananda öz PID lock faylını təmizləyir."""
    if not _owns_pid_lock:
        return
    try:
        if os.path.exists(PID_FILE):
            with open(PID_FILE, "r") as f:
                saved_pid = f.read().strip()
            if saved_pid == str(os.getpid()):
                os.remove(PID_FILE)
    except Exception as e:
        print("⚠️ PID lock təmizləmə xətası:", e)


atexit.register(release_pid_lock)


# ============================================================
# DATABASE (THREAD-SAFE)
# ============================================================

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=15)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                initial_stop REAL NOT NULL,
                exit_price REAL,
                status TEXT NOT NULL,
                position_size_usdt REAL,
                created_at REAL NOT NULL,
                closed_at REAL
            )
        """)
        conn.commit()
        conn.close()


def save_trade(trade):
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO trades
                (symbol, side, entry, initial_stop, exit_price, status,
                 position_size_usdt, created_at, closed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade["symbol"], trade["side"], trade["entry"],
                trade["initial_stop"], trade.get("exit_price"),
                trade["status"], trade.get("position_size_usdt"),
                trade["created_at"], trade.get("closed_at"),
            ))
            conn.commit()
            conn.close()
    except Exception as e:
        print("❌ save_trade xətası:", e)


def get_statistics():
    try:
        with db_lock:
            conn = sqlite3.connect(DB_FILE, timeout=15)
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*),
                       COALESCE(SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END), 0),
                       COALESCE(SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END), 0)
                FROM trades
            """)
            total, wins, losses = cur.fetchone()
            conn.close()
        win_rate = round((wins / total) * 100, 2) if total else 0
        return {"total": total, "wins": wins, "losses": losses, "win_rate": win_rate}
    except Exception as e:
        print("❌ get_statistics xətası:", e)
        return {"total": 0, "wins": 0, "losses": 0, "win_rate": 0}


init_db()


# ============================================================
# TELEGRAM ENGINE (DISPATCH & POLLING)
# ============================================================

def send_telegram(message, chat_id=None, parse_mode=None):
    token = TELEGRAM_BOT_TOKEN
    target_chat_id = chat_id or TELEGRAM_CHAT_ID

    if not token or token == "YOUR_BOT_TOKEN_HERE" or not target_chat_id:
        print("❌ Telegram token və ya chat_id təyin edilməyib.")
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": target_chat_id, "text": message}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        r = requests.post(url, json=payload, timeout=10)
        res = r.json()
        if not res.get("ok"):
            # Əgər Markdown format xətası verərsə, adi mətn kimi yenidən cəhd et
            if parse_mode:
                payload.pop("parse_mode", None)
                r = requests.post(url, json=payload, timeout=10)
                return r.json().get("ok", False)
            print("❌ Telegram xətası:", res)
            return False
        return True
    except Exception as e:
        print("❌ Telegram bağlantı xətası:", e)
        return False


def process_telegram_update(update_data):
    """Gələn Telegram mesajlarını emal edir və cavablandırır."""
    if not update_data or "message" not in update_data:
        return

    msg = update_data["message"]
    chat_id = msg.get("chat", {}).get("id")
    raw_text = msg.get("text", "").strip()

    if not chat_id or not raw_text:
        return

    # Bot istifadəçi adını temizləyirik (/status@BotName -> /status)
    cmd = raw_text.split("@")[0].strip().lower()

    response_text = ""

    if cmd in ["/start", "/help", "komek", "kömək", "yardim", "yardım"]:
        response_text = (
            "🤖 *TREND BREAKOUT BOT ƏMRLƏRİ*\n\n"
            "📊 /stats - Ümumi WIN/LOSS və Win Rate\n"
            "⚡ /active - Açıq olan pozisiyalar\n"
            "🟢 /status - Botun vəziyyəti və günlük limitlər\n"
            "❓ /help - Bu menyu"
        )

    elif cmd in ["/status", "status"]:
        with lock:
            active_count = len(active_trades)
            reset_daily_counter_if_needed()
            current_daily = daily_trade_count
            current_cooldown = cooldown_until

        cooldown_str = "Aktiv deyil"
        if current_cooldown:
            cooldown_str = current_cooldown.strftime("%d.%m.%Y %H:%M UTC")

        limit_str = "Limitsiz (Test)" if MAX_TRADES_PER_DAY >= 9999 else str(MAX_TRADES_PER_DAY)

        response_text = (
            "🤖 *BOT VƏZİYYƏTİ*\n\n"
            "🟢 Status: ONLINE\n"
            f"📈 Açıq Trade Sayı: {active_count}\n"
            f"📅 Bugünkü Trade Sayı: {current_daily}/{limit_str}\n"
            f"❄️ Cooldown: {cooldown_str}"
        )

    elif cmd in ["/stats", "stats", "statistika"]:
        stats = get_statistics()
        response_text = (
            "📊 *ÜMUMİ STATİSTİKA*\n\n"
            f"Cəmi Trade: {stats['total']}\n"
            f"✅ WIN: {stats['wins']}\n"
            f"❌ LOSS: {stats['losses']}\n"
            f"🎯 Win Rate: %{stats['win_rate']}"
        )

    elif cmd in ["/active", "active", "aciq"]:
        with lock:
            trades_list = list(active_trades.values())

        if not trades_list:
            response_text = "ℹ️ Hal-hazırda aktiv trade yoxdur."
        else:
            response_text = "⚡ *AÇIQ TRADELƏR*\n\n"
            for t in trades_list:
                emoji = "🟢" if t["side"] == "LONG" else "🔴"
                response_text += (
                    f"{emoji} *{t['symbol']} {t['side']}*\n"
                    f"Entry: `{t['entry']:.4f}`\n"
                    f"Trailing Stop: `{t['trailing_stop']:.4f}`\n"
                    f"Həcm: ~`{t['position_size_usdt']:.2f}` USDT\n\n"
                )

    if response_text:
        send_telegram(response_text, chat_id=chat_id, parse_mode="Markdown")


def telegram_polling_worker():
    """Webhook olmadan Telegram əmrlərini canlı dinləmək üçün Polling servisi."""
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("⚠️ TELEGRAM_BOT_TOKEN təyin edilmədiyi üçün Polling işə düşmədi.")
        return

    print("🤖 Telegram Long Polling başladıldı...")

    # Köhnə Webhook-u silirik ki, Polling rejimində toqquşma olmasın
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook", timeout=10)
    except Exception as e:
        print("⚠️ deleteWebhook xətası:", e)

    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            params = {"offset": offset, "timeout": 20}
            resp = requests.get(url, params=params, timeout=25)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("ok"):
                    for update in data.get("result", []):
                        offset = update["update_id"] + 1
                        process_telegram_update(update)
        except Exception as e:
            print("❌ Telegram Polling xətası:", e)
            time.sleep(5)
        time.sleep(1)


# ============================================================
# BYBIT REST
# ============================================================

def fetch_klines(symbol, interval, limit=MAX_CANDLES):
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    try:
        r = requests.get(BYBIT_KLINE_URL, params=params, timeout=15, proxies=BYBIT_PROXIES)
        data = r.json()
        if data.get("retCode") != 0:
            print(f"❌ Bybit kline xətası {symbol}:", data)
            _note_fetch_failure(symbol)
            return []
        rows = data["result"]["list"]
        rows.reverse()
        _note_fetch_success(symbol)
        return [{
            "time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
        } for row in rows]
    except Exception as e:
        print(f"❌ {symbol} kline sorğu xətası:", e)
        _note_fetch_failure(symbol)
        return []


def fetch_price(symbol):
    params = {"category": "linear", "symbol": symbol}
    try:
        r = requests.get(BYBIT_TICKER_URL, params=params, timeout=10, proxies=BYBIT_PROXIES)
        data = r.json()
        if data.get("retCode") != 0:
            _note_fetch_failure(symbol)
            return None
        lst = data["result"]["list"]
        if not lst:
            _note_fetch_failure(symbol)
            return None
        _note_fetch_success(symbol)
        return float(lst[0]["lastPrice"])
    except Exception as e:
        print(f"❌ {symbol} qiymət sorğu xətası:", e)
        _note_fetch_failure(symbol)
        return None


def fetch_spread_pct(symbol):
    """
    Bid-ask spread-i faiz kimi qaytarır. Yalnız breakout aşkarlananda çağırılır,
    ona görə əsas poll dövrəsinə əlavə yük gətirmir.
    """
    params = {"category": "linear", "symbol": symbol}
    try:
        r = requests.get(BYBIT_TICKER_URL, params=params, timeout=10, proxies=BYBIT_PROXIES)
        data = r.json()
        if data.get("retCode") != 0:
            return None
        lst = data["result"]["list"]
        if not lst:
            return None
        bid = float(lst[0].get("bid1Price", 0) or 0)
        ask = float(lst[0].get("ask1Price", 0) or 0)
        if bid <= 0 or ask <= 0:
            return None
        mid = (bid + ask) / 2
        return ((ask - bid) / mid) * 100
    except Exception as e:
        print(f"⚠️ {symbol} spread sorğu xətası:", e)
        return None


def _note_fetch_failure(symbol):
    alert_needed = False
    with lock:
        fetch_fail_counts[symbol] += 1
        if fetch_fail_counts[symbol] >= MAX_CONSECUTIVE_FETCH_FAILS and not fetch_fail_alerted[symbol]:
            fetch_fail_alerted[symbol] = True
            alert_needed = True
    if alert_needed:
        send_telegram(
            f"⚠️ {symbol}: Bybit-dən {MAX_CONSECUTIVE_FETCH_FAILS} ardıcıl dəfə "
            f"məlumat alına bilmədi. Şəbəkə/API problemi ola bilər."
        )


def _note_fetch_success(symbol):
    was_alerted = False
    with lock:
        if fetch_fail_alerted[symbol]:
            was_alerted = True
        fetch_fail_counts[symbol] = 0
        fetch_fail_alerted[symbol] = False
    if was_alerted:
        send_telegram(f"✅ {symbol}: Bybit bağlantısı bərpa olundu, məlumat axını normaldır.")


# ============================================================
# İNDİKATORLAR
# ============================================================

def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    result = sum(values[:period]) / period
    for v in values[period:]:
        result = (v - result) * k + result
    return result


def atr(candles, period=ATR_PERIOD):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l = candles[i]["high"], candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period


def donchian_channel(candles, period, exclude_last=1):
    window = candles[-(period + exclude_last):-exclude_last] if exclude_last else candles[-period:]
    if len(window) < period:
        return None, None
    return max(c["high"] for c in window), min(c["low"] for c in window)


def calc_adx(candles, period=14):
    """
    Wilder's ADX - trendin GÜCÜNÜ ölçür (istiqamət deyil).
    0-20  : zəif/yastı bazar (breakout-lar çox vaxt yalançı çıxır)
    20-40 : orta/güclü trend
    40+   : çox güclü trend
    """
    if len(candles) < period * 2 + 1:
        return None

    plus_dm, minus_dm, trs = [], [], []
    for i in range(1, len(candles)):
        up_move = candles[i]["high"] - candles[i - 1]["high"]
        down_move = candles[i - 1]["low"] - candles[i]["low"]
        plus_dm.append(up_move if (up_move > down_move and up_move > 0) else 0)
        minus_dm.append(down_move if (down_move > up_move and down_move > 0) else 0)
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    def wilder_smooth(values, period):
        if len(values) < period:
            return []
        smoothed = [sum(values[:period])]
        for v in values[period:]:
            smoothed.append(smoothed[-1] - (smoothed[-1] / period) + v)
        return smoothed

    tr_smooth = wilder_smooth(trs, period)
    plus_smooth = wilder_smooth(plus_dm, period)
    minus_smooth = wilder_smooth(minus_dm, period)

    if not tr_smooth or not plus_smooth or not minus_smooth:
        return None

    n = min(len(tr_smooth), len(plus_smooth), len(minus_smooth))
    dx = []
    for i in range(n):
        t = tr_smooth[i]
        if t == 0:
            dx.append(0)
            continue
        p_di = 100 * (plus_smooth[i] / t)
        m_di = 100 * (minus_smooth[i] / t)
        denom = p_di + m_di
        dx.append(100 * abs(p_di - m_di) / denom if denom else 0)

    if len(dx) < period:
        return None

    adx_val = sum(dx[:period]) / period
    for d in dx[period:]:
        adx_val = (adx_val * (period - 1) + d) / period
    return adx_val


def calculate_signal_score(side, entry, donchian_high, donchian_low, current_atr,
                            adx_value, breakout_volume, avg_volume):
    """
    Siqnalı 0-100 arası balla qiymətləndirir. 3 komponentdən ibarətdir:
      - ADX (trend gücü)          : max 40 xal
      - Breakout məsafəsi (ATR-ə görə) : max 30 xal
      - Həcm təsdiqi (ortalamaya nisbət): max 30 xal
    """
    adx_value = adx_value or 0
    score_adx = min(adx_value / 40.0, 1.0) * 40

    if current_atr and current_atr > 0:
        if side == "LONG":
            breakout_distance = max((entry - donchian_high) / current_atr, 0)
        else:
            breakout_distance = max((donchian_low - entry) / current_atr, 0)
    else:
        breakout_distance = 0
    score_breakout = min(breakout_distance / 1.0, 1.0) * 30

    if avg_volume and avg_volume > 0:
        volume_ratio = breakout_volume / avg_volume
    else:
        volume_ratio = 1.0
    score_volume = min(volume_ratio / 2.0, 1.0) * 30

    total = round(score_adx + score_breakout + score_volume, 1)
    breakdown = {
        "adx": round(score_adx, 1),
        "breakout": round(score_breakout, 1),
        "volume": round(score_volume, 1),
    }
    return total, breakdown


# ============================================================
# RİSK İDARƏETMƏSİ
# ============================================================

def reset_daily_counter_if_needed():
    """DİQQƏT: yalnız `lock` artıq tutulmuş halda çağırılmalıdır."""
    global daily_trade_count, daily_count_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_count_date != today:
        daily_count_date = today
        daily_trade_count = 0


def risk_checks_pass():
    """DİQQƏT: yalnız `lock` artıq tutulmuş halda çağırılmalıdır."""
    reset_daily_counter_if_needed()
    if cooldown_until is not None:
        if datetime.now(timezone.utc) < cooldown_until:
            return False, f"Cooldown aktivdir, {cooldown_until.isoformat()} tarixinə qədər"
    if daily_trade_count >= MAX_TRADES_PER_DAY:
        return False, "Günlük trade limiti dolub"
    return True, ""


def register_trade_opened():
    """DİQQƏT: yalnız `lock` artıq tutulmuş halda çağırılmalıdır."""
    global daily_trade_count
    daily_trade_count += 1


def register_trade_result(result):
    """DİQQƏT: yalnız `lock` artıq tutulmuş halda çağırılmalıdır. Telegram göndərişi lock xaricində edilir."""
    global consecutive_losses, cooldown_until
    should_alert_cooldown = False
    if result == "LOSS":
        consecutive_losses += 1
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            cooldown_until_ts = datetime.now(timezone.utc).timestamp() + COOLDOWN_HOURS_AFTER_LOSSES * 3600
            cooldown_until = datetime.fromtimestamp(cooldown_until_ts, tz=timezone.utc)
            should_alert_cooldown = True
    else:
        consecutive_losses = 0
    return should_alert_cooldown


def calc_position_size(entry, stop):
    risk_amount = ACCOUNT_BALANCE_USDT * RISK_PER_TRADE_PCT
    stop_distance = abs(entry - stop)
    if stop_distance <= 0:
        return 0
    qty = risk_amount / stop_distance
    position_value_usdt = qty * entry
    return round(position_value_usdt, 2)


# ============================================================
# SİQNAL MƏNTİQİ (NO REPAINT - BAĞLANMIŞ ŞAMLAR)
# ============================================================

def check_for_signal(symbol):
    with lock:
        trade_data = list(candles_trade_tf[symbol])
        trend_data = list(candles_trend_tf[symbol])

    if len(trade_data) < DONCHIAN_PERIOD + 10 or len(trend_data) < EMA_TREND_PERIOD + 10:
        return None

    # Bağlanmış şam (-2)
    closed_trade_candle = trade_data[-2]
    closed_trend_candle = trend_data[-2]

    trend_closes = [c["close"] for c in trend_data[:-1]]
    trend_ema = ema(trend_closes[-(EMA_TREND_PERIOD + 50):], EMA_TREND_PERIOD)
    if trend_ema is None:
        return None

    trend_price = closed_trend_candle["close"]
    bullish_regime = trend_price > trend_ema
    bearish_regime = trend_price < trend_ema

    donchian_high, donchian_low = donchian_channel(trade_data[:-1], DONCHIAN_PERIOD, exclude_last=1)
    if donchian_high is None:
        return None

    current_atr = atr(trade_data[:-1], ATR_PERIOD)
    if current_atr is None or current_atr <= 0:
        return None

    breakout_long = closed_trade_candle["close"] > donchian_high
    breakout_short = closed_trade_candle["close"] < donchian_low

    if not (breakout_long or breakout_short):
        return None

    side = "LONG" if breakout_long else "SHORT"
    if side == "LONG" and not bullish_regime:
        return None
    if side == "SHORT" and not bearish_regime:
        return None

    # Bal hesablamaq üçün: ADX (trend gücü) və orta həcm (son 20 bağlanmış şam,
    # breakout şamı XARİC olmaqla - repaint riski olmasın deyə)
    adx_value = calc_adx(trade_data[:-1], ATR_PERIOD)
    volume_window = trade_data[:-2][-20:]
    avg_volume = (sum(c["volume"] for c in volume_window) / len(volume_window)) if volume_window else None
    breakout_volume = closed_trade_candle["volume"]

    # --- FİLTR 1: Həcm təsdiqi ---
    if ENABLE_VOLUME_FILTER and avg_volume:
        volume_ratio = breakout_volume / avg_volume if avg_volume else 0
        if volume_ratio < MIN_VOLUME_RATIO:
            print(f"⏸️ {symbol} siqnalı rədd edildi: həcm zəif ({volume_ratio:.2f}x < {MIN_VOLUME_RATIO}x)")
            return None

    # --- FİLTR 2: ADX (trend gücü) ---
    if ENABLE_ADX_FILTER and adx_value is not None:
        if adx_value < MIN_ADX:
            print(f"⏸️ {symbol} siqnalı rədd edildi: ADX zəif ({adx_value:.1f} < {MIN_ADX})")
            return None

    # --- FİLTR 3: Minimum ATR (volatilite) ---
    if ENABLE_ATR_FILTER:
        atr_pct = (current_atr / closed_trade_candle["close"]) * 100
        if atr_pct < MIN_ATR_PCT:
            print(f"⏸️ {symbol} siqnalı rədd edildi: ATR çox aşağıdır ({atr_pct:.3f}% < {MIN_ATR_PCT}%)")
            return None

    # --- FİLTR 4: Multi-timeframe təsdiqi (TREND_TF üzərində daha geniş Donchian) ---
    if ENABLE_MTF_FILTER:
        trend_donchian_high, trend_donchian_low = donchian_channel(
            trend_data[:-1], TREND_DONCHIAN_PERIOD, exclude_last=1
        )
        # Kifayət qədər trend-tf data yoxdursa filtri keçirik (bloklamırıq)
        if trend_donchian_high is not None:
            if side == "LONG" and closed_trend_candle["close"] <= trend_donchian_high:
                print(f"⏸️ {symbol} siqnalı rədd edildi: {TREND_TF}dəq trend-də breakout təsdiqlənmədi (LONG)")
                return None
            if side == "SHORT" and closed_trend_candle["close"] >= trend_donchian_low:
                print(f"⏸️ {symbol} siqnalı rədd edildi: {TREND_TF}dəq trend-də breakout təsdiqlənmədi (SHORT)")
                return None

    # --- FİLTR 5: Spread/likidlik (yalnız breakout təsdiqləndikdən sonra, 1 əlavə sorğu) ---
    if ENABLE_SPREAD_FILTER:
        spread_pct = fetch_spread_pct(symbol)
        if spread_pct is not None and spread_pct > MAX_SPREAD_PCT:
            print(f"⏸️ {symbol} siqnalı rədd edildi: spread çox geniş ({spread_pct:.3f}% > {MAX_SPREAD_PCT}%)")
            return None

    entry = closed_trade_candle["close"]
    if side == "LONG":
        initial_stop = entry - current_atr * CHANDELIER_ATR_MULT
        if initial_stop >= entry:
            return None
    else:
        initial_stop = entry + current_atr * CHANDELIER_ATR_MULT
        if initial_stop <= entry:
            return None

    score, breakdown = calculate_signal_score(
        side, entry, donchian_high, donchian_low, current_atr,
        adx_value, breakout_volume, avg_volume
    )
    return {
        "symbol": symbol, "side": side, "entry": entry,
        "initial_stop": initial_stop, "candle_time": closed_trade_candle["time"],
        "atr": current_atr, "score": score, "score_breakdown": breakdown,
    }


def open_trade(signal):
    symbol = signal["symbol"]
    trade = None

    with lock:
        if symbol in active_trades:
            return
        if last_signal_candle[symbol] == signal["candle_time"]:
            return

        ok, reason = risk_checks_pass()
        if not ok:
            print(f"⏸️ {symbol} siqnalı rədd edildi: {reason}")
            return

        # --- Correlation/exposure limiti: eyni istiqamətdə çox trade açılmasın ---
        same_direction_count = sum(
            1 for t in active_trades.values() if t["side"] == signal["side"]
        )
        if same_direction_count >= MAX_SAME_DIRECTION_TRADES:
            print(
                f"⏸️ {symbol} siqnalı rədd edildi: {signal['side']} istiqamətində "
                f"artıq {same_direction_count} aktiv trade var (limit: {MAX_SAME_DIRECTION_TRADES})"
            )
            return

        last_signal_candle[symbol] = signal["candle_time"]
        position_size = calc_position_size(signal["entry"], signal["initial_stop"])

        trade = {
            "symbol": symbol,
            "side": signal["side"],
            "entry": signal["entry"],
            "initial_stop": signal["initial_stop"],
            "trailing_stop": signal["initial_stop"],
            "extreme_price": signal["entry"],
            "atr": signal["atr"],
            "status": "ACTIVE",
            "position_size_usdt": position_size,
            "created_at": time.time(),
            "r_value": abs(signal["entry"] - signal["initial_stop"]),
            "partial_taken": False,
        }
        active_trades[symbol] = trade
        register_trade_opened()
        current_daily_count = daily_trade_count

    if trade is None:
        return

    emoji = "🟢" if trade["side"] == "LONG" else "🔴"
    limit_str = "Limitsiz (Test)" if MAX_TRADES_PER_DAY >= 9999 else str(MAX_TRADES_PER_DAY)

    score = signal.get("score")
    breakdown = signal.get("score_breakdown") or {}
    score_line = ""
    if score is not None:
        score_line = (
            f"\n🎯 Siqnal Balı: {score}/100 "
            f"(ADX:{breakdown.get('adx','-')} | Breakout:{breakdown.get('breakout','-')} | "
            f"Həcm:{breakdown.get('volume','-')})\n"
        )

    message = f"""
🚨 TREND BREAKOUT SİQNALI (TEST REJİMİ)

{emoji} {symbol} {trade["side"]}

Entry: {trade["entry"]:.4f}
İlkin Stop: {trade["initial_stop"]:.4f}
Tövsiyə olunan pozisiya: ~{trade["position_size_usdt"]:.2f} USDT
{score_line}
Səbəb: Donchian({DONCHIAN_PERIOD}) breakout + EMA{EMA_TREND_PERIOD}({TREND_TF}dəq) trend

⏳ Status: ACTIVE — Trailing Stop Aktivdir
📅 Günlük Trade Sayı: {current_daily_count}/{limit_str}
"""
    print(message)
    send_telegram(message)


def update_trailing_stops(symbol, price):
    """
    DÜZƏLİŞ: Əvvəlki versiyada trade dict-i lock daxilində götürülüb,
    lock XARİCİNDƏ dəyişdirilirdi (extreme_price, trailing_stop) - bu,
    price_worker və başqa thread-lər eyni anda toxunanda data corruption-a
    səbəb ola bilərdi. İndi bütün oxuma+yazma eyni lock bloku daxilindədir.
    """
    trade_snapshot = None
    should_alert_cooldown = False
    partial_tp_hit = None  # trade snapshot at the moment partial TP fires (for telegram, outside lock)

    with lock:
        trade = active_trades.get(symbol)
        if not trade:
            return

        # --- Partial take-profit: 1R qazanılanda "yarısı bağlanır" (bildiriş) ---
        if ENABLE_PARTIAL_TP and not trade.get("partial_taken", False) and trade.get("r_value", 0) > 0:
            r_value = trade["r_value"]
            if trade["side"] == "LONG":
                target = trade["entry"] + r_value * PARTIAL_TP_R_MULTIPLE
                hit = price >= target
            else:
                target = trade["entry"] - r_value * PARTIAL_TP_R_MULTIPLE
                hit = price <= target
            if hit:
                trade["partial_taken"] = True
                trade["partial_exit_price"] = price
                partial_tp_hit = dict(trade)

        result = None

        if trade["side"] == "LONG":
            if price > trade["extreme_price"]:
                trade["extreme_price"] = price
                new_stop = trade["extreme_price"] - trade["atr"] * CHANDELIER_ATR_MULT
                if new_stop > trade["trailing_stop"]:
                    trade["trailing_stop"] = new_stop
            if price <= trade["trailing_stop"]:
                result = "WIN" if trade["trailing_stop"] > trade["entry"] else "LOSS"
        else:
            if price < trade["extreme_price"]:
                trade["extreme_price"] = price
                new_stop = trade["extreme_price"] + trade["atr"] * CHANDELIER_ATR_MULT
                if new_stop < trade["trailing_stop"]:
                    trade["trailing_stop"] = new_stop
            if price >= trade["trailing_stop"]:
                result = "WIN" if trade["trailing_stop"] < trade["entry"] else "LOSS"

        if result is None:
            if partial_tp_hit is None:
                return
        else:
            trade["status"] = result
            trade["exit_price"] = price
            trade["closed_at"] = time.time()

            active_trades.pop(symbol, None)
            should_alert_cooldown = register_trade_result(result)

            # Telegram/DB üçün lock xaricinə çıxaracağımız dəyişməz snapshot
            trade_snapshot = dict(trade)

    # --- Partial TP bildirişi (lock xaricində göndərilir) ---
    if partial_tp_hit is not None:
        pct = int(PARTIAL_TP_CLOSE_PCT * 100)
        send_telegram(
            f"💰 PARTIAL TAKE-PROFIT — {symbol} {partial_tp_hit['side']}\n\n"
            f"1R hədəfinə çatıldı, pozisiyanın ~{pct}%-i bağlandı (konseptual).\n"
            f"Entry: {partial_tp_hit['entry']:.4f}\n"
            f"Partial Exit: {price:.4f}\n"
            f"Qalan {100-pct}% trailing stop ilə davam edir."
        )

    if trade_snapshot is None:
        return

    # Bundan sonrakı hər şey lock XARİCİNDƏ - şəbəkə/DB çağırışları lock-u
    # gərək saxlamasın, əks halda digər thread-lər bloklanar
    save_trade(trade_snapshot)

    emoji = "✅" if trade_snapshot["status"] == "WIN" else "❌"
    message = f"""
{emoji} TRADE BAĞLANDI — {trade_snapshot["status"]}

{trade_snapshot["symbol"]} {trade_snapshot["side"]}
Entry: {trade_snapshot["entry"]:.4f}
Exit (trailing stop): {trade_snapshot["exit_price"]:.4f}

RESULT: {trade_snapshot["status"]}
"""
    send_telegram(message)

    stats = get_statistics()
    send_telegram(
        f"📊 STATİSTİKA\nTotal: {stats['total']} | "
        f"WIN: {stats['wins']} | LOSS: {stats['losses']} | "
        f"Win Rate: {stats['win_rate']}%"
    )

    if should_alert_cooldown:
        send_telegram(
            f"⏸️ {MAX_CONSECUTIVE_LOSSES} ardıcıl itkidən sonra bot "
            f"{COOLDOWN_HOURS_AFTER_LOSSES} saat dayandırılır."
        )


# ============================================================
# POLLING WORKERS
# ============================================================

def process_pending_signals(pending_signals):
    """
    BTC (SCORE_GROUP_SYMBOLS-ə daxil deyil) hər zaman müstəqil açılır.
    ETH və SOL eyni dövrdə (eyni pass-da) breakout versə, yalnız ən yüksək
    balı olan (MIN_SIGNAL_SCORE həddini keçən) açılır - digəri ötürülür.
    """
    if not pending_signals:
        return

    # Qrupa daxil olmayan simvollar (BTC) - filtr olmadan açılır
    for symbol, signal in pending_signals.items():
        if symbol not in SCORE_GROUP_SYMBOLS:
            open_trade(signal)

    group_signals = {s: sig for s, sig in pending_signals.items() if s in SCORE_GROUP_SYMBOLS}
    if not group_signals:
        return

    best_symbol, best_signal = max(group_signals.items(), key=lambda kv: kv[1].get("score", 0))

    for symbol, signal in group_signals.items():
        score = signal.get("score", 0)
        if symbol == best_symbol:
            if score >= MIN_SIGNAL_SCORE:
                open_trade(signal)
            else:
                print(f"⏸️ {symbol} ən yüksək bal idi ({score}/100) amma minimum həddi ({MIN_SIGNAL_SCORE}) keçmədi.")
        else:
            print(f"⏭️ {symbol} siqnalı ötürüldü — {best_symbol} daha yüksək bal aldı.")
            send_telegram(
                f"⏭️ {symbol} breakout siqnalı var idi (bal: {score}/100), "
                f"lakin {best_symbol} daha yüksək bal aldığı üçün (bal: {best_signal.get('score', 0)}/100) "
                f"yalnız {best_symbol} açıldı."
            )


def candle_worker():
    last_seen_time = {s: None for s in SYMBOLS}

    while True:
        pending_signals = {}

        for symbol in SYMBOLS:
            trade_candles = fetch_klines(symbol, TRADE_TF)
            trend_candles = fetch_klines(symbol, TREND_TF)

            if trade_candles:
                with lock:
                    candles_trade_tf[symbol] = trade_candles[-MAX_CANDLES:]
            if trend_candles:
                with lock:
                    candles_trend_tf[symbol] = trend_candles[-MAX_CANDLES:]

            if trade_candles:
                closed_time = trade_candles[-2]["time"] if len(trade_candles) >= 2 else None
                if closed_time and closed_time != last_seen_time[symbol]:
                    last_seen_time[symbol] = closed_time
                    signal = check_for_signal(symbol)
                    if signal:
                        pending_signals[symbol] = signal

            time.sleep(1)

        process_pending_signals(pending_signals)

        time.sleep(CANDLE_POLL_SECONDS)


def price_worker():
    while True:
        with lock:
            symbols_to_check = list(active_trades.keys())

        for symbol in symbols_to_check:
            price = fetch_price(symbol)
            if price is not None:
                update_trailing_stops(symbol, price)

        time.sleep(PRICE_POLL_SECONDS)


# ============================================================
# STARTUP
# ============================================================

def startup():
    global _startup_done
    with _startup_lock:
        if _startup_done:
            return
        if not check_single_instance():
            return
        _startup_done = True

    print("🚀 TREND BREAKOUT BOT BAŞLAYIR...")

    threading.Thread(target=candle_worker, daemon=True).start()
    threading.Thread(target=price_worker, daemon=True).start()

    if USE_POLLING:
        threading.Thread(target=telegram_polling_worker, daemon=True).start()

    if SEND_STARTUP_MESSAGE:
        limit_str = "Limitsiz (Test)" if MAX_TRADES_PER_DAY >= 9999 else str(MAX_TRADES_PER_DAY)
        send_telegram(
            "🚀 TREND BREAKOUT BOT AKTİVDİR!\n\n"
            f"📡 {', '.join(SYMBOLS)} izlənilir.\n"
            f"📊 Donchian({DONCHIAN_PERIOD}) + EMA{EMA_TREND_PERIOD}({TREND_TF}dəq) + Chandelier Exit\n"
            f"⚖️ Günlük Max Trade: {limit_str}\n"
            "💾 Nəticələr SQLite-də saxlanılır.\n\n"
            "💬 Bot əmrləri üçün Telegram-da /help yazın."
        )


# ============================================================
# ROUTES & WEBHOOK
# ============================================================

@app.route("/")
def home():
    stats = get_statistics()
    with lock:
        active_count = len(active_trades)
        cooldown_snapshot = cooldown_until
    return jsonify({
        "status": "online",
        "mode": "SIGNAL-ONLY (real sifariş yoxdur)",
        "symbols": SYMBOLS,
        "trade_tf": TRADE_TF,
        "trend_tf": TREND_TF,
        "active_trades": active_count,
        "statistics": stats,
        "cooldown_until": cooldown_snapshot.isoformat() if cooldown_snapshot else None,
    })


@app.route("/health")
def health():
    return "OK", 200


@app.route("/stats")
def stats_route():
    return jsonify(get_statistics())


@app.route("/active")
def active_route():
    with lock:
        return jsonify(list(active_trades.values()))


@app.route("/telegram-webhook", methods=["POST"])
def telegram_webhook():
    data = request.get_json()
    if data:
        process_telegram_update(data)
    return jsonify({"status": "ok"}), 200


# ============================================================
# MAIN
# ============================================================

startup()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, use_reloader=False)
