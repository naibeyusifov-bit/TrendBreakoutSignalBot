"""
TREND BREAKOUT BOT
===================
SMC/liquidity-sweep əvəzinə obyektiv, backtest edilə bilən qaydalar:

  - TREND FILTRİ : yüksək timeframe-də (default 1h) EMA200-ə görə istiqamət
  - GİRİŞ        : Donchian Channel breakout (son N bağlanmış şamın ən
                   yüksək/aşağı səviyyəsinin qırılması) — subyektiv "BOS"
                   yerinə, ədədi və birmənalı qayda
  - ÇIXIŞ        : Chandelier Exit — ATR-based trailing stop. Sabit RR
                   yerinə qazancın "qaçmasına" imkan verir, itkini isə
                   sərt məhdudlaşdırır
  - RİSK         : Fixed-fractional pozisiya ölçüsü, günlük trade limiti,
                   ardıcıl itkidən sonra soyuma (cooldown) — bunlar əlavə
                   yox, botun nüvəsidir

QEYD: Bu bot REAL SİFARİŞ VERMİR — yalnız siqnal göndərir və virtual
(kağız) trade-i qiymət hərəkətinə görə izləyib nəticəni (WIN/LOSS)
Telegram-a bildirir + SQLite-ə yazır.

Bu, maliyyə məsləhəti deyil. Strategiyanın özü bura yazılmazdan əvvəl
tarixi datada backtest edilməyib.
"""

import os
import time
import sqlite3
import threading
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify


# ============================================================
# CONFIG
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
BYBIT_TICKER_URL = "https://api.bybit.com/v5/market/tickers"

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
MAX_TRADES_PER_DAY = 3
MAX_CONSECUTIVE_LOSSES = 3
COOLDOWN_HOURS_AFTER_LOSSES = 24

DB_FILE = "trend_breakout.db"


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# GLOBAL STATE
# ============================================================

lock = threading.Lock()

candles_trade_tf = {s: [] for s in SYMBOLS}
candles_trend_tf = {s: [] for s in SYMBOLS}

active_trades = {}
last_signal_candle = {s: None for s in SYMBOLS}

daily_trade_count = 0
daily_count_date = None

consecutive_losses = 0
cooldown_until = None

_startup_done = False
_startup_lock = threading.Lock()


# ============================================================
# DATABASE
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_FILE)
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
        conn = sqlite3.connect(DB_FILE)
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
    init_db()
    conn = sqlite3.connect(DB_FILE)
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


init_db()


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Telegram token/chat_id yoxdur.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
        ok = r.json().get("ok")
        if not ok:
            print("❌ Telegram xətası:", r.json())
        return ok
    except Exception as e:
        print("❌ Telegram bağlantı xətası:", e)
        return False


# ============================================================
# BYBIT REST
# ============================================================

def fetch_klines(symbol, interval, limit=MAX_CANDLES):
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    try:
        r = requests.get(BYBIT_KLINE_URL, params=params, timeout=15)
        data = r.json()
        if data.get("retCode") != 0:
            print(f"❌ Bybit kline xətası {symbol}:", data)
            return []
        rows = data["result"]["list"]
        rows.reverse()
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
        return []


def fetch_price(symbol):
    params = {"category": "linear", "symbol": symbol}
    try:
        r = requests.get(BYBIT_TICKER_URL, params=params, timeout=10)
        data = r.json()
        if data.get("retCode") != 0:
            return None
        lst = data["result"]["list"]
        if not lst:
            return None
        return float(lst[0]["lastPrice"])
    except Exception as e:
        print(f"❌ {symbol} qiymət sorğu xətası:", e)
        return None


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


# ============================================================
# RİSK İDARƏETMƏSİ
# ============================================================

def reset_daily_counter_if_needed():
    global daily_trade_count, daily_count_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if daily_count_date != today:
        daily_count_date = today
        daily_trade_count = 0


def risk_checks_pass():
    reset_daily_counter_if_needed()
    if cooldown_until is not None:
        if datetime.now(timezone.utc) < cooldown_until:
            return False, f"Cooldown aktivdir, {cooldown_until.isoformat()} tarixinə qədər"
    if daily_trade_count >= MAX_TRADES_PER_DAY:
        return False, "Günlük trade limiti dolub"
    return True, ""


def register_trade_opened():
    global daily_trade_count
    daily_trade_count += 1


def register_trade_result(result):
    global consecutive_losses, cooldown_until
    if result == "LOSS":
        consecutive_losses += 1
        if consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            cooldown_until = datetime.now(timezone.utc).timestamp() + COOLDOWN_HOURS_AFTER_LOSSES * 3600
            cooldown_until = datetime.fromtimestamp(cooldown_until, tz=timezone.utc)
            send_telegram(
                f"⏸️ {MAX_CONSECUTIVE_LOSSES} ardıcıl itkidən sonra bot "
                f"{COOLDOWN_HOURS_AFTER_LOSSES} saat dayandırılır."
            )
    else:
        consecutive_losses = 0


def calc_position_size(entry, stop):
    risk_amount = ACCOUNT_BALANCE_USDT * RISK_PER_TRADE_PCT
    stop_distance = abs(entry - stop)
    if stop_distance <= 0:
        return 0
    qty = risk_amount / stop_distance
    position_value_usdt = qty * entry
    return round(position_value_usdt, 2)


# ============================================================
# SİQNAL MƏNTİQİ
# ============================================================

def check_for_signal(symbol):
    with lock:
        trade_data = list(candles_trade_tf[symbol])
        trend_data = list(candles_trend_tf[symbol])

    if len(trade_data) < DONCHIAN_PERIOD + 5 or len(trend_data) < EMA_TREND_PERIOD + 5:
        return None

    current = trade_data[-1]

    trend_closes = [c["close"] for c in trend_data]
    trend_ema = ema(trend_closes[-(EMA_TREND_PERIOD + 50):], EMA_TREND_PERIOD)
    if trend_ema is None:
        return None

    trend_price = trend_data[-1]["close"]
    bullish_regime = trend_price > trend_ema
    bearish_regime = trend_price < trend_ema

    donchian_high, donchian_low = donchian_channel(trade_data, DONCHIAN_PERIOD)
    if donchian_high is None:
        return None

    current_atr = atr(trade_data)
    if current_atr is None or current_atr <= 0:
        return None

    breakout_long = current["close"] > donchian_high
    breakout_short = current["close"] < donchian_low

    if bullish_regime and breakout_long:
        entry = current["close"]
        initial_stop = entry - current_atr * CHANDELIER_ATR_MULT
        if initial_stop >= entry:
            return None
        return {
            "symbol": symbol, "side": "LONG", "entry": entry,
            "initial_stop": initial_stop, "candle_time": current["time"],
            "atr": current_atr,
        }

    if bearish_regime and breakout_short:
        entry = current["close"]
        initial_stop = entry + current_atr * CHANDELIER_ATR_MULT
        if initial_stop <= entry:
            return None
        return {
            "symbol": symbol, "side": "SHORT", "entry": entry,
            "initial_stop": initial_stop, "candle_time": current["time"],
            "atr": current_atr,
        }

    return None


def open_trade(signal):
    symbol = signal["symbol"]

    with lock:
        if symbol in active_trades:
            return
        if last_signal_candle[symbol] == signal["candle_time"]:
            return

        ok, reason = risk_checks_pass()
        if not ok:
            print(f"⏸️ {symbol} siqnalı rədd edildi: {reason}")
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
        }
        active_trades[symbol] = trade
        register_trade_opened()

    emoji = "🟢" if trade["side"] == "LONG" else "🔴"
    message = f"""
🚨 TREND BREAKOUT SİQNALI

{emoji} {symbol} {trade["side"]}

Entry: {trade["entry"]:.4f}
İlkin Stop (trailing başlanğıc): {trade["initial_stop"]:.4f}
Tövsiyə olunan pozisiya: ~{trade["position_size_usdt"]:.2f} USDT
(balansın {RISK_PER_TRADE_PCT*100:.0f}%-i risk əsasında)

Səbəb: Donchian({DONCHIAN_PERIOD}) breakout + EMA{EMA_TREND_PERIOD}({TREND_TF}dəq) trend

⏳ Status: ACTIVE — stop qiymətin xeyrinə hərəkət edəcək (trailing)
"""
    print(message)
    send_telegram(message)


def update_trailing_stops(symbol, price):
    with lock:
        trade = active_trades.get(symbol)
    if not trade:
        return

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
        return

    trade["status"] = result
    trade["exit_price"] = price
    trade["closed_at"] = time.time()

    with lock:
        active_trades.pop(symbol, None)

    save_trade(trade)
    register_trade_result(result)

    emoji = "✅" if result == "WIN" else "❌"
    message = f"""
{emoji} TRADE BAĞLANDI — {result}

{trade["symbol"]} {trade["side"]}
Entry: {trade["entry"]:.4f}
Exit (trailing stop): {trade["exit_price"]:.4f}

RESULT: {result}
"""
    send_telegram(message)

    stats = get_statistics()
    send_telegram(
        f"📊 STATİSTİKA\nTotal: {stats['total']} | "
        f"WIN: {stats['wins']} | LOSS: {stats['losses']} | "
        f"Win Rate: {stats['win_rate']}%"
    )


# ============================================================
# POLLING WORKERS
# ============================================================

def candle_worker():
    last_seen_time = {s: None for s in SYMBOLS}

    while True:
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
                newest = trade_candles[-1]
                if newest["time"] != last_seen_time[symbol]:
                    last_seen_time[symbol] = newest["time"]
                    signal = check_for_signal(symbol)
                    if signal:
                        open_trade(signal)

            time.sleep(1)

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
        _startup_done = True

    print("🚀 TREND BREAKOUT BOT BAŞLAYIR...")

    threading.Thread(target=candle_worker, daemon=True).start()
    threading.Thread(target=price_worker, daemon=True).start()

    send_telegram(
        "🚀 TREND BREAKOUT BOT AKTİVDİR!\n\n"
        f"📡 {', '.join(SYMBOLS)} izlənilir.\n"
        f"📊 Donchian({DONCHIAN_PERIOD}) + EMA{EMA_TREND_PERIOD}({TREND_TF}dəq) + "
        f"Chandelier Exit trailing stop\n"
        f"⚖️ Risk: trade başına balansın {RISK_PER_TRADE_PCT*100:.0f}%-i, "
        f"günlük max {MAX_TRADES_PER_DAY} trade\n"
        "💾 Nəticələr SQLite-də saxlanılır.\n\n"
        "⚠️ Bu bot REAL SİFARİŞ VERMİR — yalnız siqnal göndərir."
    )


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():
    stats = get_statistics()
    with lock:
        active_count = len(active_trades)
    return jsonify({
        "status": "online",
        "mode": "SIGNAL-ONLY (real sifariş yoxdur)",
        "symbols": SYMBOLS,
        "trade_tf": TRADE_TF,
        "trend_tf": TREND_TF,
        "active_trades": active_count,
        "statistics": stats,
        "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
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


# ============================================================
# MAIN
# ============================================================

startup()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
