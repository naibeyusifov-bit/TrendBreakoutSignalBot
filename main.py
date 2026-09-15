"""
TREND BREAKOUT BOT (WebSocket + Instant Execution + 1:2 R:R)
===========================================================
- WebSocket: Milli-saniyəlik canlı ticker qiymət axını
- 1:2 Risk/Reward: Avtomatik Sl və TP (2R) hesablanması
- Breakeven: +1R mənfəətə çatdıqda Stop-Loss girişə çəkilir
"""

import os
import time
import json
import sqlite3
import threading
from datetime import datetime, timezone

import requests
import websocket  # pip install websocket-client
from flask import Flask, jsonify, request


# ============================================================
# CONFIG
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Bybit API Keys (Real Əməliyyat Açmaq İstəyirsinizsə Doldurun)
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")

BYBIT_KLINE_URL = "https://api.bybit.com/v5/market/kline"
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

TRADE_TF = "15"
TREND_TF = "60"
MAX_CANDLES = 400

DONCHIAN_PERIOD = 20
ATR_PERIOD = 14
EMA_TREND_PERIOD = 200
CHANDELIER_ATR_MULT = 3.0

# RISK & REWARD (1:2 RRR)
RRR_TARGET = 2.0  # 1:2 Risk / Reward
MOVE_SL_TO_BE_AT_1R = True

CANDLE_REFRESH_SECONDS = 60

ACCOUNT_BALANCE_USDT = float(os.getenv("ACCOUNT_BALANCE_USDT", "1000"))
RISK_PER_TRADE_PCT = 0.01  # 1% Risk ($1R)
MAX_TRADES_PER_DAY = 3
MAX_CONSECUTIVE_LOSSES = 3
COOLDOWN_HOURS_AFTER_LOSSES = 24

DB_FILE = "trend_breakout.db"


# ============================================================
# GLOBAL STATE & FLASK
# ============================================================

app = Flask(__name__)
lock = threading.Lock()

candles_trade_tf = {s: [] for s in SYMBOLS}
candles_trend_tf = {s: [] for s in SYMBOLS}
live_prices = {s: None for s in SYMBOLS}

active_trades = {}
last_signal_candle = {s: None for s in SYMBOLS}

daily_trade_count = 0
daily_count_date = None
consecutive_losses = 0
cooldown_until = None


# ============================================================
# DATABASE & TELEGRAM
# ============================================================

def init_db():
    conn = sqlite3.connect(DB_FILE, timeout=10)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL, side TEXT NOT NULL, entry REAL NOT NULL,
            initial_stop REAL NOT NULL, tp_price REAL NOT NULL, exit_price REAL,
            status TEXT NOT NULL, position_size_usdt REAL, created_at REAL NOT NULL, closed_at REAL
        )
    """)
    conn.commit()
    conn.close()

init_db()


def send_telegram(message, parse_mode="Markdown"):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print("❌ Telegram Error:", e)


# ============================================================
# INDICATORS & KLINE DATA
# ============================================================

def fetch_klines(symbol, interval, limit=MAX_CANDLES):
    params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
    try:
        r = requests.get(BYBIT_KLINE_URL, params=params, timeout=10)
        data = r.json()
        if data.get("retCode") != 0:
            return []
        rows = data["result"]["list"]
        rows.reverse()
        return [{
            "time": int(row[0]), "open": float(row[1]), "high": float(row[2]),
            "low": float(row[3]), "close": float(row[4]), "volume": float(row[5])
        } for row in rows]
    except Exception as e:
        print(f"❌ Kline Fetch Error ({symbol}):", e)
        return []


def ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    res = sum(values[:period]) / period
    for v in values[period:]:
        res = (v - res) * k + res
    return res


def atr(candles, period=ATR_PERIOD):
    if len(candles) < period + 1:
        return None
    trs = [max(candles[i]["high"] - candles[i]["low"], 
               abs(candles[i]["high"] - candles[i - 1]["close"]), 
               abs(candles[i]["low"] - candles[i - 1]["close"])) for i in range(1, len(candles))]
    return sum(trs[-period:]) / period if len(trs) >= period else None


def donchian_channel(candles, period):
    if len(candles) < period + 1:
        return None, None
    closed = candles[-(period + 1):-1]
    return max(c["high"] for c in closed), min(c["low"] for c in closed)


# ============================================================
# TRADE ENGINE & RISK LOGIC
# ============================================================

def evaluate_breakout(symbol, price):
    """WebSocket vasitəsilə gələn anlıq qiymətlə milisaniyəlik yoxlama"""
    with lock:
        trade_data = list(candles_trade_tf[symbol])
        trend_data = list(candles_trend_tf[symbol])
        if symbol in active_trades:
            return

    if len(trade_data) < DONCHIAN_PERIOD + 10 or len(trend_data) < EMA_TREND_PERIOD + 10:
        return

    # EMA 200 Trend Filter (1h)
    trend_closes = [c["close"] for c in trend_data[:-1]]
    trend_ema = ema(trend_closes, EMA_TREND_PERIOD)
    if not trend_ema:
        return

    bullish = trend_closes[-1] > trend_ema
    bearish = trend_closes[-1] < trend_ema

    d_high, d_low = donchian_channel(trade_data, DONCHIAN_PERIOD)
    c_atr = atr(trade_data[:-1])
    if not d_high or not c_atr:
        return

    # BREAKOUT CHECK
    if bullish and price > d_high:
        execute_trade(symbol, "LONG", price, c_atr, trade_data[-1]["time"])
    elif bearish and price < d_low:
        execute_trade(symbol, "SHORT", price, c_atr, trade_data[-1]["time"])


def execute_trade(symbol, side, entry, c_atr, candle_time):
    with lock:
        if symbol in active_trades or last_signal_candle[symbol] == candle_time:
            return

        risk_amount = ACCOUNT_BALANCE_USDT * RISK_PER_TRADE_PCT
        stop_dist = c_atr * CHANDELIER_ATR_MULT

        if side == "LONG":
            stop_loss = entry - stop_dist
            tp_price = entry + (stop_dist * RRR_TARGET)
            one_r_price = entry + stop_dist
        else:
            stop_loss = entry + stop_dist
            tp_price = entry - (stop_dist * RRR_TARGET)
            one_r_price = entry - stop_dist

        qty = round((risk_amount / stop_dist) * entry, 2)
        last_signal_candle[symbol] = candle_time

        trade = {
            "symbol": symbol, "side": side, "entry": entry, "initial_stop": stop_loss,
            "current_stop": stop_loss, "tp_price": tp_price, "one_r_price": one_r_price,
            "one_r_hit": False, "position_size_usdt": qty, "created_at": time.time()
        }
        active_trades[symbol] = trade

    msg = f"""
⚡ *INSTANT BREAKOUT DETECTED (WebSocket)*

{("🟢" if side=="LONG" else "🔴")} *{symbol} {side}*
📍 **Entry:** `{entry:.4f}`
🛑 **Stop-Loss:** `{stop_loss:.4f}`
🎯 **Take Profit (1:2 RR):** `{tp_price:.4f}`
🛡️ **Breakeven Target (+1R):** `{one_r_price:.4f}`
💰 **Size:** ~`{qty:.2f}` USDT
"""
    send_telegram(msg)


def manage_active_trades(symbol, price):
    """Hər bir gələn qiymət axınında TP və SL yoxlanılır"""
    with lock:
        trade = active_trades.get(symbol)
        if not trade:
            return

    side = trade["side"]
    result = None

    if side == "LONG":
        # +1R Vurulduqda Breakeven-ə çəkilir
        if not trade["one_r_hit"] and price >= trade["one_r_price"]:
            trade["one_r_hit"] = True
            trade["current_stop"] = trade["entry"]
            send_telegram(f"🛡️ *{symbol} LONG* — +1R Vuruldu! Stop-Loss Giriş Nöqtəsinə (Breakeven) gətirildi.")

        # Take Profit (2R)
        if price >= trade["tp_price"]:
            result = "WIN (2R TP)"
        # Stop-Loss
        elif price <= trade["current_stop"]:
            result = "WIN (Breakeven)" if trade["current_stop"] >= trade["entry"] else "LOSS"

    else:  # SHORT
        if not trade["one_r_hit"] and price <= trade["one_r_price"]:
            trade["one_r_hit"] = True
            trade["current_stop"] = trade["entry"]
            send_telegram(f"🛡️ *{symbol} SHORT* — +1R Vuruldu! Stop-Loss Giriş Nöqtəsinə (Breakeven) gətirildi.")

        if price <= trade["tp_price"]:
            result = "WIN (2R TP)"
        elif price >= trade["current_stop"]:
            result = "WIN (Breakeven)" if trade["current_stop"] <= trade["entry"] else "LOSS"

    if result:
        with lock:
            active_trades.pop(symbol, None)

        emoji = "✅" if "WIN" in result else "❌"
        send_telegram(f"{emoji} *TRADE CLOSED — {result}*\n{symbol} {side}\nExit Price: `{price:.4f}`")


# ============================================================
# WEBSOCKET ENGINE
# ============================================================

def on_ws_message(ws, message):
    data = json.loads(message)
    if "data" in data and "symbol" in data:
        symbol = data["symbol"]
        price = float(data["data"]["lastPrice"])
        live_prices[symbol] = price

        # Gecikməsiz qiymət axını ilə analiz
        evaluate_breakout(symbol, price)
        manage_active_trades(symbol, price)


def on_ws_open(ws):
    print("🌐 Bybit WebSocket qoşuldu!")
    # Ticker axınına abunə olunur
    args = [f"tickers.{s}" for s in SYMBOLS]
    ws.send(json.dumps({"op": "subscribe", "args": args}))


def run_websocket():
    while True:
        try:
            ws = websocket.WebSocketApp(
                BYBIT_WS_URL,
                on_open=on_ws_open,
                on_message=on_ws_message,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("❌ WebSocket Connection Error, reconnecting...", e)
            time.sleep(3)


def candle_updater():
    """Arxa fonda TF şamlarını yeniləyir"""
    while True:
        for s in SYMBOLS:
            t_candles = fetch_klines(s, TRADE_TF)
            tr_candles = fetch_klines(s, TREND_TF)
            with lock:
                if t_candles: candles_trade_tf[s] = t_candles
                if tr_candles: candles_trend_tf[s] = tr_candles
        time.sleep(CANDLE_REFRESH_SECONDS)


# ============================================================
# STARTUP & ROUTES
# ============================================================

def start_threads():
    threading.Thread(target=candle_updater, daemon=True).start()
    threading.Thread(target=run_websocket, daemon=True).start()
    send_telegram("🚀 *WebSocket Breakout Bot İşə Düşdü!* Milli-saniyəlik izləmə aktivdir.")

start_threads()

@app.route("/")
def index():
    return jsonify({"status": "online", "mode": "WebSocket Realtime", "prices": live_prices})

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
