"""
TREND BREAKOUT BOT (OKX + MULTI-TIMEFRAME, TELEGRAM READY)
===========================================================
DƏYİŞİKLİKLƏR (bu versiyada, Bybit -> OKX keçidi):
1. BYBIT ƏVƏZİNƏ OKX API: Bybit-in bəzi bölgələrdən (o cümlədən Render.com
   datacenter-lərinin bir hissəsindən) blok olunması səbəbindən exchange
   OKX-ə dəyişdirildi (blok problemi olmur).
2. ÇOXLU TIMEFRAME - MÜSTƏQİL: 5dəq, 15dəq, 1saat, 4saat timeframe-lərinin
   HƏR BİRİ tam MÜSTƏQİL izlənilir və öz Donchian+EMA+ADX+Volume siqnalını
   verir. Yəni eyni simvolda eyni anda fərqli TF-lərdən fərqli (hətta əks)
   siqnallar gələ bilər - bunlar ayrı-ayrı "trade" kimi izlənilir
   (məsələn BTCUSDT|15m və BTCUSDT|4H eyni vaxtda aktiv ola bilər).
3. Hər (simvol, timeframe) cütü üçün ayrıca EMA200 trend filtri öz TF-i
   üzərində hesablanır (əvvəlki versiyada ayrı "trend TF" var idi, indi
   hər TF öz-özünün trend filtridir).
4. MTF (multi-timeframe cross-check) filtri götürüldü, çünki artıq hər TF
   müstəqildir - əvəzinə hər TF öz ADX/Volume/ATR filtrini tətbiq edir.
5. Telegram mesajlarında və /active əmrində timeframe də göstərilir.

QALAN HİSSƏLƏR (thread-safety, PID lock, partial TP, trailing stop,
cooldown, DB) əvvəlki versiya ilə eynidir.
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
# CONFIG
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID_HERE")

USE_POLLING = os.getenv("USE_POLLING", "True").lower() == "true"
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "True").lower() == "true"

# ------------------------------------------------------------
# OKX ENDPOINTS
# ------------------------------------------------------------
OKX_BASE = "https://www.okx.com"
OKX_KLINE_URL = f"{OKX_BASE}/api/v5/market/candles"
OKX_TICKER_URL = f"{OKX_BASE}/api/v5/market/ticker"

# İstəsəniz proxy (adətən OKX-ə ehtiyac qalmır, amma dəstək saxlanılıb)
OKX_PROXY_URL = os.getenv("OKX_PROXY_URL", "").strip()
OKX_PROXIES = {"http": OKX_PROXY_URL, "https": OKX_PROXY_URL} if OKX_PROXY_URL else None

# OKX perpetual swap instId formatı: "BTC-USDT-SWAP"
SYMBOLS = ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]

# Hər TF müstəqil izlənilir. OKX 'bar' formatı: 5m, 15m, 1H, 4H
# NOT: 1H/4H üçün 'utc' variantı (1Hutc/4Hutc) UTC saat sərhədlərinə
# görə bağlanır (standart 1H/4H Honq-Konq vaxtına görəd
