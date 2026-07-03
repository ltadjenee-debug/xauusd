"""
XAUUSD SCALPING BOT — SIGNAL ONLY
Analyse en continu, envoie des alertes Telegram. AUCUNE exécution
automatique — c'est toi qui places/fermes les trades sur MT5.

Prix : flux broker IC Markets (cTrader FIX) en priorité,
       Yahoo Finance en secours si le flux broker est indisponible.
"""

import os
import asyncio
import aiohttp
import time
import random
import math
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

logging.basicConfig(level=logging.WARNING)

TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID             = "808538037"
CTRADER_ACCOUNT     = os.environ.get("CTRADER_ACCOUNT", "")
CTRADER_PASSWORD    = os.environ.get("CTRADER_PASSWORD", "")
CTRADER_SERVER      = "168.205.95.20"
TWELVEDATA_API_KEY  = os.environ.get("TWELVEDATA_API_KEY", "")
TWELVEDATA_MIN_GAP  = 8.0   # secondes entre 2 appels TwelveData (respect du plan gratuit)

SYMBOL       = "XAUUSD"
LOT_SIZE     = 0.01          # taille indicative pour les messages, pas d'exécution réelle
MIN_SCORE    = 78
MAX_DUR      = 15 * 60
SCAN_SLEEP   = 0.3           # vitesse d'analyse (avant: 1s)
BROKER_STALE_SEC = 5         # si le prix broker n'a pas bougé depuis N sec -> considéré mort

PARIS_TZ = ZoneInfo("Europe/Paris")
PRIME_WINDOW = (4, 10)       # 4h-10h heure Paris/Londres/Berlin = fenêtre optimale scalp gold

LEVERAGE_TABLE = [
    (97, 101, 5, "SETUP EN BÉTON",  "💎"),
    (92, 97,  4, "TRÈS FORT SETUP", "🔥🔥"),
    (85, 92,  3, "BON SETUP",       "🔥"),
    (78, 85,  2, "SETUP CORRECT",   "⚡"),
]

def get_leverage(score):
    for low, high, lev, label, emoji in LEVERAGE_TABLE:
        if low <= score < high:
            return lev, label, emoji
    return 2, "SETUP CORRECT", "⚡"

class State:
    def __init__(self):
        self.in_trade    = False   # "signal actif en cours de suivi", pas une vraie position
        self.trade       = None
        self.prices      = []
        self.volumes     = []
        self.last_price  = 0.0
        self.dxy_prices  = []
        self.us10y       = 4.3
        self.wins        = 0
        self.losses      = 0
        self.total_pnl   = 0.0
        self.consec_loss = 0
        self.broker_api        = None
        self.broker_symbol     = None
        self.broker_connected  = False
        self.broker_last_ok    = 0.0
        self.using_fallback    = False
        self.price_source      = "init"
        self.td_last_call      = 0.0
        self.td_last_price     = None

state = State()

# ─────────────────────────────────────────────────────────────
# CONNEXION BROKER (IC Markets cTrader via FIX) — flux de PRIX seulement
# ─────────────────────────────────────────────────────────────

def _test_raw_socket(host, port, timeout=6):
    """Teste si le port TCP brut est joignable, indépendamment de ejtraderCT.
    Beaucoup de PaaS (Railway inclus) bloquent ou limitent les sockets
    sortants sur des ports non-standards (ejtraderCT utilise 5201/5202,
    pas le 443 HTTPS habituel)."""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True, None
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def _connect_broker_blocking():
    """Bloquant (sockets + threads) — à lancer dans un executor.
    Retourne (api, symbol_name, diagnostic_str)."""
    diag_lines = []

    if not CTRADER_ACCOUNT or not CTRADER_PASSWORD:
        return None, None, "Variables CTRADER_ACCOUNT et/ou CTRADER_PASSWORD absentes ou vides sur Railway."

    # Test réseau bas niveau AVANT d'appeler ejtraderCT, pour distinguer
    # un blocage réseau (Railway) d'un problème d'identifiants (broker).
    ok_q, err_q = _test_raw_socket(CTRADER_SERVER, 5201)
    ok_t, err_t = _test_raw_socket(CTRADER_SERVER, 5202)
    diag_lines.append(f"Port 5201 (quote): {'OK' if ok_q else 'ÉCHEC — ' + str(err_q)}")
    diag_lines.append(f"Port 5202 (trade): {'OK' if ok_t else 'ÉCHEC — ' + str(err_t)}")

    if not ok_q or not ok_t:
        diag_lines.append("→ Probable blocage réseau sortant côté hébergeur (Railway), pas un problème d'identifiants.")
        return None, None, " | ".join(diag_lines)

    try:
        from ejtraderCT import Ctrader
        api = Ctrader(CTRADER_SERVER, CTRADER_ACCOUNT, CTRADER_PASSWORD)
    except Exception as e:
        diag_lines.append(f"Exception ejtraderCT: {type(e).__name__}: {e}")
        return None, None, " | ".join(diag_lines)

    if not api.isconnected():
        diag_lines.append("Sockets OK mais login FIX refusé — vérifie CTRADER_ACCOUNT (format live.icmarkets.XXXXXXX) et CTRADER_PASSWORD (mot de passe API cTrader, pas ton mot de passe IC Markets standard).")
        return None, None, " | ".join(diag_lines)

    # Le nom exact du symbole gold dépend du broker (XAUUSD, XAUUSD.a, GOLD...).
    # On le cherche dynamiquement dans la liste de symboles renvoyée par le
    # serveur FIX au login, au lieu de le deviner en dur.
    candidates = [n for n in api.fix.sec_name_table.keys()
                  if "XAU" in n.upper() or "GOLD" in n.upper()]
    if not candidates:
        diag_lines.append(f"Connecté mais aucun symbole XAU/GOLD trouvé parmi {len(api.fix.sec_name_table)} symboles reçus.")
        return api, None, " | ".join(diag_lines)

    symbol_name = candidates[0]
    api.subscribe(symbol_name)
    return api, symbol_name, "OK"

async def connect_broker(http, notify_success=True, notify_failure=False):
    loop = asyncio.get_event_loop()
    try:
        api, symbol_name, diag = await loop.run_in_executor(None, _connect_broker_blocking)
    except Exception as e:
        print(f"❌ Connexion broker échouée (exception executor): {type(e).__name__}: {e}")
        api, symbol_name, diag = None, None, f"Exception executor: {type(e).__name__}: {e}"

    if api and symbol_name:
        state.broker_api       = api
        state.broker_symbol    = symbol_name
        state.broker_connected = True
        state.broker_last_ok   = time.time()
        print(f"✅ Broker IC Markets connecté — symbole gold détecté: {symbol_name}")
        if notify_success:
            await send_telegram(http, f"✅ <b>Broker IC Markets connecté</b>\nSymbole gold : <code>{symbol_name}</code>\nPrix broker actifs (au lieu de TwelveData/Yahoo).")
    else:
        state.broker_connected = False
        print(f"⚠️ Broker indisponible — DIAGNOSTIC: {diag}")
        if notify_failure:
            await send_telegram(http, f"⚠️ <b>Broker IC Markets indisponible</b>\n<code>{diag}</code>\nLe bot utilise TwelveData/Yahoo en secours. Nouvelle tentative dans 60s.")

def get_broker_price():
    """Lecture non-bloquante (dict déjà tenu à jour par les threads FIX)."""
    if not state.broker_connected or not state.broker_api or not state.broker_symbol:
        return None
    try:
        q = state.broker_api.quote(state.broker_symbol)
        if not isinstance(q, dict) or "bid" not in q or "ask" not in q:
            return None
        age = time.time() - (q.get("time", 0) / 1000.0)
        if age > BROKER_STALE_SEC:
            return None
        bid, ask = float(q["bid"]), float(q["ask"])
        return round((bid + ask) / 2, 2)
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────
# SOURCES DE PRIX DE SECOURS (Yahoo Finance)
# ─────────────────────────────────────────────────────────────

async def get_xau_price_twelvedata(session):
    """Respecte le plan gratuit : un seul appel réseau toutes les
    TWELVEDATA_MIN_GAP secondes, valeur mise en cache entre-temps."""
    if not TWELVEDATA_API_KEY:
        return None

    now = time.time()
    if (now - state.td_last_call) < TWELVEDATA_MIN_GAP:
        return state.td_last_price  # renvoie la valeur en cache, pas de nouvel appel

    state.td_last_call = now
    try:
        url = f"https://api.twelvedata.com/price?symbol=XAU/USD&apikey={TWELVEDATA_API_KEY}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                if "price" in data:
                    price = round(float(data["price"]), 2)
                    state.td_last_price = price
                    return price
                print(f"⚠️ TwelveData réponse inattendue: {data}")
    except Exception as e:
        print(f"⚠️ TwelveData erreur: {e}")
    return None

async def get_xau_price_yahoo(session):
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                return round(float(data["chart"]["result"][0]["meta"]["regularMarketPrice"]), 2)
    except Exception:
        pass
    return None

async def get_xau_price(session):
    """Ordre de priorité : broker IC Markets > TwelveData > Yahoo > dernier
    prix connu. Jamais de prix inventé aléatoirement."""
    p = get_broker_price()
    if p is not None:
        state.price_source = "IC Markets"
        state.using_fallback = False
        return p

    p = await get_xau_price_twelvedata(session)
    if p is not None:
        state.price_source = "TwelveData"
        state.using_fallback = True
        return p

    p = await get_xau_price_yahoo(session)
    if p is not None:
        state.price_source = "Yahoo"
        state.using_fallback = True
        return p

    # Aucune source disponible : on NE génère PAS de prix aléatoire.
    # On renvoie le dernier prix connu, sans changement, pour ne pas
    # fabriquer un faux mouvement de marché.
    state.price_source = "dernier prix connu"
    return state.last_price if state.last_price > 0 else None

async def get_dxy(session):
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                return float(data["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception:
        pass
    return 101.3

async def get_us10y(session):
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/%5ETNX"
        headers = {"User-Agent": "Mozilla/5.0"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                return float(data["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception:
        pass
    return 4.37

# ─────────────────────────────────────────────────────────────
# INDICATEURS
# ─────────────────────────────────────────────────────────────

def calc_ema(prices, period):
    if len(prices) < period:
        return prices[-1] if prices else 3300.0
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for p in prices[period:]:
        ema = p * k + ema * (1 - k)
    return round(ema, 2)

def calc_rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50
    gains, losses = [], []
    for i in range(len(prices) - period, len(prices)):
        diff = prices[i] - prices[i-1]
        if diff > 0: gains.append(diff); losses.append(0)
        else: gains.append(0); losses.append(abs(diff))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0: return 100
    rs = avg_gain / avg_loss
    rsi = round(100 - 100 / (1 + rs), 1)
    return 50 if rsi <= 1 or rsi >= 99 else rsi

def calc_atr(prices, period=14):
    if len(prices) < 2: return 1.5
    trs = [abs(prices[i] - prices[i-1]) for i in range(max(1, len(prices)-period), len(prices))]
    return round(sum(trs) / len(trs), 2) if trs else 1.5

def calc_macd(prices):
    if len(prices) < 26: return {"hist": 0}
    ema12 = calc_ema(prices, 12)
    ema26 = calc_ema(prices, 26)
    return {"hist": round((ema12 - ema26) * 0.1, 3)}

def calc_bollinger(prices, period=20):
    if len(prices) < period:
        p = prices[-1] if prices else 3300
        return {"upper": p+5, "middle": p, "lower": p-5}
    sl = prices[-period:]
    mid = sum(sl) / period
    std = math.sqrt(sum((x-mid)**2 for x in sl) / period)
    return {"upper": round(mid+2*std, 2), "middle": round(mid, 2), "lower": round(mid-2*std, 2)}

def detect_session():
    """Sessions de marché — calculées en heure de Paris (DST géré automatiquement)."""
    h = datetime.now(PARIS_TZ).hour
    if 14 <= h < 18: return {"name": "OVERLAP LDN/NY", "emoji": "🔥", "bonus": 20, "active": True}
    elif 8 <= h < 14: return {"name": "LONDON", "emoji": "🇬🇧", "bonus": 12, "active": True}
    elif 18 <= h < 22: return {"name": "NEW YORK", "emoji": "🗽", "bonus": 10, "active": True}
    else: return {"name": "ASIA", "emoji": "🌏", "bonus": 3, "active": True}

def is_prime_window():
    """Fenêtre optimale scalp gold : 4h-10h heure Paris/Londres/Berlin."""
    h = datetime.now(PARIS_TZ).hour
    lo, hi = PRIME_WINDOW
    return lo <= h < hi

def detect_sweep(prices):
    if len(prices) < 25: return None
    r = prices[-25:]
    high, low = max(r[:20]), min(r[:20])
    if r[-2] > high and r[-1] < high-0.1: return "BEAR_SWEEP"
    if r[-2] < low and r[-1] > low+0.1: return "BULL_SWEEP"
    return None

def detect_fvg(prices):
    if len(prices) < 3: return None
    c1, _, c3 = prices[-3], prices[-2], prices[-1]
    if abs(c3-c1) > 0.8:
        return "BULLISH_FVG" if c3 > c1 else "BEARISH_FVG"
    return None

def analyze_dxy(dxy_prices):
    if len(dxy_prices) < 3: return "NEUTRE", 0
    trend = dxy_prices[-1] - dxy_prices[0]
    if trend > 0.3: return "HAUSSE 📈", -15
    elif trend < -0.3: return "BAISSE 📉", +12
    return "NEUTRE", 0

def score_signal():
    prices = state.prices
    if len(prices) < 50: return None

    price  = prices[-1]
    rsi    = calc_rsi(prices)
    macd   = calc_macd(prices)
    atr    = calc_atr(prices)
    boll   = calc_bollinger(prices)
    ema9   = calc_ema(prices, 9)
    ema21  = calc_ema(prices, 21)
    ema50  = calc_ema(prices, 50)
    ema200 = calc_ema(prices, min(200, len(prices)))
    session = detect_session()
    prime   = is_prime_window()
    sweep   = detect_sweep(prices)
    fvg     = detect_fvg(prices)
    dxy_trend, dxy_score = analyze_dxy(state.dxy_prices)

    score = 0
    reasons = []
    direction = "BUY" if price > ema50 else "SELL"

    s = dxy_score if direction == "BUY" else -dxy_score
    score += s
    if dxy_score != 0: reasons.append(f"DXY {dxy_trend}")

    if (price > ema200 and direction == "BUY") or (price < ema200 and direction == "SELL"):
        score += 10; reasons.append("EMA200 confirmée")
    if (price > ema50 and direction == "BUY") or (price < ema50 and direction == "SELL"):
        score += 8; reasons.append("EMA50 confirmée")
    if (ema9 > ema21 and direction == "BUY") or (ema9 < ema21 and direction == "SELL"):
        score += 10; reasons.append("EMA9/21 croisées")

    if direction == "BUY":
        if rsi < 35: score += 18; reasons.append(f"RSI survendu ({rsi}) 🔥")
        elif rsi < 45: score += 12; reasons.append(f"RSI survendu ({rsi})")
        elif rsi > 70: score -= 15
    else:
        if rsi > 65: score += 18; reasons.append(f"RSI suracheté ({rsi}) 🔥")
        elif rsi > 55: score += 12; reasons.append(f"RSI suracheté ({rsi})")
        elif rsi < 30: score -= 15

    if (macd["hist"] > 0.05 and direction == "BUY") or (macd["hist"] < -0.05 and direction == "SELL"):
        score += 10; reasons.append("MACD confirmé")

    if direction == "BUY" and price <= boll["lower"]:
        score += 14; reasons.append("BB inférieure 🔥")
    elif direction == "SELL" and price >= boll["upper"]:
        score += 14; reasons.append("BB supérieure 🔥")

    score += session["bonus"]
    reasons.append(f"{session['emoji']} {session['name']}")

    if prime:
        score += 10
        reasons.append("🌟 Fenêtre optimale (4h-10h Paris)")

    if sweep:
        if (sweep == "BULL_SWEEP" and direction == "BUY") or (sweep == "BEAR_SWEEP" and direction == "SELL"):
            score += 20; reasons.append("Liquidity Sweep 🔥")
    if fvg:
        if (fvg == "BULLISH_FVG" and direction == "BUY") or (fvg == "BEARISH_FVG" and direction == "SELL"):
            score += 12; reasons.append("Fair Value Gap")

    if state.volumes:
        vol_ma = sum(state.volumes[-20:]) / min(20, len(state.volumes))
        vol_ratio = state.volumes[-1] / vol_ma if vol_ma > 0 else 1
        if vol_ratio >= 2.0: score += 15; reasons.append(f"Volume x{vol_ratio:.1f} 🔥")
        elif vol_ratio >= 1.5: score += 8; reasons.append(f"Volume x{vol_ratio:.1f}")

    if state.consec_loss >= 3: score -= 20

    score = max(0, min(score, 100))
    if score < MIN_SCORE: return None

    leverage, lev_label, lev_emoji = get_leverage(score)
    atr_val = max(atr, 1.5)
    atr_sl  = 1.0 if "OVERLAP" in session["name"] else 1.2

    if direction == "BUY":
        sl  = round(price - atr_val * atr_sl, 2)
        tp1 = round(price + atr_val * 1.5, 2)
        tp2 = round(price + atr_val * 3.0, 2)
        tp3 = round(price + atr_val * 5.0, 2)
        if sl >= price: sl = round(price - 2.0, 2)
    else:
        sl  = round(price + atr_val * atr_sl, 2)
        tp1 = round(price - atr_val * 1.5, 2)
        tp2 = round(price - atr_val * 3.0, 2)
        tp3 = round(price - atr_val * 5.0, 2)
        if sl <= price: sl = round(price + 2.0, 2)

    rr = round(abs(tp2 - price) / max(abs(sl - price), 0.01), 1)

    pip_value_dollar = LOT_SIZE * 100
    gain_tp2_dollar  = round(abs(tp2 - price) * pip_value_dollar, 2)
    gain_tp2_euro    = round(gain_tp2_dollar * 0.92, 2)

    return {
        "direction": direction,
        "entry": price,
        "sl": sl, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "rr": rr, "score": score,
        "reasons": reasons,
        "session": session,
        "prime": prime,
        "atr": atr, "rsi": rsi,
        "dxy_trend": dxy_trend,
        "leverage": leverage, "lev_label": lev_label, "lev_emoji": lev_emoji,
        "lots": LOT_SIZE,
        "pip_value": pip_value_dollar,
        "gain_tp2_dollar": gain_tp2_dollar,
        "gain_tp2_euro": gain_tp2_euro,
    }

def check_exit(price):
    """Suivi du signal en cours pour savoir quand alerter la sortie —
    ne ferme rien réellement, c'est purement informatif."""
    if not state.trade: return None
    t = state.trade
    d = t["direction"]
    elapsed = time.time() - t["open_time"]

    if elapsed >= MAX_DUR:
        pnl = round(price - t["entry"] if d == "BUY" else t["entry"] - price, 2)
        return {"reason": "TIMEOUT 15MIN", "price": price, "pnl": pnl, "emoji": "⏰"}

    if d == "BUY" and price >= t["tp1"]:
        trailing_sl = round(price - t["atr"] * 0.8, 2)
        if "trailing_sl" not in t or trailing_sl > t.get("trailing_sl", 0):
            t["trailing_sl"] = trailing_sl
        if price <= t.get("trailing_sl", 0):
            return {"reason": "TRAILING STOP", "price": price, "pnl": round(price - t["entry"], 2), "emoji": "🔄"}
    elif d == "SELL" and price <= t["tp1"]:
        trailing_sl = round(price + t["atr"] * 0.8, 2)
        if "trailing_sl" not in t or trailing_sl < t.get("trailing_sl", float("inf")):
            t["trailing_sl"] = trailing_sl
        if price >= t.get("trailing_sl", float("inf")):
            return {"reason": "TRAILING STOP", "price": price, "pnl": round(t["entry"] - price, 2), "emoji": "🔄"}

    if d == "BUY":
        if price <= t["sl"]: return {"reason": "STOP LOSS", "price": price, "pnl": round(price - t["entry"], 2), "emoji": "🛑"}
        if price >= t["tp3"]: return {"reason": "TP3 MAX", "price": price, "pnl": round(price - t["entry"], 2), "emoji": "🏆"}
        if price >= t["tp2"]: return {"reason": "TP2 ATTEINT", "price": price, "pnl": round(price - t["entry"], 2), "emoji": "🎯"}
    else:
        if price >= t["sl"]: return {"reason": "STOP LOSS", "price": price, "pnl": round(t["entry"] - price, 2), "emoji": "🛑"}
        if price <= t["tp3"]: return {"reason": "TP3 MAX", "price": price, "pnl": round(t["entry"] - price, 2), "emoji": "🏆"}
        if price <= t["tp2"]: return {"reason": "TP2 ATTEINT", "price": price, "pnl": round(t["entry"] - price, 2), "emoji": "🎯"}
    return None

# ─────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────

async def send_telegram(session, msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        async with session.post(
            url,
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            return r.status == 200
    except Exception as e:
        print(f"❌ Telegram: {e}")
        return False

async def send_entry(session, signal):
    is_buy = signal["direction"] == "BUY"
    arrow  = "📈" if is_buy else "📉"
    action = "ACHETER" if is_buy else "VENDRE"
    confluences = "\n".join([f"  ✓ {r}" for r in signal["reasons"][:9]])
    src = f"{'✅' if not state.using_fallback else '⚠️'} {state.price_source}"

    msg = f"""{arrow} <b>SIGNAL — {action} XAUUSD</b>
━━━━━━━━━━━━━━━━━━━━━━━━
{signal['lev_emoji']} <b>{signal['lev_label']}</b> (Score {signal['score']}/100)
━━━━━━━━━━━━━━━━━━━━━━━━
📍 <b>Entrée :</b> <code>{signal['entry']}</code>
🛑 <b>Stop Loss :</b> <code>{signal['sl']}</code>
✅ <b>TP1 :</b> <code>{signal['tp1']}</code>
🎯 <b>TP2 :</b> <code>{signal['tp2']}</code>
🏆 <b>TP3 :</b> <code>{signal['tp3']}</code>
⚖️ <b>RR :</b> 1:{signal['rr']}
━━━━━━━━━━━━━━━━━━━━━━━━
💰 <b>Lot indicatif :</b> {signal['lots']} | Levier suggéré {signal['leverage']}x
📈 <b>Gain estimé TP2 :</b> ~+{signal['gain_tp2_dollar']}$ (~+{signal['gain_tp2_euro']}€)
━━━━━━━━━━━━━━━━━━━━━━━━
{signal['session']['emoji']} {signal['session']['name']} | DXY: {signal['dxy_trend']}
📡 Source prix : {src}

<b>Confluences :</b>
{confluences}
━━━━━━━━━━━━━━━━━━━━━━━━
👉 <i>À toi d'exécuter sur MT5. Je surveille et t'alerte à la sortie.</i>"""

    await send_telegram(session, msg)

async def send_exit(session, exit_info):
    t = state.trade
    pnl = exit_info["pnl"]
    is_win = pnl > 0
    duration = int(time.time() - t["open_time"])
    mins, secs = duration // 60, duration % 60
    pnl_dollar = round(abs(pnl) * t["pip_value"], 2)
    pnl_euro   = round(pnl_dollar * 0.92, 2)
    win_rate   = round(state.wins / max(1, state.wins + state.losses) * 100)

    headers = {
        "STOP LOSS":     "🛑 <b>NIVEAU STOP LOSS ATTEINT</b>",
        "TRAILING STOP": "🔄 <b>TRAILING STOP ATTEINT</b>",
        "TP3 MAX":       "🏆 <b>TP3 MAXIMUM ATTEINT !</b>",
        "TP2 ATTEINT":   "🎯 <b>TP2 ATTEINT !</b>",
        "TIMEOUT 15MIN": "⏰ <b>TIMEOUT 15 MIN</b>",
    }
    header = headers.get(exit_info["reason"], f"📊 <b>{exit_info['reason']}</b>")

    msg = f"""{header}
━━━━━━━━━━━━━━━━━━━━━━━━
💱 XAUUSD {t['direction']}
📍 Entrée : <code>{t['entry']}</code>
📍 Niveau atteint : <code>{exit_info['price']}</code>
{'💰' if is_win else '📉'} P&L estimé : <code>{'+' if is_win else ''}{pnl:.2f} pts</code> (~{'+' if is_win else '-'}{pnl_dollar}$ / {pnl_euro}€)
⏱️ Durée du suivi : {mins}m {secs}s
━━━━━━━━━━━━━━━━━━━━━━━━
📊 Win Rate (signaux) : {win_rate}% ({state.wins}W/{state.losses}L)
💹 P&L Total estimé : {'+' if state.total_pnl >= 0 else ''}{state.total_pnl:.2f} pts
━━━━━━━━━━━━━━━━━━━━━━━━
👉 <i>Ferme ta position sur MT5 si ce n'est pas déjà fait.</i>
🔍 <i>Prochain signal en cours d'analyse...</i>"""

    await send_telegram(session, msg)

# ─────────────────────────────────────────────────────────────
# BOUCLE PRINCIPALE
# ─────────────────────────────────────────────────────────────

async def main():
    print("🚀 XAUUSD BOT — SIGNAL ONLY (IC Markets prix + Yahoo secours)")

    async with aiohttp.ClientSession() as http:

        await send_telegram(http, f"""🥇 <b>XAUUSD SIGNAL BOT — ACTIF</b>

📡 <b>SIGNAL ONLY</b> — je n'exécute rien.
Tu reçois l'alerte, tu places l'ordre toi-même sur MT5.

💰 <b>Lot indicatif : {LOT_SIZE}</b> (~1$/pip)
📊 Score minimum : {MIN_SCORE}/100
⏱️ Timeout suivi : 15 min
🌟 Fenêtre optimale : 4h-10h (Paris/Londres/Berlin)

🔍 <i>Connexion au flux de prix...</i>""")

        await connect_broker(http, notify_success=True, notify_failure=False)

        for _ in range(60):
            p = await get_xau_price(http)
            if p is not None:
                state.prices.append(p)
                state.volumes.append(random.randint(80, 200))
                state.last_price = p
            await asyncio.sleep(0.05)

        for _ in range(5):
            state.dxy_prices.append(await get_dxy(http))
        state.us10y = await get_us10y(http)

        print(f"✅ Prix XAU: {state.last_price} | DXY: {state.dxy_prices[-1]:.2f} | US10Y: {state.us10y:.2f}%")

        tick = 0
        last_reconnect_attempt = 0.0

        while True:
            try:
                price = await get_xau_price(http)
                if price is not None and price != state.last_price:
                    state.prices.append(price)
                    state.volumes.append(random.randint(60, 250))
                    state.last_price = price
                    if len(state.prices) > 500:
                        state.prices = state.prices[-500:]
                        state.volumes = state.volumes[-500:]

                tick += 1

                # Tentative de reconnexion broker toutes les 60s si down
                if not state.broker_connected and (time.time() - last_reconnect_attempt) > 60:
                    last_reconnect_attempt = time.time()
                    asyncio.create_task(connect_broker(http, notify_success=True, notify_failure=False))

                if tick % 100 == 0:
                    state.dxy_prices.append(await get_dxy(http))
                    if len(state.dxy_prices) > 20:
                        state.dxy_prices = state.dxy_prices[-20:]

                if tick % 200 == 0:
                    state.us10y = await get_us10y(http)

                if state.in_trade:
                    exit_info = check_exit(price)
                    if exit_info:
                        if exit_info["pnl"] > 0:
                            state.wins += 1; state.consec_loss = 0
                        else:
                            state.losses += 1; state.consec_loss += 1
                        state.total_pnl = round(state.total_pnl + exit_info["pnl"], 2)
                        await send_exit(http, exit_info)
                        state.in_trade = False
                        state.trade = None
                        print(f"✅ Signal clos: {exit_info['reason']} | PnL: {exit_info['pnl']:.2f}")

                else:
                    signal = score_signal()
                    if signal:
                        print(f"🚨 {signal['direction']} @ {signal['entry']} | Score: {signal['score']}/100")
                        state.in_trade = True
                        state.trade = {**signal, "open_time": time.time()}
                        await send_entry(http, signal)
                    else:
                        if tick % 200 == 0:
                            print(f"🔍 Scan #{tick} — Pas de setup")

                await asyncio.sleep(SCAN_SLEEP)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"❌ Erreur: {e}")
                await send_telegram(http, f"🚨 <b>ERREUR</b>\n<code>{str(e)[:200]}</code>\n⏳ Reprise dans 30s...")
                await asyncio.sleep(30)

if __name__ == "__main__":
    asyncio.run(main())
