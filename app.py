"""
L/S DECISION ENGINE v9 - H1 REJIM SISTEMI
==========================================
Tek timeframe (H1). EMA365 rejim. 5 kosullu boga/ayi puanlama.
EMA7/30 kesisimi cikis sinyali. Pozisyon farkindaligi. Gunduz/gece modu.
"""

import http.server
import socketserver
import urllib.request
import urllib.parse
import urllib.error
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

PORT = int(os.environ.get("PORT", 8765))
HOST = "0.0.0.0"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# ============= SUPABASE =============
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
SUPABASE_ENABLED = bool(SUPABASE_URL and SUPABASE_KEY)

# ============= ESIKLER (BTC icin) =============
THRESHOLDS = {
    "wr_bull":   3.0,    # W/R >= +3 boga kosulu
    "wr_bear":  -3.0,    # W/R <= -3 ayi kosulu
    "retail_trend_min": 0.15,  # retail "degisiyor" sayilmasi icin min % (son 2 mum)
    "oi_min":    0.3,    # OI "artiyor/dusuyor" min %
    "price_min": 0.1,    # fiyat "artiyor/dusuyor" min %
    "cvd_buy":   55.0,   # CVD alim baskisi
    "cvd_sell":  45.0,   # CVD satim baskisi
    "ema_flat_band": 0.5,  # fiyat EMA365'in ±%0.5'inde ise YATAY
}

# Karar dereceleri (5 kosul uzerinden)
SCORE_STRONG = 5   # 5/5 KESIN
SCORE_OPEN   = 4   # 4/5 AC
SCORE_GAMBLE = 3   # 3/5 GAMBLE


import time as _time

# ====== Binance ban takibi (global) ======
# 418 alindiginda, banin bitis zamanini kaydet. O zamana kadar Binance'e hic istek atma.
_binance_ban_until = 0  # epoch saniye

def _binance_banned():
    return _time.time() < _binance_ban_until

def _set_binance_ban(seconds):
    global _binance_ban_until
    until = _time.time() + seconds
    if until > _binance_ban_until:
        _binance_ban_until = until
        sys.stderr.write(f"Binance ban: {seconds}s (until {int(until)})\n")


def http_get(url, timeout=10, retries=2):
    """418/429 durumunda Retry-After'a uyar, banli ise Binance'e hic gitmez."""
    is_binance = "binance" in url
    if is_binance and _binance_banned():
        raise RuntimeError("Binance gecici banli (yerel takip), istek atlandi")

    last_err = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        })
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code in (418, 429):
                # Retry-After header'ini oku
                retry_after = 0
                try:
                    ra = e.headers.get("Retry-After")
                    if ra:
                        retry_after = int(ra)
                except Exception:
                    pass
                if is_binance:
                    # Ban suresi: header varsa o kadar, yoksa 418 icin 5dk, 429 icin 60s
                    ban_secs = retry_after if retry_after > 0 else (300 if e.code == 418 else 60)
                    _set_binance_ban(ban_secs)
                # Banlandiysa tekrar denemenin anlami yok, hemen cik
                raise
            raise
        except Exception as e:
            last_err = e
            if attempt < retries:
                _time.sleep(0.5)
                continue
            raise
    if last_err:
        raise last_err


# ====== Genel cache (rate limit azaltma) ======
_cache = {}  # url -> (timestamp, data)
CACHE_TTL = {
    "klines": 180,       # 3 dakika
    "globalLong": 180,   # 3 dakika
    "topLong": 180,
    "openInterest": 180,
    "taker": 180,
    "premium": 180,
    "default": 120,
}

def _ttl_for(url):
    if "klines" in url: return CACHE_TTL["klines"]
    if "globalLongShort" in url: return CACHE_TTL["globalLong"]
    if "topLongShort" in url: return CACHE_TTL["topLong"]
    if "openInterest" in url: return CACHE_TTL["openInterest"]
    if "takerlongshort" in url: return CACHE_TTL["taker"]
    if "premiumIndex" in url: return CACHE_TTL["premium"]
    return CACHE_TTL["default"]

def http_get_cached(url, timeout=10):
    """TTL'e gore cache'li GET. Ban sirasinda eski cache'i (bayatsa bile) dondurur."""
    now = _time.time()
    ttl = _ttl_for(url)
    cached = _cache.get(url)
    if cached and (now - cached[0] < ttl):
        return cached[1]
    # Cache bayat veya yok - cekmeyi dene
    try:
        data = http_get(url, timeout=timeout)
        _cache[url] = (now, data)
        return data
    except Exception:
        # Cekemedik (ban vs) - elimizde eski cache varsa onu dondur (bayat da olsa)
        if cached:
            return cached[1]
        raise


def safe(fn, default=None):
    try:
        return fn()
    except Exception:
        return default


# ======================================================================
# SUPABASE HELPERS
# ======================================================================

def supabase_request(method, path, body=None):
    if not SUPABASE_ENABLED:
        return None
    url = f"{SUPABASE_URL}/rest/v1/{path.lstrip('/')}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else []
    except urllib.error.HTTPError as e:
        sys.stderr.write(f"Supabase {method} {path}: HTTP {e.code}\n")
        return None
    except Exception as e:
        sys.stderr.write(f"Supabase {method} {path}: {e}\n")
        return None


def save_signal_if_meaningful(symbol, decision, price):
    """Anlamli sinyalleri kaydet: acilis (kesin/guclu/gamble) + kapatmalar."""
    if not SUPABASE_ENABLED:
        return False
    saveable = {
        "LONG_OPEN", "SHORT_OPEN", "GAMBLE_LONG", "GAMBLE_SHORT",
        "CLOSE", "CLOSE_TP", "CLOSE_SL",
    }
    if decision.get("action") not in saveable:
        return False

    # Histerezis: ayni aksiyon 45dk, ayni aile 10dk
    try:
        latest = supabase_request("GET", f"signals?symbol=eq.{symbol}&order=ts.desc&limit=1")
        if latest and len(latest) > 0:
            last = latest[0]
            from datetime import datetime, timezone, timedelta
            try:
                last_ts = datetime.fromisoformat(last.get("ts", "").replace("Z", "+00:00"))
                elapsed = datetime.now(timezone.utc) - last_ts
                la = last.get("action", "")
                na = decision.get("action", "")
                if la == na and elapsed < timedelta(minutes=45):
                    return False
                opens = {"LONG_OPEN", "SHORT_OPEN", "GAMBLE_LONG", "GAMBLE_SHORT"}
                closes = {"CLOSE", "CLOSE_TP", "CLOSE_SL"}
                if ((la in opens and na in opens) or (la in closes and na in closes)) and elapsed < timedelta(minutes=10):
                    return False
            except Exception:
                pass
    except Exception:
        pass

    c = decision.get("conditions", {})
    body = {
        "symbol": symbol,
        "action": decision.get("action"),
        "action_label": decision.get("actionLabel"),
        "regime": decision.get("regime"),
        "bull_score": decision.get("bullScore", 0),
        "bear_score": decision.get("bearScore", 0),
        "bull_ema": c.get("bull_ema", 0),
        "bull_wr": c.get("bull_wr", 0),
        "bull_retail": c.get("bull_retail", 0),
        "bull_oi": c.get("bull_oi", 0),
        "bull_cvd": c.get("bull_cvd", 0),
        "bear_ema": c.get("bear_ema", 0),
        "bear_wr": c.get("bear_wr", 0),
        "bear_retail": c.get("bear_retail", 0),
        "bear_oi": c.get("bear_oi", 0),
        "bear_cvd": c.get("bear_cvd", 0),
        "ema_cross": decision.get("emaCross"),
        "price": price,
        "reasons": " | ".join((decision.get("reasons") or [])[:6])[:1000],
    }
    result = supabase_request("POST", "signals", body)
    return result is not None


def get_signal_history(symbol, limit=50):
    if not SUPABASE_ENABLED:
        return []
    try:
        return supabase_request("GET", f"signals?symbol=eq.{symbol}&order=ts.desc&limit={limit}") or []
    except Exception:
        return []


# ======================================================================
# EMA HESAPLAMA
# ======================================================================

def calc_ema(closes, period):
    if not closes or len(closes) < period:
        return [None] * len(closes)
    ema = [None] * len(closes)
    sma = sum(closes[:period]) / period
    ema[period - 1] = sma
    mult = 2 / (period + 1)
    for i in range(period, len(closes)):
        ema[i] = (closes[i] - ema[i-1]) * mult + ema[i-1]
    return ema


# ======================================================================
# VERI CEKME - H1 (tek timeframe)
# ======================================================================

def fetch_h1_data(sym):
    """BTC H1 verisi - SADECE KAPANMIS MUMLAR (canli mum atlanir, stabilite icin)."""
    p = "1h"

    # NOT: Binance dizilerinde son eleman aktif/canli mumdur, dalgalanir.
    # Bu yuzden limit=4 cekip son elemani (canli) atliyoruz.
    # Kalan kapanmis mumlardan: now = son kapanmis [-1], 2ago = [-3]

    # 1) Global L/S account ratio
    g = http_get_cached(f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={sym}&period={p}&limit=4")
    if not g or len(g) < 2:
        raise RuntimeError("global ratio yok")
    gc = g[:-1] if len(g) >= 4 else g  # canli mumu at
    global_long_now = float(gc[-1]["longAccount"]) * 100
    global_long_2ago = float(gc[-3]["longAccount"]) * 100 if len(gc) >= 3 else float(gc[0]["longAccount"]) * 100

    # 2) Top trader position ratio (whale)
    t = safe(lambda: http_get_cached(f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={sym}&period={p}&limit=4"), [])
    tc = t[:-1] if t and len(t) >= 4 else t
    whale_now = float(tc[-1]["longAccount"]) * 100 if tc else None
    whale_2ago = float(tc[-3]["longAccount"]) * 100 if tc and len(tc) >= 3 else (float(tc[0]["longAccount"]) * 100 if tc else None)

    # True retail (cikarma)
    retail_now = retail_2ago = None
    if whale_now is not None:
        retail_now = max(0, min(100, (global_long_now - whale_now * 0.2) / 0.8))
    if whale_2ago is not None:
        retail_2ago = max(0, min(100, (global_long_2ago - whale_2ago * 0.2) / 0.8))

    # 3) OI - kapanmis mum
    oi = safe(lambda: http_get_cached(f"https://fapi.binance.com/futures/data/openInterestHist?symbol={sym}&period={p}&limit=4"), [])
    oic = oi[:-1] if oi and len(oi) >= 4 else oi
    oi_now = float(oic[-1].get("sumOpenInterestValue") or 0) if oic else None
    oi_2ago = float(oic[-3].get("sumOpenInterestValue") or 0) if oic and len(oic) >= 3 else None
    oi_change = None
    if oi_now and oi_2ago and oi_2ago > 0:
        oi_change = (oi_now - oi_2ago) / oi_2ago * 100

    # 4) Fiyat - kline (EMA icin 400 mum). Son eleman CANLI mum - EMA icin tut ama
    #    fiyat trendi icin kapanmis kullan.
    kl = http_get_cached(f"https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval={p}&limit=400")
    closes_all = [float(k[4]) for k in kl]  # canli mum dahil (EMA hesabi icin)
    closes_closed = closes_all[:-1]  # canli mumu at (fiyat trendi + EMA365 rejim icin)
    price_now = closes_closed[-1]    # son KAPANMIS fiyat
    price_2ago = closes_closed[-3] if len(closes_closed) >= 3 else closes_closed[0]
    price_change = (price_now - price_2ago) / price_2ago * 100 if price_2ago > 0 else None

    # 5) CVD - taker buy/sell, kapanmis mum + 3 nokta agirlikli ortalama
    cvd = safe(lambda: http_get_cached(f"https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={sym}&period={p}&limit=5"), [])
    cvdc = cvd[:-1] if cvd and len(cvd) >= 5 else cvd  # canli mumu at
    taker_buy_now = taker_buy_2ago = None
    if cvdc:
        def buy_pct(point):
            b = float(point.get("buyVol") or 0); s = float(point.get("sellVol") or 0)
            return b / (b + s) * 100 if (b + s) > 0 else None
        pcts = [buy_pct(pt) for pt in cvdc if buy_pct(pt) is not None]
        if pcts:
            if len(pcts) >= 3:
                taker_buy_now = pcts[-1] * 0.5 + pcts[-2] * 0.3 + pcts[-3] * 0.2
                taker_buy_2ago = pcts[0]
            else:
                taker_buy_now = pcts[-1]
                taker_buy_2ago = pcts[0]

    # 6) Funding (sadece gosterim icin)
    funding = None
    try:
        pj = http_get_cached(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym}")
        if pj.get("lastFundingRate"):
            funding = float(pj["lastFundingRate"]) * 100
    except Exception:
        pass

    # 7) EMA hesaplari - KAPANMIS MUMLAR (rejim ve cross stabil olmali)
    ema365 = calc_ema(closes_closed, 365)
    ema7 = calc_ema(closes_closed, 7)
    ema30 = calc_ema(closes_closed, 30)

    # Fiyat EMA365 ustunde/altinda kac mum? (son 2 KAPANMIS mum)
    ema365_now = ema365[-1]
    above_ema365_2mum = below_ema365_2mum = False
    if ema365_now and ema365[-2]:
        above_ema365_2mum = closes_closed[-1] > ema365[-1] and closes_closed[-2] > ema365[-2]
        below_ema365_2mum = closes_closed[-1] < ema365[-1] and closes_closed[-2] < ema365[-2]

    # Fiyat EMA365'e uzakligi (yatay bant kontrolu)
    ema365_dist = None
    if ema365_now:
        ema365_dist = (price_now - ema365_now) / ema365_now * 100

    # EMA7/30 cross - DURUM BAZLI + 2 MUM KAPANIS TEYIDI
    # Mantik: kesisim ne zaman olursa olsun, EMA7 bir tarafta + son 2 kapanmis
    # mum o tarafta ise o yonde "onayli kesisim durumu" aktiftir.
    # crossUp = EMA7, EMA30 uzerinde 2 mum kapanis (yukari onayli)
    # crossDown = EMA7, EMA30 altinda 2 mum kapanis (asagi onayli)
    ema_cross = None        # anlik yon: BULL (7>30) / BEAR (7<30)
    ema_cross_up = False    # EMA7 ustte + 2 mum kapanis teyidi
    ema_cross_down = False  # EMA7 altta + 2 mum kapanis teyidi
    if ema7[-1] and ema30[-1]:
        ema_cross = "BULL" if ema7[-1] > ema30[-1] else "BEAR"
        # 2 mum kapanis teyidi: son 2 kapanmis mumda da ayni taraf
        if ema7[-2] and ema30[-2]:
            up_now = ema7[-1] > ema30[-1]
            up_prev = ema7[-2] > ema30[-2]
            if up_now and up_prev:
                ema_cross_up = True       # 2 mumdur EMA7 ustte = yukari onayli
            elif (not up_now) and (not up_prev):
                ema_cross_down = True     # 2 mumdur EMA7 altta = asagi onayli

    return {
        "ok": True,
        "priceNow": price_now,
        "priceChange": price_change,
        "globalLongNow": global_long_now,
        "whaleNow": whale_now,
        "whale2ago": whale_2ago,
        "retailNow": retail_now,
        "retail2ago": retail_2ago,
        "oiNow": oi_now,
        "oiChange": oi_change,
        "takerBuyNow": taker_buy_now,
        "takerBuy2ago": taker_buy_2ago,
        "funding": funding,
        "ema365": round(ema365_now, 2) if ema365_now else None,
        "ema7": round(ema7[-1], 2) if ema7[-1] else None,
        "ema30": round(ema30[-1], 2) if ema30[-1] else None,
        "ema365Dist": round(ema365_dist, 2) if ema365_dist is not None else None,
        "aboveEma365": above_ema365_2mum,
        "belowEma365": below_ema365_2mum,
        "emaCross": ema_cross,
        "emaCrossUp": ema_cross_up,
        "emaCrossDown": ema_cross_down,
    }


# ======================================================================
# KARAR MOTORU - 5 KOSULLU BOGA/AYI
# ======================================================================

def evaluate_conditions(d):
    """5 boga + 5 ayi kosulunu degerlendirir. Her biri 1/0."""
    c = {}

    # --- Ortak hesaplar ---
    wr_delta = None
    if d.get("whaleNow") is not None and d.get("retailNow") is not None:
        wr_delta = d["whaleNow"] - d["retailNow"]

    retail_trend = None  # pozitif = retail long artiyor
    if d.get("retailNow") is not None and d.get("retail2ago") is not None:
        retail_trend = d["retailNow"] - d["retail2ago"]

    oi_ch = d.get("oiChange")
    pr_ch = d.get("priceChange")
    cvd = d.get("takerBuyNow")

    tmin = THRESHOLDS["retail_trend_min"]
    oimin = THRESHOLDS["oi_min"]
    prmin = THRESHOLDS["price_min"]

    # ===== BOGA KOSULLARI =====
    # 1. Fiyat EMA365 ustu (2+ mum)
    c["bull_ema"] = 1 if d.get("aboveEma365") else 0
    # 2. W/R >= +3
    c["bull_wr"] = 1 if (wr_delta is not None and wr_delta >= THRESHOLDS["wr_bull"]) else 0
    # 3. Retail long dusuyor (son 2 mum)
    c["bull_retail"] = 1 if (retail_trend is not None and retail_trend <= -tmin) else 0
    # 4. OI artiyor + fiyat artiyor
    c["bull_oi"] = 1 if (oi_ch is not None and pr_ch is not None and oi_ch >= oimin and pr_ch >= prmin) else 0
    # 5. CVD artiyor (alim) + fiyat artiyor
    c["bull_cvd"] = 1 if (cvd is not None and pr_ch is not None and cvd >= THRESHOLDS["cvd_buy"] and pr_ch >= prmin) else 0

    # ===== AYI KOSULLARI =====
    # 1. Fiyat EMA365 alti (2+ mum)
    c["bear_ema"] = 1 if d.get("belowEma365") else 0
    # 2. W/R <= -3
    c["bear_wr"] = 1 if (wr_delta is not None and wr_delta <= THRESHOLDS["wr_bear"]) else 0
    # 3. Retail long artiyor (son 2 mum)
    c["bear_retail"] = 1 if (retail_trend is not None and retail_trend >= tmin) else 0
    # 4. OI yukseliyor + fiyat dusuyor
    c["bear_oi"] = 1 if (oi_ch is not None and pr_ch is not None and oi_ch >= oimin and pr_ch <= -prmin) else 0
    # 5. CVD dusuyor (satim) + fiyat dusuyor
    c["bear_cvd"] = 1 if (cvd is not None and pr_ch is not None and cvd <= THRESHOLDS["cvd_sell"] and pr_ch <= -prmin) else 0

    bull_score = c["bull_ema"] + c["bull_wr"] + c["bull_retail"] + c["bull_oi"] + c["bull_cvd"]
    bear_score = c["bear_ema"] + c["bear_wr"] + c["bear_retail"] + c["bear_oi"] + c["bear_cvd"]

    return c, bull_score, bear_score, wr_delta, retail_trend


def build_reasons(d, c, wr_delta, retail_trend):
    """Gerekceleri BOGA ve AYI olarak ayri listeler doner."""
    bull = []
    bear = []
    pr = d.get("priceChange")
    oi = d.get("oiChange")
    cvd = d.get("takerBuyNow")

    # EMA365 rejim -> hangi tarafa aitse oraya
    if d.get("aboveEma365"):
        bull.append(f"Fiyat EMA365 ustunde (+{d.get('ema365Dist')}%)")
    elif d.get("belowEma365"):
        bear.append(f"Fiyat EMA365 altinda ({d.get('ema365Dist')}%)")
    # belirsizse iki tarafa da yazma

    # W/R
    if wr_delta is not None:
        if wr_delta >= THRESHOLDS["wr_bull"]:
            bull.append(f"W/R +{wr_delta:.1f} (whale long baskin)")
        elif wr_delta <= THRESHOLDS["wr_bear"]:
            bear.append(f"W/R {wr_delta:.1f} (whale short baskin)")

    # Retail trend (boga: dusuyor, ayi: artiyor)
    if retail_trend is not None:
        if retail_trend <= -THRESHOLDS["retail_trend_min"]:
            bull.append(f"Retail long dusuyor ({retail_trend:.2f})")
        elif retail_trend >= THRESHOLDS["retail_trend_min"]:
            bear.append(f"Retail long artiyor (+{retail_trend:.2f})")

    # OI + fiyat
    if oi is not None and pr is not None:
        if oi >= THRESHOLDS["oi_min"] and pr >= THRESHOLDS["price_min"]:
            bull.append(f"OI +{oi:.2f}% + fiyat +{pr:.2f}% = saglam yukselis")
        elif oi >= THRESHOLDS["oi_min"] and pr <= -THRESHOLDS["price_min"]:
            bear.append(f"OI +{oi:.2f}% + fiyat {pr:.2f}% = saglam dusus")

    # CVD
    if cvd is not None:
        if cvd >= THRESHOLDS["cvd_buy"] and pr is not None and pr >= THRESHOLDS["price_min"]:
            bull.append(f"CVD %{cvd:.1f} alim + fiyat artiyor")
        elif cvd <= THRESHOLDS["cvd_sell"] and pr is not None and pr <= -THRESHOLDS["price_min"]:
            bear.append(f"CVD %{cvd:.1f} satim + fiyat dusuyor")

    # EMA7/30 kesisim durumu (2 mum onayli)
    if d.get("emaCrossUp"):
        bull.append("EMA7 EMA30 ustunde (2 mum) - yukari momentum")
    elif d.get("emaCrossDown"):
        bear.append("EMA7 EMA30 altinda (2 mum) - asagi momentum")

    return bull, bear


def decide(symbol, d, user_position, entry_price=None):
    """Ana karar fonksiyonu."""
    if not d or not d.get("ok"):
        return {"action": "FLAT", "actionLabel": "VERI YOK",
                "subtitle": "Veri alinamadi", "regime": None,
                "bullScore": 0, "bearScore": 0, "conditions": {},
                "reasons": [], "pnlPct": None, "emaCross": None}

    c, bull_score, bear_score, wr_delta, retail_trend = evaluate_conditions(d)
    bull_reasons, bear_reasons = build_reasons(d, c, wr_delta, retail_trend)

    # Rejim belirleme
    dist = d.get("ema365Dist")
    flat_band = THRESHOLDS["ema_flat_band"]
    if d.get("aboveEma365"):
        regime = "BOGA"
    elif d.get("belowEma365"):
        regime = "AYI"
    else:
        regime = "GECIS"

    # YATAY kontrolu: fiyat EMA365'e cok yakin VEYA iki skor da dusuk
    is_flat = False
    if dist is not None and abs(dist) < flat_band:
        is_flat = True
        regime = "YATAY"
    if bull_score <= 2 and bear_score <= 2:
        is_flat = True

    # PnL
    pnl_pct = None
    price = d.get("priceNow")
    if entry_price and price and entry_price > 0:
        if user_position == "long":
            pnl_pct = (price - entry_price) / entry_price * 100
        elif user_position == "short":
            pnl_pct = (entry_price - price) / entry_price * 100

    cross_up = d.get("emaCrossUp", False)      # EMA7 ustte 2 mum (yukari onayli)
    cross_down = d.get("emaCrossDown", False)  # EMA7 altta 2 mum (asagi onayli)

    # ============ POZISYON BAZLI KARAR ============
    if user_position == "long":
        action, label, subtitle = _decide_long(bull_score, bear_score, cross_up, cross_down, pnl_pct)
    elif user_position == "short":
        action, label, subtitle = _decide_short(bull_score, bear_score, cross_up, cross_down, pnl_pct)
    else:
        action, label, subtitle = _decide_flat(bull_score, bear_score, cross_up, cross_down, regime)

    return {
        "action": action, "actionLabel": label, "subtitle": subtitle,
        "regime": regime,
        "bullScore": bull_score, "bearScore": bear_score,
        "conditions": c,
        "wrDelta": round(wr_delta, 2) if wr_delta is not None else None,
        "retailTrend": round(retail_trend, 2) if retail_trend is not None else None,
        "bullReasons": bull_reasons,
        "bearReasons": bear_reasons,
        "reasons": bull_reasons + bear_reasons,
        "pnlPct": pnl_pct,
        "emaCross": d.get("emaCross"),
        "emaCrossUp": cross_up,
        "emaCrossDown": cross_down,
    }


def _decide_flat(bull, bear, cross_up, cross_down, regime):
    """FLAT iken giris: EMA7/30 kesisim ANA tetik, 5 kosul yon teyidi (>=3/5).
    - Asagi kesisim + ayi>=3 -> SHORT AC
    - Yukari kesisim + boga>=3 -> LONG AC
    - Kesisim + ters yon (diger taraf>=3) -> GAMBLE
    - Kesisim + zayif yon (ikisi de <3) -> BEKLE
    - Kesisim yok -> BEKLE
    """
    # Yukari kesisim (LONG yonu)
    if cross_up:
        if bull >= SCORE_GAMBLE:  # boga >=3 teyit
            if bull >= SCORE_STRONG:
                return "LONG_OPEN", "KESIN BOGA - LONG AC", f"Yukari kesisim + boga {bull}/5 (tam teyit)"
            return "LONG_OPEN", "LONG AC", f"Yukari kesisim + boga {bull}/5 teyit"
        if bear >= SCORE_GAMBLE:  # ters yon: ayi >=3
            return "GAMBLE_LONG", "GAMBLE - LONG (yon ters)", f"Yukari kesisim ama ayi {bear}/5 - celiski, manuel bak"
        # zayif yon
        return "FLAT", "BEKLE", f"Yukari kesisim ama yon zayif (boga {bull}/5) - teyit yok"
    # Asagi kesisim (SHORT yonu)
    if cross_down:
        if bear >= SCORE_GAMBLE:  # ayi >=3 teyit
            if bear >= SCORE_STRONG:
                return "SHORT_OPEN", "KESIN AYI - SHORT AC", f"Asagi kesisim + ayi {bear}/5 (tam teyit)"
            return "SHORT_OPEN", "SHORT AC", f"Asagi kesisim + ayi {bear}/5 teyit"
        if bull >= SCORE_GAMBLE:  # ters yon: boga >=3
            return "GAMBLE_SHORT", "GAMBLE - SHORT (yon ters)", f"Asagi kesisim ama boga {bull}/5 - celiski, manuel bak"
        return "FLAT", "BEKLE", f"Asagi kesisim ama yon zayif (ayi {bear}/5) - teyit yok"
    # Kesisim yok
    return "FLAT", "BEKLE", f"EMA7/30 kesisimi yok (yon: boga {bull}/5, ayi {bear}/5)"


def _decide_long(bull, bear, cross_up, cross_down, pnl_pct):
    """LONG pozisyonda. EMA7 EMA30'u ASAGI kesti (2 mum) -> KAPAT. Rejim fark etmez."""
    pnl_txt = f" (PnL {pnl_pct:+.2f}%)" if pnl_pct is not None else ""
    # Cikis: asagi kesisim onayli (EMA7 altta 2 mum)
    if cross_down:
        if pnl_pct is not None and pnl_pct > 0:
            return "CLOSE_TP", "KAR AL - LONG KAPAT", f"EMA7 EMA30'u asagi kesti (2 mum onay){pnl_txt}"
        else:
            return "CLOSE_SL", "ZARARI KES - LONG KAPAT", f"EMA7 EMA30'u asagi kesti (2 mum onay){pnl_txt}"
    # Devam: yukari kesisim hala aktif veya kesisim yok
    return "HOLD", "TUT", f"Long korunuyor - EMA7 ustte (boga {bull}/5){pnl_txt}"


def _decide_short(bull, bear, cross_up, cross_down, pnl_pct):
    """SHORT pozisyonda. EMA7 EMA30'u YUKARI kesti (2 mum) -> KAPAT. Rejim fark etmez."""
    pnl_txt = f" (PnL {pnl_pct:+.2f}%)" if pnl_pct is not None else ""
    # Cikis: yukari kesisim onayli (EMA7 ustte 2 mum)
    if cross_up:
        if pnl_pct is not None and pnl_pct > 0:
            return "CLOSE_TP", "KAR AL - SHORT KAPAT", f"EMA7 EMA30'u yukari kesti (2 mum onay){pnl_txt}"
        else:
            return "CLOSE_SL", "ZARARI KES - SHORT KAPAT", f"EMA7 EMA30'u yukari kesti (2 mum onay){pnl_txt}"
    # Devam
    return "HOLD", "TUT", f"Short korunuyor - EMA7 altta (ayi {bear}/5){pnl_txt}"


# ======================================================================
# DIGER BORSALAR (Exchange Breakdown - sadece gosterim)
# ======================================================================

def bybit_summary(sym):
    try:
        j = http_get(f"https://api.bybit.com/v5/market/account-ratio?category=linear&symbol={sym}&period=1h&limit=1")
        if j.get("retCode") != 0:
            return {"ok": False, "error": j.get("retMsg")}
        lst = (j.get("result") or {}).get("list") or []
        if not lst:
            return {"ok": False, "error": "NO DATA"}
        long_pct = float(lst[0]["buyRatio"]) * 100
        oi = funding = None
        t = safe(lambda: http_get(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={sym}"), {})
        l2 = (t.get("result") or {}).get("list") or []
        if l2:
            tk = l2[0]
            if tk.get("openInterestValue"): oi = float(tk["openInterestValue"])
            if tk.get("fundingRate"): funding = float(tk["fundingRate"]) * 100
        return {"ok": True, "longPct": long_pct, "shortPct": 100-long_pct, "openInterest": oi, "fundingRate": funding}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def okx_summary(symbol):
    ccy = symbol.replace("USDT", "")
    inst = f"{ccy}-USDT-SWAP"
    try:
        j = http_get(f"https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy={ccy}&period=1H&limit=1")
        if j.get("code") != "0" or not j.get("data"):
            return {"ok": False, "error": "NO DATA"}
        ratio = float(j["data"][0][1])
        long_pct = ratio / (1 + ratio) * 100
        oi = funding = None
        oj = safe(lambda: http_get(f"https://www.okx.com/api/v5/public/open-interest?instId={inst}"), {})
        if oj.get("code") == "0" and oj.get("data") and oj["data"][0].get("oiUsd"):
            oi = float(oj["data"][0]["oiUsd"])
        fj = safe(lambda: http_get(f"https://www.okx.com/api/v5/public/funding-rate?instId={inst}"), {})
        if fj.get("code") == "0" and fj.get("data"):
            funding = float(fj["data"][0]["fundingRate"]) * 100
        return {"ok": True, "longPct": long_pct, "shortPct": 100-long_pct, "openInterest": oi, "fundingRate": funding}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def bitget_summary(sym):
    try:
        j = http_get(f"https://api.bitget.com/api/v2/mix/market/account-long-short?symbol={sym}&period=1h&productType=USDT-FUTURES&limit=1")
        if j.get("code") != "00000" or not j.get("data"):
            return {"ok": False, "error": "NO DATA"}
        long_pct = float(j["data"][0]["longAccountRatio"]) * 100
        oi = funding = None
        oj = safe(lambda: http_get(f"https://api.bitget.com/api/v2/mix/market/open-interest?symbol={sym}&productType=USDT-FUTURES"), {})
        if oj.get("code") == "00000":
            ol = (oj.get("data") or {}).get("openInterestList") or []
            if ol:
                qty = float(ol[0].get("size") or 0)
                tj = safe(lambda: http_get(f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={sym}&productType=USDT-FUTURES"), {})
                td = tj.get("data")
                if td:
                    if isinstance(td, list): td = td[0]
                    price = float(td.get("lastPr") or 0)
                    oi = qty * price if price else None
        fj = safe(lambda: http_get(f"https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={sym}&productType=USDT-FUTURES"), {})
        if fj.get("code") == "00000":
            fd = fj.get("data") or []
            if isinstance(fd, list) and fd: funding = float(fd[0].get("fundingRate") or 0) * 100
        return {"ok": True, "longPct": long_pct, "shortPct": 100-long_pct, "openInterest": oi, "fundingRate": funding}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ======================================================================
# BACKTEST MOTORU (BACKTEST AREA) - canli decide() ile ayni mantik
# ======================================================================

# Backtest sonuc cache (5 gunde 1 calistirilacak ama yine de koru)
_bt_cache = {}  # key -> (timestamp, result)
_BT_CACHE_TTL = 1800  # 30 dakika

def _bt_paginated(base_url, sym, period, total_needed):
    """futures/data endpoint'leri max 500. Daha fazlasi icin endTime ile geriye sayfala.
    eski->yeni sirali tek liste doner. Istekler arasi kisa gecikme (ban korumasi)."""
    out = []
    remaining = total_needed
    end_time = None
    safety = 0
    while remaining > 0 and safety < 6:
        safety += 1
        limit = min(500, remaining)
        url = f"{base_url}?symbol={sym}&period={period}&limit={limit}"
        if end_time:
            url += f"&endTime={end_time}"
        try:
            chunk = http_get(url)
        except Exception:
            break
        if not chunk:
            break
        out = chunk + out  # basa ekle (daha eski veri)
        first_ts = int(chunk[0].get("timestamp", 0))
        if first_ts <= 0:
            break
        end_time = first_ts - 1
        remaining -= len(chunk)
        _time.sleep(0.4)  # ban korumasi: istekler arasi gecikme
        if len(chunk) < limit:
            break
    return out


def run_backtest(symbol, days=22):
    """Son N gun, her 1h bar icin canli decide() karari + sonraki PnL.
    Cache'li (30dk). decide() AYNEN canli sistemdeki - tek kaynak."""
    sym = symbol.upper().replace("USDT", "") + "USDT"
    cache_key = f"{sym}_{days}"
    now = _time.time()
    if cache_key in _bt_cache:
        ts, res = _bt_cache[cache_key]
        if now - ts < _BT_CACHE_TTL:
            res = dict(res); res["cached"] = True
            return res

    if _binance_banned():
        raise RuntimeError("Binance gecici banli, backtest simdi yapilamaz. Birkac dakika sonra dene.")

    bars = days * 24                     # test bar sayisi (~528)
    kline_need = bars + 370              # EMA365 isinmasi icin ek
    kline_need = min(kline_need, 1500)   # Binance kline limiti

    # 1) Kline (fiyat + EMA365/7/30 icin) - tek istek, 1500'e kadar
    kl = http_get(f"https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=1h&limit={kline_need}")
    if not kl or len(kl) < 400:
        raise RuntimeError("Yeterli kline verisi yok")
    _time.sleep(0.4)

    closes = [float(k[4]) for k in kl]
    open_times = [int(k[0]) for k in kl]
    ema365_arr = calc_ema(closes, 365)
    ema7_arr = calc_ema(closes, 7)
    ema30_arr = calc_ema(closes, 30)

    # 2) Whale/retail/OI/CVD - sayfalama ile (futures/data 500 limit)
    g_data = _bt_paginated("https://fapi.binance.com/futures/data/globalLongShortAccountRatio", sym, "1h", bars)
    t_data = _bt_paginated("https://fapi.binance.com/futures/data/topLongShortPositionRatio", sym, "1h", bars)
    oi_data = _bt_paginated("https://fapi.binance.com/futures/data/openInterestHist", sym, "1h", bars)
    cvd_data = _bt_paginated("https://fapi.binance.com/futures/data/takerlongshortRatio", sym, "1h", bars)

    # Timestamp -> deger haritalari
    def to_map(arr, fn):
        m = {}
        for x in arr:
            ts = int(x.get("timestamp", 0))
            v = fn(x)
            if ts and v is not None:
                m[ts] = v
        return m

    g_map = to_map(g_data, lambda x: float(x["longAccount"]) * 100)
    t_map = to_map(t_data, lambda x: float(x["longAccount"]) * 100)
    oi_map = to_map(oi_data, lambda x: float(x.get("sumOpenInterestValue") or 0))
    def cvd_pct(x):
        b = float(x.get("buyVol") or 0); s = float(x.get("sellVol") or 0)
        return b / (b + s) * 100 if (b + s) > 0 else None
    cvd_map = to_map(cvd_data, cvd_pct)

    # futures/data timestamp'leri kline openTime ile ayni hizada (1h sinirlari)
    # Her kline bari icin en yakin futures/data noktasini bul
    def nearest(m, ts):
        if ts in m: return m[ts]
        # 1h tolerans
        for delta in (3600000, -3600000, 7200000):
            if ts + delta in m: return m[ts + delta]
        return None

    # 3) Her bar icin karar - EMA365 hazir olan barlardan basla
    signals = []
    bars_data = []      # state machine icin her bar'in karari
    regime_series = []  # grafik icin: her bar rejim + fiyat
    start_idx = None
    for i in range(len(closes)):
        if ema365_arr[i] is not None:
            start_idx = i
            break
    if start_idx is None:
        raise RuntimeError("EMA365 hesaplanamadi (yetersiz veri)")

    # Sadece son `bars` kadarini test et (ama EMA365 olan yerden)
    test_start = max(start_idx + 2, len(closes) - bars)

    for i in range(test_start, len(closes)):
        ts = open_times[i]
        price_now = closes[i]
        price_2ago = closes[i-2] if i >= 2 else closes[i]
        price_change = (price_now - price_2ago) / price_2ago * 100 if price_2ago > 0 else None

        # EMA365 rejim (2 mum kapanis kurali)
        e365 = ema365_arr[i]; e365_prev = ema365_arr[i-1] if i >= 1 else None
        above = below = False
        if e365 and e365_prev:
            above = closes[i] > e365 and closes[i-1] > e365_prev
            below = closes[i] < e365 and closes[i-1] < e365_prev
        ema365_dist = (price_now - e365) / e365 * 100 if e365 else None

        # EMA7/30 cross - 2 mum kapanis teyitli (canli sistemle ayni)
        ema_cross = None
        ema_cu = ema_cd = False
        if ema7_arr[i] and ema30_arr[i]:
            ema_cross = "BULL" if ema7_arr[i] > ema30_arr[i] else "BEAR"
            if i >= 1 and ema7_arr[i-1] and ema30_arr[i-1]:
                up_now = ema7_arr[i] > ema30_arr[i]
                up_prev = ema7_arr[i-1] > ema30_arr[i-1]
                if up_now and up_prev: ema_cu = True
                elif (not up_now) and (not up_prev): ema_cd = True

        # Whale/retail/oi/cvd bu bar icin
        g_now = nearest(g_map, ts)
        t_now = nearest(t_map, ts)
        g_2ago = nearest(g_map, ts - 2*3600000)
        t_2ago = nearest(t_map, ts - 2*3600000)
        oi_now = nearest(oi_map, ts)
        oi_2ago = nearest(oi_map, ts - 2*3600000)
        cvd_now = nearest(cvd_map, ts)

        whale_now = t_now
        whale_2ago = t_2ago
        retail_now = retail_2ago = None
        if g_now is not None and whale_now is not None:
            retail_now = max(0, min(100, (g_now - whale_now * 0.2) / 0.8))
        if g_2ago is not None and whale_2ago is not None:
            retail_2ago = max(0, min(100, (g_2ago - whale_2ago * 0.2) / 0.8))
        oi_change = None
        if oi_now and oi_2ago and oi_2ago > 0:
            oi_change = (oi_now - oi_2ago) / oi_2ago * 100

        # decide() icin d dict (canli sistemle ayni format)
        d = {
            "ok": True, "priceNow": price_now, "priceChange": price_change,
            "globalLongNow": g_now, "whaleNow": whale_now, "whale2ago": whale_2ago,
            "retailNow": retail_now, "retail2ago": retail_2ago,
            "oiNow": oi_now, "oiChange": oi_change,
            "takerBuyNow": cvd_now, "takerBuy2ago": None, "funding": None,
            "ema365": e365, "ema7": ema7_arr[i], "ema30": ema30_arr[i],
            "ema365Dist": ema365_dist, "aboveEma365": above, "belowEma365": below,
            "emaCross": ema_cross, "emaCrossUp": ema_cu, "emaCrossDown": ema_cd,
        }

        dec = decide(symbol, d, "flat")  # yon + giris karari

        regime_series.append({"t": int(ts/1000), "price": price_now,
                              "regime": dec.get("regime"), "ema365": round(e365,2) if e365 else None})

        # Her bar'in karar + kesisim bilgisini sakla (state machine icin)
        bars_data.append({
            "i": i, "ts": int(ts/1000), "price": price_now,
            "action": dec.get("action"), "actionLabel": dec.get("actionLabel"),
            "regime": dec.get("regime"), "bull": dec.get("bullScore"), "bear": dec.get("bearScore"),
            "crossUp": ema_cu, "crossDown": ema_cd,
        })

    # 4) GERCEK STRATEJI SIMULASYONU (state machine)
    # FLAT -> kesisim+yon ile AC -> ters kesisimde KAPAT -> FLAT
    trades = []
    pos = None  # {"dir","entryPrice","entryTs","entryBull","entryBear","entryRegime","gamble"}
    for b in bars_data:
        if pos is None:
            # FLAT: giris sinyali var mi? (gamble dahil opsiyonel)
            act = b["action"]
            if act in ("LONG_OPEN", "SHORT_OPEN"):
                direction = "long" if "LONG" in act else "short"
                pos = {"dir": direction, "entryPrice": b["price"], "entryTs": b["ts"],
                       "entryBull": b["bull"], "entryBear": b["bear"],
                       "entryRegime": b["regime"], "gamble": False}
            # NOT: GAMBLE girisleri simulasyona DAHIL EDILMIYOR (manuel karar)
        else:
            # POZISYONDA: ters kesisim ile cikis
            exit_now = False
            if pos["dir"] == "long" and b["crossDown"]:
                exit_now = True
            elif pos["dir"] == "short" and b["crossUp"]:
                exit_now = True
            if exit_now:
                if pos["dir"] == "long":
                    pnl = (b["price"] - pos["entryPrice"]) / pos["entryPrice"] * 100
                else:
                    pnl = (pos["entryPrice"] - b["price"]) / pos["entryPrice"] * 100
                hours = round((b["ts"] - pos["entryTs"]) / 3600, 1)
                trades.append({
                    "dir": pos["dir"], "entryTs": pos["entryTs"], "exitTs": b["ts"],
                    "entryPrice": round(pos["entryPrice"], 2), "exitPrice": round(b["price"], 2),
                    "pnl": round(pnl, 2), "hours": hours,
                    "entryRegime": pos["entryRegime"],
                    "entryBull": pos["entryBull"], "entryBear": pos["entryBear"],
                })
                pos = None

    # Acik kalan pozisyon (test sonunda hala acik) - son fiyatla degerlendir (acik isaretle)
    open_trade = None
    if pos is not None and bars_data:
        last = bars_data[-1]
        if pos["dir"] == "long":
            pnl = (last["price"] - pos["entryPrice"]) / pos["entryPrice"] * 100
        else:
            pnl = (pos["entryPrice"] - last["price"]) / pos["entryPrice"] * 100
        open_trade = {
            "dir": pos["dir"], "entryTs": pos["entryTs"], "exitTs": last["ts"],
            "entryPrice": round(pos["entryPrice"], 2), "exitPrice": round(last["price"], 2),
            "pnl": round(pnl, 2), "hours": round((last["ts"]-pos["entryTs"])/3600, 1),
            "entryRegime": pos["entryRegime"], "entryBull": pos["entryBull"],
            "entryBear": pos["entryBear"], "open": True,
        }

    # 5) Trade istatistikleri
    def trade_stats(tlist):
        if not tlist:
            return None
        wins = [t for t in tlist if t["pnl"] > 0]
        losses = [t for t in tlist if t["pnl"] <= 0]
        total_pnl = sum(t["pnl"] for t in tlist)
        return {
            "n": len(tlist),
            "wins": len(wins), "losses": len(losses),
            "winRate": round(len(wins)/len(tlist)*100, 1),
            "totalPnl": round(total_pnl, 2),
            "avgPnl": round(total_pnl/len(tlist), 2),
            "avgWin": round(sum(t["pnl"] for t in wins)/len(wins), 2) if wins else 0,
            "avgLoss": round(sum(t["pnl"] for t in losses)/len(losses), 2) if losses else 0,
            "avgHours": round(sum(t["hours"] for t in tlist)/len(tlist), 1),
            "bestPnl": round(max(t["pnl"] for t in tlist), 2),
            "worstPnl": round(min(t["pnl"] for t in tlist), 2),
        }

    longs = [t for t in trades if t["dir"] == "long"]
    shorts = [t for t in trades if t["dir"] == "short"]

    summary = {
        "totalTrades": len(trades),
        "all": trade_stats(trades),
        "long": trade_stats(longs),
        "short": trade_stats(shorts),
        "openTrade": open_trade,
    }

    # Grafik icin sinyal marker'lari (giris + cikis)
    signals = []
    for t in trades + ([open_trade] if open_trade else []):
        signals.append({"ts": t["entryTs"], "type": "entry", "dir": t["dir"], "price": t["entryPrice"]})
        if not t.get("open"):
            signals.append({"ts": t["exitTs"], "type": "exit", "dir": t["dir"], "price": t["exitPrice"]})

    result = {
        "ok": True, "symbol": symbol, "days": days,
        "barsCovered": len(regime_series),
        "trades": trades, "summary": summary,
        "signals": signals,
        "regimeSeries": regime_series,
        "cached": False,
    }
    _bt_cache[cache_key] = (now, result)
    return result


# ======================================================================
# ANA ANALIZ
# ======================================================================

def analyze_symbol(symbol, user_position, entry_price=None):
    sym = symbol.upper().replace("USDT", "") + "USDT"

    # H1 ana veri + diger borsalar paralel
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_main = pool.submit(fetch_h1_data, sym)
        f_bb = pool.submit(bybit_summary, sym)
        f_okx = pool.submit(okx_summary, symbol)
        f_bg = pool.submit(bitget_summary, sym)
        try:
            d = f_main.result()
        except Exception as e:
            return {"ok": False, "error": f"Ana veri hatasi: {e}"}
        exchanges = {}
        # Binance ana veriden
        exchanges["Binance"] = {
            "ok": True,
            "longPct": d.get("globalLongNow"),
            "shortPct": 100 - d.get("globalLongNow") if d.get("globalLongNow") else None,
            "whaleLongPct": d.get("whaleNow"),
            "retailLongPct": d.get("retailNow"),
            "openInterest": d.get("oiNow"),
            "fundingRate": d.get("funding"),
        }
        for name, fut in [("Bybit", f_bb), ("OKX", f_okx), ("Bitget", f_bg)]:
            try: exchanges[name] = fut.result()
            except Exception as e: exchanges[name] = {"ok": False, "error": str(e)}

    decision = decide(symbol, d, user_position, entry_price)

    saved = False
    if SUPABASE_ENABLED:
        try:
            saved = save_signal_if_meaningful(symbol, decision, d.get("priceNow"))
        except Exception as e:
            sys.stderr.write(f"save hatasi: {e}\n")

    return {
        "ok": True, "symbol": symbol, "userPosition": user_position,
        "entryPrice": entry_price, "currentPrice": d.get("priceNow"),
        "decision": decision, "data": d, "exchanges": exchanges,
        "signalSaved": saved,
    }


# ======================================================================
# HTTP SERVER
# ======================================================================

MANIFEST_JSON = json.dumps({
    "name": "L/S Decision Engine", "short_name": "L/S Engine",
    "start_url": "/", "display": "standalone",
    "background_color": "#0a0e0d", "theme_color": "#0a0e0d",
    "orientation": "portrait",
    "icons": [{"src": "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 192 192'%3E%3Crect fill='%230a0e0d' width='192' height='192'/%3E%3Ctext x='96' y='110' font-family='monospace' font-size='48' font-weight='bold' fill='%2300d09c' text-anchor='middle'%3EL/S%3C/text%3E%3C/svg%3E",
               "sizes": "192x192", "type": "image/svg+xml"}]
})


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write(f"  - {self.address_string()} - {fmt % args}\n")

    def _send(self, status, ctype, body):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status, payload):
        self._send(status, "application/json; charset=utf-8", json.dumps(payload).encode("utf-8"))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        q = urllib.parse.parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._send(200, "text/html; charset=utf-8", DASHBOARD_HTML.encode("utf-8"))
            return
        if path == "/backtest":
            self._send(200, "text/html; charset=utf-8", BACKTEST_HTML.encode("utf-8"))
            return
        if path == "/manifest.json":
            self._send(200, "application/json; charset=utf-8", MANIFEST_JSON.encode("utf-8"))
            return
        if path == "/healthz":
            self._json(200, {"ok": True})
            return

        if path == "/api/analyze":
            symbol = (q.get("symbol", ["BTC"])[0] or "BTC").strip().upper()
            pos = (q.get("position", ["flat"])[0] or "flat").lower()
            if pos not in ("long", "short", "flat"): pos = "flat"
            entry = None
            try:
                e = q.get("entry", [""])[0]
                if e and e.strip():
                    entry = float(e)
                    if entry <= 0: entry = None
            except (ValueError, TypeError):
                entry = None
            try:
                self._json(200, analyze_symbol(symbol, pos, entry))
            except Exception as e:
                self._json(200, {"ok": False, "error": str(e)})
            return

        if path == "/api/cron-check":
            try:
                data = analyze_symbol("BTC", "flat", None)
                self._json(200, {"ok": True,
                    "action": data.get("decision", {}).get("action"),
                    "regime": data.get("decision", {}).get("regime"),
                    "bull": data.get("decision", {}).get("bullScore"),
                    "bear": data.get("decision", {}).get("bearScore"),
                    "saved": data.get("signalSaved", False),
                    "supabase": SUPABASE_ENABLED})
            except Exception as e:
                self._json(200, {"ok": False, "error": str(e)})
            return

        if path == "/api/signals":
            symbol = (q.get("symbol", ["BTC"])[0] or "BTC").strip().upper()
            try:
                limit = max(1, min(int(q.get("limit", ["50"])[0]), 200))
            except ValueError:
                limit = 50
            try:
                hist = get_signal_history(symbol, limit)
                self._json(200, {"ok": True, "symbol": symbol, "count": len(hist),
                                 "signals": hist, "supabaseEnabled": SUPABASE_ENABLED})
            except Exception as e:
                self._json(200, {"ok": False, "error": str(e), "supabaseEnabled": SUPABASE_ENABLED})
            return

        if path == "/api/backtest":
            symbol = (q.get("symbol", ["BTC"])[0] or "BTC").strip().upper()
            try:
                days = max(5, min(int(q.get("days", ["22"])[0]), 30))
            except ValueError:
                days = 22
            try:
                self._json(200, run_backtest(symbol, days))
            except Exception as e:
                self._json(200, {"ok": False, "error": str(e)})
            return

        self._json(404, {"ok": False, "error": "not found"})


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    print(f"L/S Decision Engine v9 listening on {HOST}:{PORT}", flush=True)
    print(f"Supabase: {'ENABLED' if SUPABASE_ENABLED else 'DISABLED'}", flush=True)
    try:
        with ThreadedServer((HOST, PORT), Handler) as srv:
            srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)


DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="theme-color" content="#0a0e0d">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>L/S Decision Engine</title>
<link rel="manifest" href="/manifest.json">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Major+Mono+Display&display=swap" rel="stylesheet">
<script src="https://s3.tradingview.com/tv.js"></script>
<style>
  :root {
    --bg:#0a0e0d; --bg-2:#0f1413;
    --border:#1f2a28; --border-strong:#2a3a37;
    --text:#d4dcd9; --text-dim:#6e7976; --text-faint:#3f4845;
    --green:#00d09c; --red:#ff4d6d; --amber:#ffb83d; --accent:#6df5d4;
    --binance:#f3ba2f; --bybit:#ff4d6d; --okx:#ffffff; --bitget:#6df5d4;
    --grid-line:rgba(255,255,255,0.012);
  }
  body.light {
    --bg:#f4f6f5; --bg-2:#ffffff;
    --border:#dde3e1; --border-strong:#c4cecb;
    --text:#1a2422; --text-dim:#6e7976; --text-faint:#a8b2af;
    --green:#00a37a; --red:#e02e4d; --amber:#d68a00; --accent:#00a37a;
    --okx:#333333;
    --grid-line:rgba(0,0,0,0.015);
  }
  * { box-sizing:border-box; margin:0; padding:0; -webkit-tap-highlight-color:transparent; }
  html, body { background:var(--bg); color:var(--text);
    font-family:'JetBrains Mono', monospace; font-size:13px; line-height:1.5;
    min-height:100vh; overflow-x:hidden; -webkit-font-smoothing:antialiased;
    transition:background 0.3s, color 0.3s; }
  body::after { content:''; position:fixed; inset:0;
    background-image: linear-gradient(var(--grid-line) 1px, transparent 1px),
                      linear-gradient(90deg, var(--grid-line) 1px, transparent 1px);
    background-size:40px 40px; pointer-events:none; z-index:0; }
  .wrap { position:relative; z-index:1; max-width:1400px; margin:0 auto;
    padding:24px; padding-top:calc(24px + env(safe-area-inset-top));
    padding-bottom:calc(24px + env(safe-area-inset-bottom)); }

  header { display:flex; align-items:center; justify-content:space-between;
    padding-bottom:18px; border-bottom:1px solid var(--border); margin-bottom:24px; gap:12px; flex-wrap:wrap; }
  .logo { font-family:'Major Mono Display', monospace; font-size:20px; letter-spacing:0.04em; color:var(--text); }
  .logo span { color:var(--green); }
  .header-right { display:flex; gap:14px; align-items:center; }
  .meta { display:flex; gap:14px; align-items:center; font-size:11px; color:var(--text-dim); }
  .meta .dot { width:6px; height:6px; border-radius:50%; background:var(--green);
    display:inline-block; margin-right:6px; box-shadow:0 0 6px var(--green); animation:pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1;} 50%{opacity:0.4;} }
  .theme-btn { background:transparent; border:1px solid var(--border-strong); color:var(--text);
    width:34px; height:34px; border-radius:0; cursor:pointer; font-size:16px;
    display:flex; align-items:center; justify-content:center; -webkit-appearance:none; }
  .theme-btn:hover { border-color:var(--text-dim); }

  .controls { display:grid; grid-template-columns:1fr auto; gap:12px; margin-bottom:24px;
    padding:14px; background:var(--bg-2); border:1px solid var(--border); }
  .input-group { display:flex; flex-direction:column; gap:6px; }
  .input-group label { font-size:10px; letter-spacing:0.18em; color:var(--text-dim); text-transform:uppercase; }
  input[type="text"] { background:var(--bg); border:1px solid var(--border); color:var(--text);
    font-family:'JetBrains Mono',monospace; font-size:14px; padding:10px 12px; outline:none;
    text-transform:uppercase; -webkit-appearance:none; border-radius:0; }
  input[type="text"]:focus { border-color:var(--green); }
  button.run { background:var(--green); color:var(--bg); border:none; padding:0 24px;
    font-family:'JetBrains Mono',monospace; font-weight:700; font-size:13px; letter-spacing:0.12em;
    cursor:pointer; align-self:end; height:40px; -webkit-appearance:none; border-radius:0; }
  button.run:hover { background:var(--accent); }
  button.run:disabled { background:var(--border-strong); color:var(--text-dim); cursor:wait; }

  h2.section { font-size:11px; letter-spacing:0.22em; color:var(--text-dim); text-transform:uppercase;
    margin:32px 0 12px; padding-bottom:8px; border-bottom:1px solid var(--border); }
  h2.section .arrow { color:var(--green); margin-right:6px; }

  /* KARAR PANELI */
  .decision { background:var(--bg-2); border:1px solid var(--border-strong); padding:24px;
    margin-bottom:24px; position:relative; overflow:hidden; }
  .decision::before { content:''; position:absolute; left:0; top:0; bottom:0; width:4px;
    background:var(--text-dim); transition:background 0.3s; }
  .decision.act-long::before { background:var(--green); }
  .decision.act-short::before { background:var(--red); }
  .decision.act-close::before { background:var(--amber); }
  .decision.act-flat::before { background:var(--text-dim); }
  .decision.act-gamble::before { background:#b06dff; }
  .decision-head { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:18px; gap:16px; flex-wrap:wrap; }
  .action-label { font-size:24px; font-weight:700; letter-spacing:0.05em; }
  .act-long .action-label { color:var(--green); }
  .act-short .action-label { color:var(--red); }
  .act-close .action-label { color:var(--amber); }
  .act-flat .action-label { color:var(--text-dim); }
  .act-gamble .action-label { color:#b06dff; }
  .pills { display:flex; gap:8px; align-items:center; flex-wrap:wrap; }
  .pill { font-size:11px; letter-spacing:0.06em; padding:4px 10px; border:1px solid var(--border-strong); }
  .pill b { font-size:13px; }
  .pill.regime.boga b { color:var(--green); }
  .pill.regime.ayi b { color:var(--red); }
  .pill.regime.yatay b { color:var(--text-dim); }
  .pill.pnl.pos b { color:var(--green); }
  .pill.pnl.neg b { color:var(--red); }

  .subtitle { font-size:11px; color:var(--text-dim); letter-spacing:0.1em; margin-top:4px; }

  .position-control { display:flex; gap:8px; margin-top:16px; flex-wrap:wrap; align-items:center; }
  .pos-btn { background:transparent; border:1px solid var(--border-strong); color:var(--text-dim);
    padding:8px 16px; font-family:inherit; font-size:11px; letter-spacing:0.1em; cursor:pointer;
    -webkit-appearance:none; border-radius:0; transition:all 0.15s; }
  .pos-btn:hover { color:var(--text); border-color:var(--text-dim); }
  .pos-btn.active.long { background:var(--green); color:var(--bg); border-color:var(--green); }
  .pos-btn.active.short { background:var(--red); color:var(--bg); border-color:var(--red); }
  .pos-btn.active.flat { background:var(--text-dim); color:var(--bg); border-color:var(--text-dim); }
  .pos-label { font-size:10px; color:var(--text-dim); margin-right:8px; letter-spacing:0.15em; }
  .entry-control { display:none; gap:8px; margin-top:12px; align-items:center; flex-wrap:wrap; }
  .entry-control.visible { display:flex; }
  .entry-control label { font-size:10px; letter-spacing:0.15em; color:var(--text-dim); }
  .entry-control input { background:var(--bg); border:1px solid var(--border-strong); color:var(--text);
    font-family:inherit; font-size:13px; padding:6px 10px; width:130px; outline:none;
    -webkit-appearance:none; border-radius:0; }
  .entry-control input:focus { border-color:var(--green); }
  .price-info { font-size:10px; color:var(--text-dim); letter-spacing:0.1em; }
  .price-info b { color:var(--text); }

  /* KOSUL MATRISI */
  .cond-wrap { display:grid; grid-template-columns:1fr 1fr; gap:1px; background:var(--border);
    border:1px solid var(--border); margin-bottom:8px; }
  .cond-col { background:var(--bg-2); padding:16px; }
  .cond-col h3 { font-size:12px; letter-spacing:0.1em; margin-bottom:12px; display:flex;
    justify-content:space-between; align-items:center; }
  .cond-col.bull h3 { color:var(--green); }
  .cond-col.bear h3 { color:var(--red); }
  .cond-col h3 .score { font-size:18px; font-weight:700; }
  .cond-row { display:flex; align-items:center; gap:10px; padding:6px 0; font-size:11px;
    border-top:1px solid var(--border); }
  .cond-row:first-of-type { border-top:none; }
  .cond-bit { width:22px; height:22px; display:flex; align-items:center; justify-content:center;
    font-weight:700; font-size:12px; flex-shrink:0; border:1px solid var(--border-strong); }
  .cond-bit.on.bull { background:var(--green); color:var(--bg); border-color:var(--green); }
  .cond-bit.on.bear { background:var(--red); color:var(--bg); border-color:var(--red); }
  .cond-bit.off { color:var(--text-faint); }
  .cond-text { color:var(--text-dim); }
  .cond-row.active .cond-text { color:var(--text); }

  .reasoning { margin-top:8px; padding:16px; background:var(--bg-2); border:1px solid var(--border); }
  .reasoning .r-title { font-size:10px; letter-spacing:0.18em; color:var(--text-dim); margin-bottom:12px; }
  #reasoningList { display:grid; grid-template-columns:1fr 1fr; gap:1px; background:var(--border); }
  .reason-col { background:var(--bg-2); padding:12px 14px; }
  .reason-col-title { font-size:10px; letter-spacing:0.12em; margin-bottom:8px; font-weight:700; }
  .reason-col-title.bull { color:var(--green); }
  .reason-col-title.bear { color:var(--red); }
  .reason-ul { list-style:none; padding:0; }
  .reason-ul li { font-size:11px; color:var(--text); padding:3px 0; padding-left:14px; position:relative; }
  .reason-ul.bull li::before { content:'+'; color:var(--green); position:absolute; left:0; font-weight:700; }
  .reason-ul.bear li::before { content:'-'; color:var(--red); position:absolute; left:0; font-weight:700; }
  .reason-ul li.r-none { color:var(--text-faint); }
  .reason-ul li.r-none::before { content:'\\00b7'; color:var(--text-faint); }
  @media (max-width:720px) { #reasoningList { grid-template-columns:1fr; } }

  /* TradingView */
  .tv-wrap { background:var(--bg-2); border:1px solid var(--border); padding:1px; }
  .tv-chart { height:450px; width:100%; }

  /* Sinyal gecmisi */
  .signals-wrap { background:var(--bg-2); border:1px solid var(--border); max-height:360px; overflow-y:auto; }
  .signal-row { display:grid; grid-template-columns:100px 150px 50px 1fr; gap:12px; padding:10px 14px;
    border-bottom:1px solid var(--border); font-size:11px; align-items:center; }
  .signal-row:last-child { border-bottom:none; }
  .sig-time { color:var(--text-dim); font-size:10px; }
  .sig-action { font-weight:700; letter-spacing:0.04em; font-size:11px; }
  .sig-action.long { color:var(--green); }
  .sig-action.short { color:var(--red); }
  .sig-action.close { color:var(--amber); }
  .sig-action.gamble { color:#b06dff; }
  .sig-score { font-weight:600; text-align:center; }
  .sig-meta { color:var(--text-dim); font-size:10px; }
  .sig-meta b { color:var(--text); }
  .signals-empty { padding:32px 20px; text-align:center; color:var(--text-faint); font-size:11px; }

  /* Exchange grid */
  .grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(260px,1fr)); gap:1px;
    background:var(--border); border:1px solid var(--border); }
  .card { background:var(--bg-2); padding:18px; border-top:2px solid transparent; }
  .card.ex-binance { border-top-color:var(--binance); }
  .card.ex-bybit { border-top-color:var(--bybit); }
  .card.ex-okx { border-top-color:var(--okx); }
  .card.ex-bitget { border-top-color:var(--bitget); }
  .card-head { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:14px; }
  .ex-name { font-size:14px; font-weight:700; letter-spacing:0.08em; }
  .ex-status { font-size:10px; }
  .ex-status.ok { color:var(--green); }
  .ex-status.err { color:var(--red); }
  .ratio-row { display:flex; justify-content:space-between; margin-top:8px; font-size:11px; }
  .ratio-row .l { color:var(--text-dim); }
  .ratio-row .v { color:var(--text); font-weight:500; }
  .divider { height:1px; background:var(--border); margin:12px 0 6px; }
  .v.fr-pos { color:var(--green); }
  .v.fr-neg { color:var(--red); }
  .err-msg { color:var(--red); font-size:11px; margin-top:8px; opacity:0.7; }

  .info { margin-top:32px; padding:16px; background:var(--bg-2); border:1px dashed var(--border-strong);
    font-size:11px; color:var(--text-dim); line-height:1.7; }
  .info b { color:var(--text); }
  .blink { animation:blink 1s infinite; }
  @keyframes blink { 50% { opacity:0.3; } }
  .loading-overlay { color:var(--text-faint); padding:40px 20px; text-align:center; font-size:12px;
    letter-spacing:0.15em; grid-column:1/-1; background:var(--bg-2); }

  @media (max-width:720px) {
    .wrap { padding:16px; }
    .controls { grid-template-columns:1fr; padding:12px; }
    button.run { height:46px; align-self:stretch; font-size:14px; }
    .action-label { font-size:20px; }
    .cond-wrap { grid-template-columns:1fr; }
    .tv-chart { height:380px; }
    .grid { grid-template-columns:1fr; }
    input[type="text"] { font-size:16px; }
    .signal-row { grid-template-columns:80px 1fr 40px; gap:8px; padding:8px 10px; font-size:10px; }
    .signal-row .sig-meta { display:none; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="logo">L/S<span>&middot;</span>ENGINE<span>&middot;</span>H1</div>
    <div class="header-right">
      <div class="meta">
        <span><span class="dot"></span>LIVE</span>
        <span id="clock">--:--:-- UTC</span>
      </div>
      <button class="theme-btn" id="themeBtn" title="Tema degistir">&#9789;</button>
    </div>
  </header>

  <div class="controls">
    <div class="input-group">
      <label>SEMBOL</label>
      <input type="text" id="symbolInput" value="BTC" autocomplete="off" autocapitalize="characters">
    </div>
    <button class="run" id="runBtn">ANALIZ ET &gt;</button>
  </div>

  <h2 class="section"><span class="arrow">&#9656;</span>KARAR MOTORU</h2>
  <div class="decision act-flat" id="decisionPanel">
    <div class="decision-head">
      <div>
        <div class="action-label" id="actionLabel">--</div>
        <div class="subtitle" id="actionSubtitle">Analiz icin bekleyin</div>
      </div>
      <div class="pills">
        <div class="pill regime" id="regimePill" style="display:none">REJIM: <b id="regimeValue">--</b></div>
        <div class="pill pnl" id="pnlPill" style="display:none">PnL: <b id="pnlValue">--</b></div>
      </div>
    </div>
    <div class="position-control">
      <span class="pos-label">POZISYONUM:</span>
      <button class="pos-btn" data-pos="long">LONG</button>
      <button class="pos-btn" data-pos="short">SHORT</button>
      <button class="pos-btn active flat" data-pos="flat">FLAT</button>
    </div>
    <div class="entry-control" id="entryControl">
      <label>GIRIS FIYATI:</label>
      <input type="text" id="entryPriceInput" placeholder="orn: 80000" inputmode="decimal">
      <span class="price-info">Simdiki: <b id="currentPriceDisplay">--</b></span>
    </div>
  </div>

  <h2 class="section"><span class="arrow">&#9656;</span>KOSUL MATRISI (H1)</h2>
  <div class="cond-wrap" id="condWrap">
    <div class="cond-col bull">
      <h3>BOGA <span class="score" id="bullScoreDisplay">0/5</span></h3>
      <div id="bullConds"></div>
    </div>
    <div class="cond-col bear">
      <h3>AYI <span class="score" id="bearScoreDisplay">0/5</span></h3>
      <div id="bearConds"></div>
    </div>
  </div>
  <div class="reasoning" id="reasoning" style="display:none">
    <div class="r-title">GEREKCE</div>
    <ul id="reasoningList"></ul>
  </div>

  <h2 class="section"><span class="arrow">&#9656;</span>BTCUSDT.P 1H GRAFIK</h2>
  <div class="tv-wrap"><div id="tvChart" class="tv-chart"></div></div>

  <h2 class="section"><span class="arrow">&#9656;</span>SINYAL GECMISI</h2>
  <div class="signals-wrap" id="signalsWrap"><div class="loading-overlay">Yukleniyor...</div></div>

  <h2 class="section"><span class="arrow">&#9656;</span>EXCHANGE BREAKDOWN (1h) - FUNDING ICIN</h2>
  <div class="grid" id="cards"></div>

  <div class="info">
    <b>v9 - H1 REJIM SISTEMI:</b> Tek timeframe (H1). EMA365 boga/ayi rejimini belirler.<br><br>
    <b>5 KOSUL:</b> (1) Fiyat EMA365 tarafi (2+ mum), (2) W/R >= 3 veya <= -3, (3) Retail trendi, (4) OI+fiyat yonu, (5) CVD+fiyat yonu. Her kosul 1/0.<br><br>
    <b>KARAR:</b> 5/5 KESIN, 4/5 GUCLU AC, 3/5 GAMBLE (manuel kontrol), <=2 YATAY/BEKLE.<br><br>
    <b>CIKIS:</b> Pozisyondayken EMA7/EMA30 ters kesisim (taze+teyitli) -> KAPAT. Ters rejim skoru guclenirse -> KAPAT.<br><br>
    <b>UYARI:</b> Finansal tavsiye degildir. Funding'i Exchange Breakdown'dan kendin degerlendir.
  </div>
</div>
<script>
const EXCHANGES = ['Binance', 'Bybit', 'OKX', 'Bitget'];
let lastFetch = null;
let userPosition = 'flat';

// Bogga kosul etiketleri
const BULL_LABELS = {
  bull_ema: 'Fiyat EMA365 ustunde (2+ mum)',
  bull_wr: 'W/R >= +3 (whale long)',
  bull_retail: 'Retail long dusuyor',
  bull_oi: 'OI artiyor + fiyat artiyor',
  bull_cvd: 'CVD alim + fiyat artiyor',
};
const BEAR_LABELS = {
  bear_ema: 'Fiyat EMA365 altinda (2+ mum)',
  bear_wr: 'W/R <= -3 (whale short)',
  bear_retail: 'Retail long artiyor',
  bear_oi: 'OI artiyor + fiyat dusuyor',
  bear_cvd: 'CVD satim + fiyat dusuyor',
};

function cleanSymbol(raw) { return (raw || '').toUpperCase().replace(/[^A-Z0-9]/g, ''); }

function fmtUSD(n) {
  if (n == null || isNaN(n)) return '\u2014';
  const a = Math.abs(n);
  if (a >= 1e9) return '$' + (n/1e9).toFixed(2) + 'B';
  if (a >= 1e6) return '$' + (n/1e6).toFixed(2) + 'M';
  if (a >= 1e3) return '$' + (n/1e3).toFixed(2) + 'K';
  return '$' + n.toFixed(2);
}

function fmtFunding(p) {
  if (p == null || isNaN(p)) return { t: '\u2014', c: '' };
  return { t: (p>=0?'+':'') + p.toFixed(4) + '%', c: p>0?'fr-pos':(p<0?'fr-neg':'') };
}

function actCls(action) {
  if (!action) return 'flat';
  if (action.includes('GAMBLE')) return 'gamble';
  if (action.includes('LONG_OPEN') || (action.includes('LONG') && action.includes('OPEN'))) return 'long';
  if (action.includes('SHORT_OPEN')) return 'short';
  if (action.includes('LONG')) return 'long';
  if (action.includes('SHORT')) return 'short';
  if (action.includes('CLOSE') || action.includes('TIGHTEN')) return 'close';
  if (action.includes('HOLD')) return userPosition === 'short' ? 'short' : 'long';
  return 'flat';
}

function renderDecision(symbol, data) {
  const dec = data.decision;
  const panel = document.getElementById('decisionPanel');
  panel.className = 'decision act-' + actCls(dec.action);
  document.getElementById('actionLabel').textContent = dec.actionLabel;
  document.getElementById('actionSubtitle').textContent = symbol + ' \u00b7 ' + dec.subtitle;

  // Rejim pill
  const rp = document.getElementById('regimePill'), rv = document.getElementById('regimeValue');
  if (dec.regime) {
    rp.style.display = ''; rp.classList.remove('boga','ayi','yatay');
    if (dec.regime === 'BOGA') { rv.textContent='BOGA'; rp.classList.add('boga'); }
    else if (dec.regime === 'AYI') { rv.textContent='AYI'; rp.classList.add('ayi'); }
    else { rv.textContent=dec.regime; rp.classList.add('yatay'); }
  } else rp.style.display = 'none';

  // PnL
  const pp = document.getElementById('pnlPill'), pv = document.getElementById('pnlValue');
  if (dec.pnlPct != null) {
    pp.style.display=''; pv.textContent=(dec.pnlPct>=0?'+':'')+dec.pnlPct.toFixed(2)+'%';
    pp.classList.remove('pos','neg'); pp.classList.add(dec.pnlPct>=0?'pos':'neg');
  } else pp.style.display='none';

  // Simdiki fiyat
  const cpd = document.getElementById('currentPriceDisplay');
  cpd.textContent = data.currentPrice ? '$'+data.currentPrice.toLocaleString('en-US',{maximumFractionDigits:2}) : '--';

  // Kosul matrisi
  const c = dec.conditions || {};
  document.getElementById('bullScoreDisplay').textContent = (dec.bullScore||0) + '/5';
  document.getElementById('bearScoreDisplay').textContent = (dec.bearScore||0) + '/5';

  document.getElementById('bullConds').innerHTML = Object.keys(BULL_LABELS).map(k => {
    const on = c[k] === 1;
    return `<div class="cond-row ${on?'active':''}">
      <span class="cond-bit ${on?'on bull':'off'}">${on?'1':'0'}</span>
      <span class="cond-text">${BULL_LABELS[k]}</span></div>`;
  }).join('');
  document.getElementById('bearConds').innerHTML = Object.keys(BEAR_LABELS).map(k => {
    const on = c[k] === 1;
    return `<div class="cond-row ${on?'active':''}">
      <span class="cond-bit ${on?'on bear':'off'}">${on?'1':'0'}</span>
      <span class="cond-text">${BEAR_LABELS[k]}</span></div>`;
  }).join('');

  // Gerekce - boga ve ayi ayri sutunlar
  const rsn = document.getElementById('reasoning');
  const bullR = dec.bullReasons || [];
  const bearR = dec.bearReasons || [];
  if (bullR.length || bearR.length) {
    rsn.style.display='';
    const bullHtml = bullR.length ? bullR.map(r=>`<li>${r}</li>`).join('') : '<li class="r-none">- yok -</li>';
    const bearHtml = bearR.length ? bearR.map(r=>`<li>${r}</li>`).join('') : '<li class="r-none">- yok -</li>';
    document.getElementById('reasoningList').innerHTML =
      `<div class="reason-col">
         <div class="reason-col-title bull">BOGA GEREKCELERI</div>
         <ul class="reason-ul bull">${bullHtml}</ul>
       </div>
       <div class="reason-col">
         <div class="reason-col-title bear">AYI GEREKCELERI</div>
         <ul class="reason-ul bear">${bearHtml}</ul>
       </div>`;
  } else rsn.style.display='none';
}

function renderExchanges(exchanges) {
  const grid = document.getElementById('cards');
  grid.innerHTML = '';
  EXCHANGES.forEach(ex => {
    const r = exchanges[ex];
    const card = document.createElement('div');
    card.className = 'card ex-' + ex.toLowerCase();
    if (r && r.ok) {
      const fr = fmtFunding(r.fundingRate);
      const whale = r.whaleLongPct, retail = r.retailLongPct;
      let ws = '';
      if (whale != null && retail != null) {
        const delta = whale - retail;
        const dc = delta>0?'fr-pos':(delta<0?'fr-neg':'');
        ws = `<div class="divider"></div>
          <div class="ratio-row"><span class="l">WHALE LONG%</span><span class="v">${whale.toFixed(2)}%</span></div>
          <div class="ratio-row"><span class="l">RETAIL LONG%</span><span class="v">${retail.toFixed(2)}%</span></div>
          <div class="ratio-row"><span class="l">W/R DELTA</span><span class="v ${dc}">${delta>0?'+':''}${delta.toFixed(2)}</span></div>`;
      }
      card.innerHTML = `
        <div class="card-head"><div class="ex-name">${ex.toUpperCase()}</div><div class="ex-status ok">\u25CF ONLINE</div></div>
        <div class="ratio-row"><span class="l">LONG</span><span class="v">${r.longPct!=null?r.longPct.toFixed(2)+'%':'--'}</span></div>
        <div class="ratio-row"><span class="l">SHORT</span><span class="v">${r.shortPct!=null?r.shortPct.toFixed(2)+'%':'--'}</span></div>
        ${ws}
        <div class="divider"></div>
        <div class="ratio-row"><span class="l">OPEN INTEREST</span><span class="v">${fmtUSD(r.openInterest)}</span></div>
        <div class="ratio-row"><span class="l">FUNDING</span><span class="v ${fr.c}">${fr.t}</span></div>`;
    } else {
      card.innerHTML = `<div class="card-head"><div class="ex-name">${ex.toUpperCase()}</div><div class="ex-status err">\u25CF NO DATA</div></div>
        <div class="err-msg">${r?.error||'veri yok'}</div>`;
    }
    grid.appendChild(card);
  });
}

async function run() {
  const raw = document.getElementById('symbolInput').value;
  const sym = cleanSymbol(raw);
  if (sym !== raw.trim().toUpperCase()) document.getElementById('symbolInput').value = sym;
  if (!sym) return;
  const btn = document.getElementById('runBtn');
  btn.disabled = true; btn.textContent = 'ANALIZ EDILIYOR...';

  let entryQ = '';
  if (userPosition !== 'flat') {
    const ep = parseFloat(document.getElementById('entryPriceInput').value);
    if (ep && ep > 0) entryQ = '&entry=' + ep;
  }

  try {
    const r = await fetch('/api/analyze?symbol='+encodeURIComponent(sym)+'&position='+userPosition+entryQ);
    const data = await r.json();
    if (!data.ok) throw new Error(data.error || 'API hatasi');
    lastFetch = data;
    renderDecision(sym, data);
    renderExchanges(data.exchanges);
  } catch (e) {
    document.getElementById('actionLabel').textContent = 'HATA';
    document.getElementById('actionSubtitle').textContent = e.message;
  } finally {
    btn.disabled = false; btn.textContent = 'ANALIZ ET >';
  }
}

// Pozisyon
function loadPosition() {
  try { const p = localStorage.getItem('userPosition'); if (p && ['long','short','flat'].includes(p)) userPosition = p; } catch {}
  document.querySelectorAll('.pos-btn').forEach(b => {
    b.classList.remove('active','long','short','flat');
    if (b.dataset.pos === userPosition) b.classList.add('active', userPosition);
  });
  const ec = document.getElementById('entryControl');
  if (userPosition === 'long' || userPosition === 'short') {
    ec.classList.add('visible');
    try { const e = localStorage.getItem('entryPrice_'+userPosition); if (e) document.getElementById('entryPriceInput').value = e; } catch {}
  } else { ec.classList.remove('visible'); document.getElementById('entryPriceInput').value=''; }
}
document.getElementById('entryPriceInput').addEventListener('input', e => {
  if (userPosition!=='flat') { try { localStorage.setItem('entryPrice_'+userPosition, e.target.value); } catch {} }
});
document.getElementById('entryPriceInput').addEventListener('blur', () => { if (lastFetch) run(); });
document.getElementById('entryPriceInput').addEventListener('keydown', e => { if (e.key==='Enter') e.target.blur(); });
document.querySelectorAll('.pos-btn').forEach(b => {
  b.addEventListener('click', () => {
    userPosition = b.dataset.pos;
    try { localStorage.setItem('userPosition', userPosition); } catch {}
    loadPosition();
    if (lastFetch) run();
  });
});
document.getElementById('runBtn').addEventListener('click', run);
document.getElementById('symbolInput').addEventListener('keydown', e => { if (e.key==='Enter'){ e.target.blur(); run(); } });

// Tema
function applyTheme(light) {
  document.body.classList.toggle('light', light);
  document.getElementById('themeBtn').innerHTML = light ? '\u2600' : '\u263D';
  document.querySelector('meta[name=theme-color]').setAttribute('content', light ? '#f4f6f5' : '#0a0e0d');
}
function loadTheme() {
  let light = false;
  try { light = localStorage.getItem('theme') === 'light'; } catch {}
  applyTheme(light);
}
document.getElementById('themeBtn').addEventListener('click', () => {
  const light = !document.body.classList.contains('light');
  try { localStorage.setItem('theme', light ? 'light' : 'dark'); } catch {}
  applyTheme(light);
  // TradingView temasini da degistir (reinit)
  setTimeout(initTV, 100);
});

// TradingView
function initTV() {
  if (typeof TradingView === 'undefined') return;
  document.getElementById('tvChart').innerHTML = '';
  new TradingView.widget({
    autosize: true, symbol: "BINANCE:BTCUSDT.P", interval: "60",
    timezone: "Europe/Istanbul", theme: document.body.classList.contains('light')?"light":"dark",
    style: "1", locale: "tr", enable_publishing: false, hide_side_toolbar: false,
    allow_symbol_change: false, container_id: "tvChart",
    studies: ["MAExp@tv-basicstudies"],
  });
}

// Sinyal gecmisi
function sigActCls(a) {
  if (!a) return '';
  if (a.includes('GAMBLE')) return 'gamble';
  if (a.includes('LONG')) return 'long';
  if (a.includes('SHORT')) return 'short';
  if (a.includes('CLOSE')) return 'close';
  return '';
}
function fmtSigTime(ts) {
  if (!ts) return '--';
  try {
    const d = new Date(ts), diff = (new Date()-d)/1000;
    if (diff<60) return Math.floor(diff)+'s';
    if (diff<3600) return Math.floor(diff/60)+'dk';
    if (diff<86400) return Math.floor(diff/3600)+'sa';
    if (diff<604800) return Math.floor(diff/86400)+'g';
    return d.toLocaleDateString('tr-TR',{day:'2-digit',month:'2-digit',hour:'2-digit',minute:'2-digit'});
  } catch { return ts.substring(0,16); }
}
async function loadSignals() {
  const wrap = document.getElementById('signalsWrap');
  try {
    const r = await fetch('/api/signals?symbol=BTC&limit=50');
    const data = await r.json();
    if (!data.ok) throw new Error(data.error||'hata');
    if (!data.supabaseEnabled) { wrap.innerHTML='<div class="signals-empty">Supabase yapilandirilmamis</div>'; return; }
    if (!data.signals || !data.signals.length) { wrap.innerHTML='<div class="signals-empty">Henuz sinyal yok. Sistem izliyor...</div>'; return; }
    wrap.innerHTML = data.signals.map(s => {
      const cls = sigActCls(s.action);
      const bull = s.bull_score||0, bear = s.bear_score||0;
      const sc = Math.max(bull, bear);
      const priceT = s.price ? '$'+(+s.price).toLocaleString('en-US',{maximumFractionDigits:0}) : '--';
      return `<div class="signal-row">
        <span class="sig-time">${fmtSigTime(s.ts)}</span>
        <span class="sig-action ${cls}">${s.action_label||s.action}</span>
        <span class="sig-score">${sc}/5</span>
        <span class="sig-meta">${priceT} \u00b7 ${s.regime||''} \u00b7 B${bull}/A${bear}</span>
      </div>`;
    }).join('');
  } catch (e) { wrap.innerHTML = `<div class="signals-empty">Hata: ${e.message}</div>`; }
}

// Saat
function tick() {
  const d = new Date(), z = n => String(n).padStart(2,'0');
  document.getElementById('clock').textContent = `${z(d.getUTCHours())}:${z(d.getUTCMinutes())}:${z(d.getUTCSeconds())} UTC`;
}
setInterval(tick, 1000); tick();

// Otomatik yenileme
setInterval(() => { if (!document.hidden && lastFetch) run(); }, 60000);
setInterval(() => { if (!document.hidden) loadSignals(); }, 60000);
document.addEventListener('visibilitychange', () => { if (!document.hidden && lastFetch) run(); });

// Init
loadTheme();
loadPosition();
run();
setTimeout(initTV, 300);
loadSignals();
</script>
</body>
</html>
"""



BACKTEST_HTML = """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="theme-color" content="#0a0e0d">
<title>Backtest Area - L/S Engine</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Major+Mono+Display&display=swap" rel="stylesheet">
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
  :root {
    --bg:#0a0e0d; --bg-2:#0f1413; --border:#1f2a28; --border-strong:#2a3a37;
    --text:#d4dcd9; --text-dim:#6e7976; --text-faint:#3f4845;
    --green:#00d09c; --red:#ff4d6d; --amber:#ffb83d; --accent:#6df5d4; --purple:#b06dff;
    --grid-line:rgba(255,255,255,0.012);
  }
  body.light {
    --bg:#f4f6f5; --bg-2:#ffffff; --border:#dde3e1; --border-strong:#c4cecb;
    --text:#1a2422; --text-dim:#6e7976; --text-faint:#a8b2af;
    --green:#00a37a; --red:#e02e4d; --amber:#d68a00; --accent:#00a37a; --purple:#8a3fd1;
    --grid-line:rgba(0,0,0,0.015);
  }
  * { box-sizing:border-box; margin:0; padding:0; -webkit-tap-highlight-color:transparent; }
  html, body { background:var(--bg); color:var(--text); font-family:'JetBrains Mono',monospace;
    font-size:13px; line-height:1.5; min-height:100vh; overflow-x:hidden;
    -webkit-font-smoothing:antialiased; transition:background 0.3s,color 0.3s; }
  body::after { content:''; position:fixed; inset:0;
    background-image:linear-gradient(var(--grid-line) 1px,transparent 1px),linear-gradient(90deg,var(--grid-line) 1px,transparent 1px);
    background-size:40px 40px; pointer-events:none; z-index:0; }
  .wrap { position:relative; z-index:1; max-width:1400px; margin:0 auto;
    padding:24px; padding-top:calc(24px + env(safe-area-inset-top));
    padding-bottom:calc(24px + env(safe-area-inset-bottom)); }
  header { display:flex; align-items:center; justify-content:space-between;
    padding-bottom:18px; border-bottom:1px solid var(--border); margin-bottom:24px; gap:12px; flex-wrap:wrap; }
  .logo { font-family:'Major Mono Display',monospace; font-size:20px; letter-spacing:0.04em; }
  .logo span { color:var(--amber); }
  .header-right { display:flex; gap:14px; align-items:center; }
  .nav-link { font-size:11px; letter-spacing:0.1em; color:var(--text-dim); text-decoration:none;
    border:1px solid var(--border-strong); padding:7px 12px; transition:all 0.15s; }
  .nav-link:hover { color:var(--text); border-color:var(--text-dim); }
  .theme-btn { background:transparent; border:1px solid var(--border-strong); color:var(--text);
    width:34px; height:34px; cursor:pointer; font-size:16px; display:flex; align-items:center;
    justify-content:center; -webkit-appearance:none; }

  .controls { display:flex; gap:12px; margin-bottom:24px; padding:16px;
    background:var(--bg-2); border:1px solid var(--border); align-items:flex-end; flex-wrap:wrap; }
  .input-group { display:flex; flex-direction:column; gap:6px; }
  .input-group label { font-size:10px; letter-spacing:0.18em; color:var(--text-dim); text-transform:uppercase; }
  .day-btns { display:flex; gap:6px; }
  .day-btn { background:transparent; border:1px solid var(--border-strong); color:var(--text-dim);
    padding:8px 14px; font-family:inherit; font-size:12px; cursor:pointer; -webkit-appearance:none; }
  .day-btn:hover { color:var(--text); border-color:var(--text-dim); }
  .day-btn.active { background:var(--amber); color:var(--bg); border-color:var(--amber); }
  button.run { background:var(--amber); color:var(--bg); border:none; padding:0 24px; height:42px;
    font-family:inherit; font-weight:700; font-size:13px; letter-spacing:0.1em; cursor:pointer;
    -webkit-appearance:none; margin-left:auto; }
  button.run:hover { opacity:0.85; }
  button.run:disabled { background:var(--border-strong); color:var(--text-dim); cursor:wait; }

  h2.section { font-size:11px; letter-spacing:0.22em; color:var(--text-dim); text-transform:uppercase;
    margin:28px 0 12px; padding-bottom:8px; border-bottom:1px solid var(--border); }
  h2.section .arrow { color:var(--amber); margin-right:6px; }

  .status-bar { padding:14px 16px; background:var(--bg-2); border:1px solid var(--border);
    font-size:12px; color:var(--text-dim); margin-bottom:8px; }
  .status-bar.error { border-color:var(--red); color:var(--red); }
  .status-bar b { color:var(--text); }

  .summary-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:1px;
    background:var(--border); border:1px solid var(--border); margin-bottom:8px; }
  .stat { background:var(--bg-2); padding:14px; }
  .stat-label { font-size:9px; letter-spacing:0.15em; color:var(--text-dim); text-transform:uppercase; }
  .stat-value { font-size:20px; font-weight:700; margin-top:6px; }
  .stat-value.pos { color:var(--green); }
  .stat-value.neg { color:var(--red); }
  .stat-value.neutral { color:var(--text); }
  .stat-sub { font-size:10px; color:var(--text-faint); margin-top:3px; }

  .wr-table { width:100%; border-collapse:collapse; background:var(--bg-2); border:1px solid var(--border); margin-bottom:8px; }
  .wr-table th, .wr-table td { padding:10px 14px; text-align:left; font-size:11px; border-bottom:1px solid var(--border); }
  .wr-table th { color:var(--text-dim); letter-spacing:0.1em; font-weight:500; text-transform:uppercase; font-size:10px; }
  .wr-table td.long { color:var(--green); font-weight:600; }
  .wr-table td.short { color:var(--red); font-weight:600; }
  .wr-table .wr-pos { color:var(--green); }
  .wr-table .wr-neg { color:var(--red); }
  .wr-table .wr-dim { color:var(--text-faint); }

  .chart-wrap { background:var(--bg-2); border:1px solid var(--border); padding:1px; margin-bottom:8px; }
  #btChart { height:420px; width:100%; }
  .chart-legend { display:flex; gap:16px; padding:10px 14px; font-size:10px; color:var(--text-dim); flex-wrap:wrap; }
  .chart-legend span { display:flex; align-items:center; gap:5px; }
  .chart-legend .dot { width:8px; height:8px; border-radius:50%; }

  .sig-list { background:var(--bg-2); border:1px solid var(--border); max-height:400px; overflow-y:auto; }
  .sig-head, .sig-row { display:grid; grid-template-columns:120px 130px 60px 1fr 70px 70px 70px; gap:10px;
    padding:9px 14px; font-size:10px; align-items:center; border-bottom:1px solid var(--border); }
  .sig-head { background:#000; color:var(--text-dim); letter-spacing:0.08em; position:sticky; top:0; }
  body.light .sig-head { background:#eef1f0; }
  .sig-row .a-long { color:var(--green); font-weight:700; }
  .sig-row .a-short { color:var(--red); font-weight:700; }
  .sig-row .a-gamble { color:var(--purple); font-weight:700; }
  .sig-row .r-boga { color:var(--green); }
  .sig-row .r-ayi { color:var(--red); }
  .sig-row .pnl-pos { color:var(--green); text-align:right; }
  .sig-row .pnl-neg { color:var(--red); text-align:right; }
  .sig-row .pnl-dim { color:var(--text-faint); text-align:right; }
  .empty { padding:40px 20px; text-align:center; color:var(--text-faint); font-size:12px; }

  .info { margin-top:28px; padding:16px; background:var(--bg-2); border:1px dashed var(--border-strong);
    font-size:11px; color:var(--text-dim); line-height:1.7; }
  .info b { color:var(--text); }

  @media (max-width:720px) {
    .wrap { padding:16px; }
    .controls { flex-direction:column; align-items:stretch; }
    button.run { margin-left:0; height:46px; }
    #btChart { height:340px; }
    .sig-head, .sig-row { grid-template-columns:70px 1fr 44px 56px; gap:6px; font-size:9px; padding:7px 8px; }
    .sig-head .h-regime, .sig-row .c-regime, .sig-head .h-1h, .sig-row .c-1h, .sig-head .h-4h, .sig-row .c-4h { display:none; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="logo">BACKTEST<span>.</span>AREA</div>
    <div class="header-right">
      <a href="/" class="nav-link">&larr; DECISION ENGINE</a>
      <button class="theme-btn" id="themeBtn" title="Tema">&#9789;</button>
    </div>
  </header>

  <div class="controls">
    <div class="input-group">
      <label>SEMBOL</label>
      <input type="text" id="symbolInput" value="BTC" style="background:var(--bg);border:1px solid var(--border);color:var(--text);font-family:inherit;font-size:14px;padding:10px 12px;outline:none;text-transform:uppercase;-webkit-appearance:none;width:100px;">
    </div>
    <div class="input-group">
      <label>GUN SAYISI</label>
      <div class="day-btns">
        <button class="day-btn" data-days="10">10</button>
        <button class="day-btn" data-days="15">15</button>
        <button class="day-btn active" data-days="22">22</button>
        <button class="day-btn" data-days="30">30</button>
      </div>
    </div>
    <button class="run" id="runBtn">BACKTEST CALISTIR &gt;</button>
  </div>

  <div class="status-bar" id="statusBar">Hazir. Gun sayisi secip BACKTEST CALISTIR'a bas. (Binance ban korumasi: 5 gunde 1 calistirman onerilir)</div>

  <h2 class="section"><span class="arrow">&#9656;</span>OZET PERFORMANS</h2>
  <div class="summary-grid" id="summaryGrid">
    <div class="stat"><div class="stat-label">Sonuc</div><div class="stat-value neutral">--</div><div class="stat-sub">backtest bekleniyor</div></div>
  </div>

  <h2 class="section"><span class="arrow">&#9656;</span>LONG / SHORT KIRILIM</h2>
  <table class="wr-table" id="wrTable">
    <thead><tr><th>YON</th><th>WIN%</th><th>TRADE</th><th>TOPLAM PnL</th><th>ORT PnL</th><th>ORT SURE</th></tr></thead>
    <tbody id="wrBody"><tr><td colspan="6" class="wr-dim" style="text-align:center;padding:24px;">Backtest calistirilmadi</td></tr></tbody>
  </table>

  <h2 class="section"><span class="arrow">&#9656;</span>GRAFIK - FIYAT + EMA365 REJIM + SINYALLER</h2>
  <div class="chart-wrap">
    <div id="btChart"></div>
    <div class="chart-legend">
      <span><span class="dot" style="background:var(--green)"></span>AL (long giris)</span>
      <span><span class="dot" style="background:var(--red)"></span>SAT (short giris)</span>
      <span><span class="dot" style="background:var(--amber)"></span>X (cikis) + EMA365</span>
    </div>
  </div>

  <h2 class="section"><span class="arrow">&#9656;</span>TRADE LISTESI (gercek giris-cikis)</h2>
  <div class="sig-list" id="sigList"><div class="empty">Backtest calistirilmadi</div></div>

  <div class="info">
    <b>BACKTEST AREA:</b> Canli Decision Engine ile <b>ayni decide() mantigini</b> kullanir. Sistem degisikliklerini burada test et.<br><br>
    <b>GERCEK STRATEJI SIMULASYONU:</b> Kesisim + yon teyidi (>=3/5) ile pozisyon ACILIR, ters EMA7/30 kesisiminde KAPATILIR. Her trade'in gercek giris-cikis PnL'i ve tutus suresi hesaplanir. GAMBLE girisleri dahil edilmez (manuel karar).<br><br>
    <b>SINIR:</b> Binance whale/retail verisi max 30 gun. EMA365 icin ek 370 bar isinma cekilir. Sonuc 30dk cache'lenir.<br><br>
    <b>NOT:</b> Gecmis performans gelecek garantisi degildir. Bu bir analiz aracidir.
  </div>
</div>
<script>
let selectedDays = 22;
let btChart = null, priceSeries = null;

// Tema (Decision Engine ile ayni mantik, paylasimli localStorage)
function applyTheme(light) {
  document.body.classList.toggle('light', light);
  document.getElementById('themeBtn').innerHTML = light ? '☀' : '☽';
  if (btChart) initChartTheme();
}
function loadTheme() {
  let light = false;
  try { light = localStorage.getItem('theme') === 'light'; } catch {}
  applyTheme(light);
}
document.getElementById('themeBtn').addEventListener('click', () => {
  const light = !document.body.classList.contains('light');
  try { localStorage.setItem('theme', light ? 'light' : 'dark'); } catch {}
  applyTheme(light);
});

// Gun secimi
document.querySelectorAll('.day-btn').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('.day-btn').forEach(x => x.classList.remove('active'));
    b.classList.add('active');
    selectedDays = +b.dataset.days;
  });
});

function css(v) { return getComputedStyle(document.body).getPropertyValue(v).trim(); }

function initChartTheme() {
  if (!btChart) return;
  const light = document.body.classList.contains('light');
  btChart.applyOptions({
    layout: { background: { color: light ? '#ffffff' : '#0f1413' }, textColor: css('--text') },
    grid: { vertLines: { color: css('--border') }, horzLines: { color: css('--border') } },
    rightPriceScale: { borderColor: css('--border-strong') },
    timeScale: { borderColor: css('--border-strong') },
  });
}

function ensureChart() {
  if (btChart) return;
  const el = document.getElementById('btChart');
  btChart = LightweightCharts.createChart(el, {
    width: el.clientWidth, height: el.clientHeight,
    layout: { background: { color: '#0f1413' }, textColor: '#d4dcd9', fontFamily: 'JetBrains Mono, monospace' },
    grid: { vertLines: { color: '#1f2a28' }, horzLines: { color: '#1f2a28' } },
    rightPriceScale: { borderColor: '#2a3a37' },
    timeScale: { borderColor: '#2a3a37', timeVisible: false, secondsVisible: false },
    crosshair: { mode: 1 },
  });
  priceSeries = btChart.addLineSeries({ color: '#6df5d4', lineWidth: 2, priceLineVisible: false, lastValueVisible: true });
  const ema = btChart.addLineSeries({ color: '#ffb83d', lineWidth: 1, priceLineVisible: false, lastValueVisible: false });
  btChart._emaSeries = ema;
  window.addEventListener('resize', () => { if (btChart) btChart.applyOptions({ width: el.clientWidth }); });
  initChartTheme();
}

function fmtTime(t) {
  const d = new Date(t * 1000);
  return d.toLocaleDateString('tr-TR', { day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit' });
}
function pnlCell(v) {
  if (v == null) return '<span class="pnl-dim">--</span>';
  const cls = v >= 0 ? 'pnl-pos' : 'pnl-neg';
  return `<span class="${cls}">${v>=0?'+':''}${v}%</span>`;
}
function wrCell(info, field) {
  if (!info) return '<span class="wr-dim">--</span>';
  const v = info[field];
  if (field === 'winRate') {
    const cls = v >= 50 ? 'wr-pos' : 'wr-neg';
    return `<span class="${cls}">${v}%</span>`;
  }
  return v;
}

async function runBacktest() {
  const btn = document.getElementById('runBtn');
  const sym = (document.getElementById('symbolInput').value || 'BTC').toUpperCase().replace(/[^A-Z0-9]/g,'');
  btn.disabled = true; btn.textContent = 'CALISIYOR... (~15sn)';
  const status = document.getElementById('statusBar');
  status.className = 'status-bar';
  status.innerHTML = `<b>${sym}</b> icin son <b>${selectedDays} gun</b> backtest calisiyor... Binance'den veri cekiliyor, sabret.`;

  try {
    const r = await fetch(`/api/backtest?symbol=${encodeURIComponent(sym)}&days=${selectedDays}`);
    const data = await r.json();
    if (!data.ok) throw new Error(data.error || 'Bilinmeyen hata');
    renderResults(data);
    status.innerHTML = `<b>${sym}</b> &middot; ${data.barsCovered} bar tarandi &middot; ${data.summary.totalTrades} trade${data.cached ? ' &middot; (cache)' : ''}`;
  } catch (e) {
    status.className = 'status-bar error';
    status.textContent = 'Hata: ' + e.message;
  } finally {
    btn.disabled = false; btn.textContent = 'BACKTEST CALISTIR >';
  }
}

function renderResults(data) {
  const s = data.summary;
  const all = s.all, lng = s.long, sht = s.short, op = s.openTrade;
  // Ozet kartlar - GERCEK TRADE sonuclari
  const sg = document.getElementById('summaryGrid');
  const totalCls = all && all.totalPnl >= 0 ? 'pos' : 'neg';
  sg.innerHTML = `
    <div class="stat"><div class="stat-label">Toplam Trade</div><div class="stat-value neutral">${s.totalTrades}</div><div class="stat-sub">${data.days} gun / ${data.barsCovered} bar</div></div>
    <div class="stat"><div class="stat-label">Toplam Getiri</div><div class="stat-value ${totalCls}">${all?(all.totalPnl>=0?'+':'')+all.totalPnl+'%':'--'}</div><div class="stat-sub">${all?'kume kar/zarar':'trade yok'}</div></div>
    <div class="stat"><div class="stat-label">Win Rate</div><div class="stat-value ${all&&all.winRate>=50?'pos':'neg'}">${all?all.winRate+'%':'--'}</div><div class="stat-sub">${all?all.wins+'W / '+all.losses+'L':'--'}</div></div>
    <div class="stat"><div class="stat-label">Ort. Trade</div><div class="stat-value ${all&&all.avgPnl>=0?'pos':'neg'}">${all?(all.avgPnl>=0?'+':'')+all.avgPnl+'%':'--'}</div><div class="stat-sub">${all?'ort '+all.avgHours+'sa tutuldu':'--'}</div></div>
    <div class="stat"><div class="stat-label">En Iyi / Kotu</div><div class="stat-value neutral" style="font-size:14px">${all?'<span class="wr-pos">+'+all.bestPnl+'%</span> / <span class="wr-neg">'+all.worstPnl+'%</span>':'--'}</div><div class="stat-sub">tek trade</div></div>
  `;

  // Long/Short kirilim tablosu
  const wb = document.getElementById('wrBody');
  function trow(label, cls, st) {
    if (!st) return `<tr><td class="${cls}">${label}</td><td colspan="5" class="wr-dim">trade yok</td></tr>`;
    return `<tr>
      <td class="${cls}">${label}</td>
      <td>${st.winRate>=50?'<span class="wr-pos">':'<span class="wr-neg">'}${st.winRate}%</span></td>
      <td>${st.n} (${st.wins}W/${st.losses}L)</td>
      <td>${st.totalPnl>=0?'<span class="wr-pos">+':'<span class="wr-neg">'}${st.totalPnl}%</span></td>
      <td>${st.avgPnl>=0?'<span class="wr-pos">+':'<span class="wr-neg">'}${st.avgPnl}%</span></td>
      <td>${st.avgHours}sa</td>
    </tr>`;
  }
  wb.innerHTML = trow('TUM TRADELER','', all) + trow('LONG','long', lng) + trow('SHORT','short', sht);

  // Grafik
  ensureChart();
  const priceData = data.regimeSeries.map(p => ({ time: p.t, value: p.price }));
  const emaData = data.regimeSeries.filter(p => p.ema365).map(p => ({ time: p.t, value: p.ema365 }));
  priceSeries.setData(priceData);
  btChart._emaSeries.setData(emaData);

  // Marker: giris (ok) + cikis (daire)
  const markers = (data.signals||[]).map(sig => {
    if (sig.type === 'entry') {
      if (sig.dir === 'long') return { time: sig.ts, position:'belowBar', color:'#00d09c', shape:'arrowUp', text:'AL' };
      return { time: sig.ts, position:'aboveBar', color:'#ff4d6d', shape:'arrowDown', text:'SAT' };
    } else {
      return { time: sig.ts, position: sig.dir==='long'?'aboveBar':'belowBar', color:'#ffb83d', shape:'circle', text:'X' };
    }
  }).filter(m=>m).sort((a,b)=>a.time-b.time);
  priceSeries.setMarkers(markers);
  btChart.timeScale().fitContent();

  // Trade listesi
  const sl = document.getElementById('sigList');
  const trades = (data.trades||[]).slice();
  if (op) trades.push(op);
  if (!trades.length) { sl.innerHTML = '<div class="empty">Bu donemde tamamlanmis trade yok</div>'; return; }
  const head = `<div class="sig-head"><span>GIRIS</span><span>YON</span><span>GIRIS\u2192CIKIS</span><span class="h-regime">REJIM</span><span class="h-1h">SURE</span><span>PnL</span></div>`;
  const rows = trades.slice().reverse().map(t => {
    const dcls = t.dir==='long' ? 'a-long' : 'a-short';
    const dlabel = t.dir==='long' ? 'LONG' : 'SHORT';
    const rcls = t.entryRegime==='BOGA'?'r-boga':(t.entryRegime==='AYI'?'r-ayi':'');
    const pcls = t.pnl>=0 ? 'pnl-pos' : 'pnl-neg';
    const openTag = t.open ? ' <span style="color:var(--amber)">(ACIK)</span>' : '';
    return `<div class="sig-row">
      <span>${fmtTime(t.entryTs)}</span>
      <span class="${dcls}">${dlabel}${openTag}</span>
      <span style="font-size:9px">${t.entryPrice}\u2192${t.exitPrice}</span>
      <span class="c-regime ${rcls}">${t.entryRegime||'--'}</span>
      <span class="c-1h">${t.hours}sa</span>
      <span class="${pcls}">${t.pnl>=0?'+':''}${t.pnl}%</span>
    </div>`;
  }).join('');
  sl.innerHTML = head + rows;
}

document.getElementById('runBtn').addEventListener('click', runBacktest);
loadTheme();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()