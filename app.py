"""
L/S RATIO TERMINAL - Decision Engine v3
========================================
Multi-TF (15m/1h/4h/1d), whale-retail delta, true retail,
karar motoru (LONG AC/TUT, SHORT AC/TUT, FLAT, KAPAT), pozisyon takibi.
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
USER_AGENT = "Mozilla/5.0 LSDecisionEngine/6.0"

# ============= SUPABASE KONFIG =============
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
SUPABASE_ENABLED = bool(SUPABASE_URL and SUPABASE_KEY)

TIMEFRAMES = ["15m", "1h", "4h"]
# Swing trader profili: 15m timing, 1h entry/exit, 4h trend takibi
# 1d kullanici tarafindan manuel/gozle takip ediliyor, sistemde yok
TF_WEIGHTS = {"15m": 0.5, "1h": 2.0, "4h": 2.5}
# Toplam: 5.0

THRESHOLDS = {
    # Karar esikleri (yeni toplam agirlik 5.0 + killshot ±2)
    "long_open":   4.5,
    "long_hold":   1.8,
    "short_hold": -1.8,
    "short_open": -4.5,
    # Whale-retail delta (kullanici kurali: ±3)
    "whale_long":   3.0,
    "whale_short": -3.0,
    # Killshot: ekstrem divergence + retail trendi
    "killshot_delta_long":   3.0,
    "killshot_delta_short": -3.0,
    "killshot_retail_trend": 0.3,           # 4h icin
    "killshot_retail_trend_1h": 0.15,       # 1h icin daha hassas
    "killshot_retail_trend_15m": 0.05,      # 15m icin cok hassas
    # Retail asiri pozisyon
    "retail_extreme_long":  65.0,
    "retail_extreme_short": 35.0,
    # Funding
    "funding_extreme_pos":  0.015,
    "funding_extreme_neg": -0.015,
    # OI ve fiyat
    "oi_delta_strong":   0.5,
    "price_delta_strong": 0.3,
    # CVD
    "cvd_buy_strong":  57.0,
    "cvd_sell_strong": 43.0,
    # Pozisyon tavsiye esikleri
    "counter_weak":     1.8,
    "counter_medium":   2.7,
    "counter_strong":   4.5,
    "same_strong":      4.5,
    # EMA agirliklari
    "ema_regime_aligned":   1.0,   # 4h rejime uygun sinyal +1 (yumusak)
    "ema_regime_against":  -1.0,   # rejime ters sinyal -1
    "ema_cross_fresh":      2.0,   # taze 1h EMA7/30 kesisimi (killshot gibi guclu)
    "ema_cross_state":      0.5,   # surekli yon biasi (EMA7>EMA30 ise hep +0.5)
}


def http_get(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw)


def safe(fn, default=None):
    try:
        return fn()
    except Exception:
        return default


# ======================================================================
# SUPABASE HELPERS
# ======================================================================

def supabase_request(method, path, body=None):
    """Supabase REST API'ye istek atar. Hata olursa None."""
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
            if raw:
                return json.loads(raw)
            return []
    except urllib.error.HTTPError as e:
        sys.stderr.write(f"Supabase {method} {path}: HTTP {e.code}\n")
        return None
    except Exception as e:
        sys.stderr.write(f"Supabase {method} {path}: {e}\n")
        return None


def save_signal_if_meaningful(symbol, decision, current_price, tf_results):
    """Anlamli sinyalleri DB'ye kaydeder. Ayni sinyal kisa sure icinde tekrar gelirse kaydetmez.
    Returns: True kaydedildi, False atlandi."""
    if not SUPABASE_ENABLED:
        return False

    # Kayit edilecek aksiyonlar (gurultu olanlari atla)
    saveable = {
        "LONG_OPEN", "SHORT_OPEN",
        "CLOSE", "CLOSE_TP", "CLOSE_SL",
        "PARTIAL_CLOSE", "TIGHTEN_STOP",
    }
    if decision.get("action") not in saveable:
        return False

    # Histerezis + dedup: ayni yon sinyali 45dk, herhangi bir sinyal 10dk icinde tekrar yazma
    try:
        latest = supabase_request("GET", f"signals?symbol=eq.{symbol}&order=ts.desc&limit=1")
        if latest and len(latest) > 0:
            last = latest[0]
            from datetime import datetime, timezone, timedelta
            try:
                last_ts = datetime.fromisoformat(last.get("ts", "").replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                elapsed = now - last_ts
                last_action = last.get("action", "")
                new_action = decision.get("action", "")

                # Ayni aksiyon -> 45 dakika dedup
                if last_action == new_action and elapsed < timedelta(minutes=45):
                    return False

                # Ayni "yon" (ikisi de acilis veya ikisi de kapat tipi) -> 10dk minimum
                open_actions = {"LONG_OPEN", "SHORT_OPEN"}
                close_actions = {"CLOSE", "CLOSE_TP", "CLOSE_SL", "PARTIAL_CLOSE", "TIGHTEN_STOP"}
                same_family = (
                    (last_action in open_actions and new_action in open_actions) or
                    (last_action in close_actions and new_action in close_actions)
                )
                if same_family and elapsed < timedelta(minutes=10):
                    return False
            except Exception:
                pass
    except Exception:
        pass

    # Killshot skorlarini cikar
    killshot = {"15m": 0, "1h": 0, "4h": 0}
    for tf, d in tf_results.items():
        if d and d.get("ok"):
            v = compute_tf_verdict(d, tf)
            if v:
                killshot[tf] = v["scores"].get("killshot", 0)

    body = {
        "symbol": symbol,
        "action": decision.get("action"),
        "action_label": decision.get("actionLabel"),
        "total_score": round(decision.get("totalScore", 0), 2),
        "h1_verdict": decision.get("h1Verdict"),
        "killshot_15m": killshot.get("15m", 0),
        "killshot_1h": killshot.get("1h", 0),
        "killshot_4h": killshot.get("4h", 0),
        "price": current_price,
        "reasons": " | ".join((decision.get("reasons") or [])[:5])[:1000],
    }
    result = supabase_request("POST", "signals", body)
    return result is not None


def get_signal_history(symbol, limit=50):
    if not SUPABASE_ENABLED:
        return []
    try:
        result = supabase_request("GET", f"signals?symbol=eq.{symbol}&order=ts.desc&limit={limit}")
        return result or []
    except Exception:
        return []


# ======================================================================
# BACKTEST MOTORU
# ======================================================================

def fetch_historical_series(sym, period, limit=500):
    """Bir TF icin tum gerekli tarihi veriyi tek seferde ceker.
    Returns: dict, her metrigin zaman serisi
    """
    period_map_kline = {"15m":"15m","1h":"1h","4h":"4h"}
    period_binance = {"15m":"15m","1h":"1h","4h":"4h"}

    p = period_binance.get(period, "1h")

    # Tum endpoint'leri paralel cek
    with ThreadPoolExecutor(max_workers=5) as pool:
        f_global = pool.submit(safe, lambda: http_get(
            f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
            f"?symbol={sym}&period={p}&limit={limit}"), [])
        f_top = pool.submit(safe, lambda: http_get(
            f"https://fapi.binance.com/futures/data/topLongShortPositionRatio"
            f"?symbol={sym}&period={p}&limit={limit}"), [])
        f_oi = pool.submit(safe, lambda: http_get(
            f"https://fapi.binance.com/futures/data/openInterestHist"
            f"?symbol={sym}&period={p}&limit={limit}"), [])
        f_kline = pool.submit(safe, lambda: http_get(
            f"https://fapi.binance.com/fapi/v1/klines"
            f"?symbol={sym}&interval={period_map_kline.get(period, '1h')}&limit={limit}"), [])
        f_cvd = pool.submit(safe, lambda: http_get(
            f"https://fapi.binance.com/futures/data/takerlongshortRatio"
            f"?symbol={sym}&period={p}&limit={limit}"), [])

        global_data = f_global.result() or []
        top_data = f_top.result() or []
        oi_data = f_oi.result() or []
        kline_data = f_kline.result() or []
        cvd_data = f_cvd.result() or []

    # Funding rate (tarihi)
    funding_data = safe(lambda: http_get(
        f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={sym}&limit={min(limit, 1000)}"), [])

    # Timestamp index olustur (kline'in kapanis zamanini referans alacagiz)
    # Her endpoint'in timestamp'ini bir dict'e koy: ts -> deger
    def to_map_global(arr):
        return {int(d["timestamp"]): {"long": float(d["longAccount"]) * 100,
                                       "short": float(d["shortAccount"]) * 100}
                for d in arr if "timestamp" in d}
    def to_map_top(arr):
        return {int(d["timestamp"]): float(d["longAccount"]) * 100
                for d in arr if "timestamp" in d}
    def to_map_oi(arr):
        return {int(d["timestamp"]): float(d.get("sumOpenInterestValue") or 0)
                for d in arr if "timestamp" in d}
    def to_map_cvd(arr):
        result = {}
        for d in arr:
            if "timestamp" not in d: continue
            buy = float(d.get("buyVol") or 0)
            sell = float(d.get("sellVol") or 0)
            tot = buy + sell
            if tot > 0:
                result[int(d["timestamp"])] = buy / tot * 100
        return result

    global_map = to_map_global(global_data)
    top_map = to_map_top(top_data)
    oi_map = to_map_oi(oi_data)
    cvd_map = to_map_cvd(cvd_data)

    # Kline timestamp olarak kullanacagiz (referans serisi)
    # Her kline: [openTime, open, high, low, close, volume, closeTime, ...]
    klines = []
    for k in kline_data:
        klines.append({
            "ts": int(k[0]),
            "close_ts": int(k[6]),
            "close": float(k[4]),
        })

    # Funding rate'i kline timestamp'ine en yakin esleme
    funding_sorted = sorted(funding_data, key=lambda x: int(x.get("fundingTime", 0)))

    def nearest_funding(ts):
        if not funding_sorted: return None
        # En yakini bul (basit linear scan, kucuk veride sorun degil)
        best = None
        for f in funding_sorted:
            ft = int(f.get("fundingTime", 0))
            if ft <= ts:
                best = f
            else:
                break
        if best:
            return float(best.get("fundingRate", 0)) * 100
        return None

    # Her kline icin tum metrikleri birlestir
    series = []
    for i, k in enumerate(klines):
        ts = k["ts"]
        # Bu timestamp'a en yakin global/top/oi/cvd verisi
        # (Binance bu endpoint'leri kline ile ayni timestamp'a hizalar)
        global_v = global_map.get(ts)
        if not global_v:
            continue  # Bu nokta icin global veri yok, atla

        top_v = top_map.get(ts)
        oi_v = oi_map.get(ts)
        cvd_v = cvd_map.get(ts)

        # Bir onceki nokta (trend icin)
        prev_global = global_map.get(klines[i-1]["ts"]) if i > 0 else None
        prev_top = top_map.get(klines[i-1]["ts"]) if i > 0 else None
        prev_oi = oi_map.get(klines[i-1]["ts"]) if i > 0 else None
        prev_close = klines[i-1]["close"] if i > 0 else None

        # Whale, retail hesabi
        whale = top_v
        whale_prev = prev_top
        retail = None
        retail_prev = None
        if whale is not None:
            retail = (global_v["long"] - whale * 0.2) / 0.8
            retail = max(0, min(100, retail))
        if whale_prev is not None and prev_global:
            retail_prev = (prev_global["long"] - whale_prev * 0.2) / 0.8
            retail_prev = max(0, min(100, retail_prev))

        # Degisimler
        price_change = None
        if prev_close and prev_close > 0:
            price_change = (k["close"] - prev_close) / prev_close * 100
        oi_change = None
        if prev_oi and prev_oi > 0 and oi_v:
            oi_change = (oi_v - prev_oi) / prev_oi * 100

        series.append({
            "ts": ts,
            "ok": True,
            "longPct": global_v["long"],
            "shortPct": global_v["short"],
            "whaleLongPct": whale,
            "retailLongPct": retail,
            "whaleLongPctPrev": whale_prev,
            "retailLongPctPrev": retail_prev,
            "priceNow": k["close"],
            "priceChangePct": price_change,
            "oiNow": oi_v,
            "oiChangePct": oi_change,
            "fundingRate": nearest_funding(ts),
            "takerBuyPct": cvd_v,
        })

    return series


def run_backtest(symbol, days=7):
    """Gecmis N gun icin sistemin verecegi sinyalleri hesapla.

    BTC icin 3 TF (15m, 1h, 4h) tarihi veri cekilir, her 1h ana adim icin
    multi-TF karar verilir. Sonuc: zaman serisi sinyaller + onlardan sonraki PnL.
    """
    sym = symbol.upper().replace("USDT", "") + "USDT"

    # Limit hesabi:
    # 7 gun = 168 saat = 168 nokta 1h, 42 nokta 4h, 672 nokta 15m
    # Binance API limiti genelde 500, daha uzun icin pagination gerekir.
    # Simdilik 7 gun limit, 1h = 168, 4h = 42, 15m = 672 (limit 500'e cap)
    limit_1h = min(days * 24, 500)
    limit_4h = min(days * 6, 500)
    limit_15m = min(days * 96, 500)

    # 3 TF'i paralel cek (her TF zaten icinde paralel istek atiyor)
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_15m = pool.submit(fetch_historical_series, sym, "15m", limit_15m)
        f_1h = pool.submit(fetch_historical_series, sym, "1h", limit_1h)
        f_4h = pool.submit(fetch_historical_series, sym, "4h", limit_4h)
        series_15m = f_15m.result() or []
        series_1h = f_1h.result() or []
        series_4h = f_4h.result() or []

    if not series_1h:
        return {"ok": False, "error": "1h verisi alinamadi"}

    # 15m ve 4h serilerini timestamp'a gore index'le
    map_15m = {s["ts"]: s for s in series_15m}
    map_4h_sorted = sorted(series_4h, key=lambda s: s["ts"])

    # --- EMA hesaplari (backtest icin tarihi) ---
    # 4h EMA50 rejim
    sorted_4h = sorted(series_4h, key=lambda s: s["ts"])
    closes_4h = [s["priceNow"] for s in sorted_4h]
    ema50_4h_vals = calc_ema(closes_4h, 50)
    ema50_4h_map = {sorted_4h[i]["ts"]: ema50_4h_vals[i] for i in range(len(sorted_4h))}

    # 1h EMA7/30 cross
    sorted_1h = sorted(series_1h, key=lambda s: s["ts"])
    closes_1h_list = [s["priceNow"] for s in sorted_1h]
    ema7_vals = calc_ema(closes_1h_list, 7)
    ema30_vals = calc_ema(closes_1h_list, 30)
    ema7_map = {sorted_1h[i]["ts"]: ema7_vals[i] for i in range(len(sorted_1h))}
    ema30_map = {sorted_1h[i]["ts"]: ema30_vals[i] for i in range(len(sorted_1h))}
    # 1h timestamp -> index (taze kesisim icin onceki mumlara bakmak lazim)
    h1_ts_to_idx = {sorted_1h[i]["ts"]: i for i in range(len(sorted_1h))}

    def ema_at(ts):
        """Belirli bir 1h timestamp icin EMA durumu hesapla."""
        result = {"regime": None, "regimeDistance": None,
                  "crossState": None, "freshCross": None}
        # 4h rejim: bu ts'den onceki en yakin 4h noktasi
        best_4h_ts = None
        for s in map_4h_sorted:
            if s["ts"] <= ts:
                best_4h_ts = s["ts"]
            else:
                break
        if best_4h_ts and ema50_4h_map.get(best_4h_ts):
            price_4h = next((s["priceNow"] for s in map_4h_sorted if s["ts"] == best_4h_ts), None)
            ema_v = ema50_4h_map[best_4h_ts]
            if price_4h and ema_v:
                result["regime"] = "BULL" if price_4h > ema_v else "BEAR"
                result["regimeDistance"] = round((price_4h - ema_v) / ema_v * 100, 2)
        # 1h cross
        idx = h1_ts_to_idx.get(ts)
        if idx is not None and ema7_vals[idx] and ema30_vals[idx]:
            now_above = ema7_vals[idx] > ema30_vals[idx]
            result["crossState"] = "BULL" if now_above else "BEAR"
            for back in (1, 2):
                pi = idx - back
                if pi >= 0 and ema7_vals[pi] and ema30_vals[pi]:
                    prev_above = ema7_vals[pi] > ema30_vals[pi]
                    if now_above and not prev_above:
                        result["freshCross"] = "BULL"; break
                    elif not now_above and prev_above:
                        result["freshCross"] = "BEAR"; break
        return result

    def nearest_15m(ts):
        candidates = [t for t in [ts, ts - 15*60*1000, ts - 30*60*1000, ts - 45*60*1000] if t in map_15m]
        return map_15m[candidates[0]] if candidates else None

    def nearest_4h(ts):
        best = None
        for s in map_4h_sorted:
            if s["ts"] <= ts:
                best = s
            else:
                break
        return best

    # Her 1h noktasi icin karar ver
    signals = []
    for h1 in series_1h:
        tf_results = {
            "15m": nearest_15m(h1["ts"]),
            "1h": h1,
            "4h": nearest_4h(h1["ts"]),
        }
        # None olanlari at
        tf_results = {k: v for k, v in tf_results.items() if v and v.get("ok")}
        if "1h" not in tf_results:
            continue

        decision = decide(symbol, tf_results, "flat", None, h1["priceNow"], ema_at(h1["ts"]))

        # Backtest FLAT giris simulasyonu - sadece acilis sinyalleri
        saveable = {"LONG_OPEN", "SHORT_OPEN"}
        if decision.get("action") not in saveable:
            continue

        signals.append({
            "ts": h1["ts"],
            "action": decision.get("action"),
            "actionLabel": decision.get("actionLabel"),
            "totalScore": round(decision.get("totalScore", 0), 2),
            "h1Verdict": decision.get("h1Verdict"),
            "price": h1["priceNow"],
        })

    # Her sinyal icin sonraki 1h, 4h, 24h fiyatini bul (PnL hesabi)
    one_hour_ms = 60 * 60 * 1000
    for sig in signals:
        sig_ts = sig["ts"]
        sig_price = sig["price"]
        for label, hours in [("pnl1h", 1), ("pnl4h", 4), ("pnl24h", 24)]:
            target_ts = sig_ts + hours * one_hour_ms
            future = None
            for h1 in series_1h:
                if h1["ts"] >= target_ts:
                    future = h1
                    break
            if future and sig_price:
                future_price = future["priceNow"]
                if sig["action"] == "LONG_OPEN":
                    pnl = (future_price - sig_price) / sig_price * 100
                elif sig["action"] == "SHORT_OPEN":
                    pnl = (sig_price - future_price) / sig_price * 100
                else:
                    pnl = None
                sig[label] = round(pnl, 2) if pnl is not None else None
            else:
                sig[label] = None

    # Ozet istatistikler
    long_signals = [s for s in signals if s["action"] == "LONG_OPEN"]
    short_signals = [s for s in signals if s["action"] == "SHORT_OPEN"]

    def win_rate(sigs, key):
        valid = [s for s in sigs if s.get(key) is not None]
        if not valid: return None
        wins = sum(1 for s in valid if s[key] > 0)
        return {"total": len(valid), "wins": wins,
                "winRate": round(wins / len(valid) * 100, 1),
                "avgPnl": round(sum(s[key] for s in valid) / len(valid), 2)}

    summary = {
        "totalSignals": len(signals),
        "longOpen": len(long_signals),
        "shortOpen": len(short_signals),
        "long_1h": win_rate(long_signals, "pnl1h"),
        "long_4h": win_rate(long_signals, "pnl4h"),
        "long_24h": win_rate(long_signals, "pnl24h"),
        "short_1h": win_rate(short_signals, "pnl1h"),
        "short_4h": win_rate(short_signals, "pnl4h"),
        "short_24h": win_rate(short_signals, "pnl24h"),
    }

    # Mum verisi de gonderelim (LightweightCharts icin)
    candles = [{
        "time": int(s["ts"] / 1000),  # saniye
        "close": s["priceNow"],
    } for s in series_1h]

    return {
        "ok": True,
        "symbol": symbol,
        "days": days,
        "signals": signals,
        "candles": candles,
        "summary": summary,
        "dataPoints1h": len(series_1h),
    }


# ======================================================================
# EMA HESAPLAMA
# ======================================================================

def calc_ema(closes, period):
    """Kapanis listesi (eski->yeni) icin EMA serisi. Ilk period-1 None."""
    if not closes or len(closes) < period:
        return [None] * len(closes)
    ema = [None] * len(closes)
    sma = sum(closes[:period]) / period
    ema[period - 1] = sma
    mult = 2 / (period + 1)
    for i in range(period, len(closes)):
        ema[i] = (closes[i] - ema[i-1]) * mult + ema[i-1]
    return ema


def fetch_ema_signals(sym):
    """4h EMA50 rejim + 1h EMA7/30 cross."""
    result = {"regime": None, "regimeDistance": None,
              "crossState": None, "freshCross": None,
              "ema7": None, "ema30": None, "ema50_4h": None}

    # 4h EMA50 rejim
    kl_4h = safe(lambda: http_get(
        f"https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=4h&limit=120"), [])
    if kl_4h and len(kl_4h) >= 50:
        closes_4h = [float(k[4]) for k in kl_4h]
        ema50 = calc_ema(closes_4h, 50)
        last_price = closes_4h[-1]
        if ema50[-1]:
            result["regime"] = "BULL" if last_price > ema50[-1] else "BEAR"
            result["regimeDistance"] = round((last_price - ema50[-1]) / ema50[-1] * 100, 2)
            result["ema50_4h"] = round(ema50[-1], 2)

    # 1h EMA7/30 cross
    kl_1h = safe(lambda: http_get(
        f"https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval=1h&limit=120"), [])
    if kl_1h and len(kl_1h) >= 30:
        closes_1h = [float(k[4]) for k in kl_1h]
        ema7 = calc_ema(closes_1h, 7)
        ema30 = calc_ema(closes_1h, 30)
        if ema7[-1] and ema30[-1]:
            result["crossState"] = "BULL" if ema7[-1] > ema30[-1] else "BEAR"
            result["ema7"] = round(ema7[-1], 2)
            result["ema30"] = round(ema30[-1], 2)
            # Taze kesisim (son 3 mum toleransli)
            now_above = ema7[-1] > ema30[-1]
            for back in (2, 3):
                if ema7[-back] and ema30[-back]:
                    prev_above = ema7[-back] > ema30[-back]
                    if now_above and not prev_above:
                        result["freshCross"] = "BULL"; break
                    elif not now_above and prev_above:
                        result["freshCross"] = "BEAR"; break

    return result


def binance_tf_data(sym, tf):
    period_map = {"15m":"15m","1h":"1h","4h":"4h","1d":"1d"}
    p = period_map.get(tf, "1h")

    url1 = f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={sym}&period={p}&limit=2"
    d1 = http_get(url1)
    if not d1 or len(d1) == 0:
        raise RuntimeError("NO DATA")
    last1 = d1[-1]
    long_pct = float(last1["longAccount"]) * 100
    short_pct = float(last1["shortAccount"]) * 100
    # Bir oncekini de al (trend hesabi icin)
    long_pct_prev = float(d1[-2]["longAccount"]) * 100 if len(d1) >= 2 else None

    # Top trader position ratio - bir onceki de lazim (trend icin)
    url2 = f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={sym}&period={p}&limit=2"
    d2 = safe(lambda: http_get(url2), [])
    whale_long_pct = None
    whale_long_pct_prev = None
    if d2 and len(d2) > 0:
        whale_long_pct = float(d2[-1]["longAccount"]) * 100
        if len(d2) >= 2:
            whale_long_pct_prev = float(d2[-2]["longAccount"]) * 100

    retail_long_pct = None
    retail_long_pct_prev = None
    if whale_long_pct is not None:
        retail_long_pct = (long_pct - whale_long_pct * 0.2) / 0.8
        retail_long_pct = max(0, min(100, retail_long_pct))
    if whale_long_pct_prev is not None and long_pct_prev is not None:
        retail_long_pct_prev = (long_pct_prev - whale_long_pct_prev * 0.2) / 0.8
        retail_long_pct_prev = max(0, min(100, retail_long_pct_prev))

    oi_url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={sym}&period={p}&limit=2"
    oi_data = safe(lambda: http_get(oi_url), [])
    oi_now = None
    oi_change_pct = None
    if oi_data and len(oi_data) >= 1:
        oi_now = float(oi_data[-1].get("sumOpenInterestValue") or 0)
        if len(oi_data) >= 2:
            oi_prev = float(oi_data[-2].get("sumOpenInterestValue") or 0)
            if oi_prev > 0:
                oi_change_pct = (oi_now - oi_prev) / oi_prev * 100

    kline_url = f"https://fapi.binance.com/fapi/v1/klines?symbol={sym}&interval={p}&limit=2"
    kline_data = safe(lambda: http_get(kline_url), [])
    price_now = None
    price_change_pct = None
    if kline_data and len(kline_data) >= 1:
        price_now = float(kline_data[-1][4])
        if len(kline_data) >= 2:
            price_prev = float(kline_data[-2][4])
            if price_prev > 0:
                price_change_pct = (price_now - price_prev) / price_prev * 100

    funding = None
    try:
        pj = http_get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym}")
        if pj.get("lastFundingRate"):
            funding = float(pj["lastFundingRate"]) * 100
    except Exception:
        pass

    # 6) CVD / Taker Buy-Sell Ratio - SON 3 NOKTA ORTALAMASI (gurultu azaltma)
    taker_buy_pct = None
    cvd_url = f"https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={sym}&period={p}&limit=3"
    cvd_data = safe(lambda: http_get(cvd_url), [])
    if cvd_data and len(cvd_data) >= 1:
        try:
            pcts = []
            for point in cvd_data:  # son 3 nokta
                buy_vol = float(point.get("buyVol") or 0)
                sell_vol = float(point.get("sellVol") or 0)
                tot = buy_vol + sell_vol
                if tot > 0:
                    pcts.append(buy_vol / tot * 100)
            if pcts:
                # Agirlikli ortalama: en son nokta daha onemli (0.5, 0.3, 0.2)
                if len(pcts) >= 3:
                    taker_buy_pct = pcts[-1] * 0.5 + pcts[-2] * 0.3 + pcts[-3] * 0.2
                elif len(pcts) == 2:
                    taker_buy_pct = pcts[-1] * 0.6 + pcts[-2] * 0.4
                else:
                    taker_buy_pct = pcts[-1]
        except Exception:
            pass

    return {
        "ok": True,
        "longPct": long_pct, "shortPct": short_pct,
        "whaleLongPct": whale_long_pct, "retailLongPct": retail_long_pct,
        "whaleLongPctPrev": whale_long_pct_prev,
        "retailLongPctPrev": retail_long_pct_prev,
        "priceNow": price_now, "priceChangePct": price_change_pct,
        "oiNow": oi_now, "oiChangePct": oi_change_pct,
        "fundingRate": funding,
        "takerBuyPct": taker_buy_pct,
    }


def bybit_summary(sym):
    try:
        url = f"https://api.bybit.com/v5/market/account-ratio?category=linear&symbol={sym}&period=1h&limit=1"
        j = http_get(url)
        if j.get("retCode") != 0:
            return {"ok": False, "error": j.get("retMsg")}
        lst = (j.get("result") or {}).get("list") or []
        if not lst:
            return {"ok": False, "error": "NO DATA"}
        last = lst[0]
        long_pct = float(last["buyRatio"]) * 100
        short_pct = float(last["sellRatio"]) * 100
    except Exception as e:
        return {"ok": False, "error": str(e)}
    oi, funding = None, None
    try:
        t = http_get(f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={sym}")
        if t.get("retCode") == 0:
            lst2 = (t.get("result") or {}).get("list") or []
            if lst2:
                tk = lst2[0]
                if tk.get("openInterestValue"):
                    oi = float(tk["openInterestValue"])
                elif tk.get("openInterest") and tk.get("lastPrice"):
                    oi = float(tk["openInterest"]) * float(tk["lastPrice"])
                if tk.get("fundingRate"):
                    funding = float(tk["fundingRate"]) * 100
    except Exception:
        pass
    return {"ok": True, "longPct": long_pct, "shortPct": short_pct,
            "whaleLongPct": None, "retailLongPct": None,
            "openInterest": oi, "fundingRate": funding}


def okx_summary(symbol):
    ccy = symbol.replace("USDT", "").replace("-USDT-SWAP", "")
    inst_id = f"{ccy}-USDT-SWAP"
    try:
        url = f"https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy={ccy}&period=1H&limit=1"
        j = http_get(url)
        if j.get("code") != "0":
            return {"ok": False, "error": j.get("msg") or "API ERROR"}
        arr = j.get("data") or []
        if not arr:
            return {"ok": False, "error": "NO DATA"}
        ratio = float(arr[0][1])
        long_pct = ratio / (1 + ratio) * 100
        short_pct = 100 - long_pct
    except Exception as e:
        return {"ok": False, "error": str(e)}
    oi, funding = None, None
    try:
        oj = http_get(f"https://www.okx.com/api/v5/public/open-interest?instId={inst_id}")
        if oj.get("code") == "0" and oj.get("data"):
            d = oj["data"][0]
            if d.get("oiUsd"):
                oi = float(d["oiUsd"])
            elif d.get("oiCcy"):
                pj = http_get(f"https://www.okx.com/api/v5/market/ticker?instId={inst_id}")
                if pj.get("code") == "0" and pj.get("data"):
                    oi = float(d["oiCcy"]) * float(pj["data"][0]["last"])
    except Exception:
        pass
    try:
        fj = http_get(f"https://www.okx.com/api/v5/public/funding-rate?instId={inst_id}")
        if fj.get("code") == "0" and fj.get("data"):
            funding = float(fj["data"][0]["fundingRate"]) * 100
    except Exception:
        pass
    return {"ok": True, "longPct": long_pct, "shortPct": short_pct,
            "whaleLongPct": None, "retailLongPct": None,
            "openInterest": oi, "fundingRate": funding}


def bitget_summary(sym):
    try:
        url = f"https://api.bitget.com/api/v2/mix/market/account-long-short?symbol={sym}&period=1h&productType=USDT-FUTURES&limit=1"
        j = http_get(url)
        if j.get("code") != "00000":
            return {"ok": False, "error": j.get("msg") or "API ERROR"}
        arr = j.get("data") or []
        if not arr:
            return {"ok": False, "error": "NO DATA"}
        last = arr[0]
        long_pct = float(last["longAccountRatio"]) * 100
        short_pct = float(last["shortAccountRatio"]) * 100
    except Exception as e:
        return {"ok": False, "error": str(e)}
    oi, funding = None, None
    try:
        oj = http_get(f"https://api.bitget.com/api/v2/mix/market/open-interest?symbol={sym}&productType=USDT-FUTURES")
        if oj.get("code") == "00000":
            data = oj.get("data") or {}
            ol = data.get("openInterestList") or []
            if ol:
                qty = float(ol[0].get("size") or 0)
                tj = http_get(f"https://api.bitget.com/api/v2/mix/market/ticker?symbol={sym}&productType=USDT-FUTURES")
                if tj.get("code") == "00000" and tj.get("data"):
                    tdata = tj["data"]
                    if isinstance(tdata, list) and tdata: tdata = tdata[0]
                    price = float(tdata.get("lastPr") or 0)
                    oi = qty * price if price else None
    except Exception:
        pass
    try:
        fj = http_get(f"https://api.bitget.com/api/v2/mix/market/current-fund-rate?symbol={sym}&productType=USDT-FUTURES")
        if fj.get("code") == "00000":
            d = fj.get("data") or []
            if isinstance(d, list) and d:
                funding = float(d[0].get("fundingRate") or 0) * 100
            elif isinstance(d, dict):
                funding = float(d.get("fundingRate") or 0) * 100
    except Exception:
        pass
    return {"ok": True, "longPct": long_pct, "shortPct": short_pct,
            "whaleLongPct": None, "retailLongPct": None,
            "openInterest": oi, "fundingRate": funding}


# DECISION ENGINE
def score_oi_price(d):
    oi = d.get("oiChangePct"); pr = d.get("priceChangePct")
    if oi is None or pr is None: return 0, None
    strong = abs(oi) >= THRESHOLDS["oi_delta_strong"] or abs(pr) >= THRESHOLDS["price_delta_strong"]
    if pr > 0 and oi > 0 and strong:
        return +1, f"Fiyat +{pr:.2f}% + OI +{oi:.2f}% = saglam yukselis"
    if pr < 0 and oi > 0 and strong:
        return -1, f"Fiyat {pr:.2f}% + OI +{oi:.2f}% = saglam dusus"
    if pr > 0 and oi < 0:
        return 0, f"Fiyat +{pr:.2f}% + OI {oi:.2f}% = zayif yukselis (squeeze sonu)"
    if pr < 0 and oi < 0:
        return 0, f"Fiyat {pr:.2f}% + OI {oi:.2f}% = zayif dusus (long kapaniyor)"
    return 0, None


def score_whale(d):
    whale = d.get("whaleLongPct"); retail = d.get("retailLongPct")
    if whale is None or retail is None: return 0, None
    delta = whale - retail
    if delta >= THRESHOLDS["whale_long"]:
        return +1, f"Whale long baskin (delta +{delta:.1f})"
    if delta <= THRESHOLDS["whale_short"]:
        return -1, f"Whale short baskin (delta {delta:.1f})"
    return 0, None


def score_retail(d):
    retail = d.get("retailLongPct")
    if retail is None: return 0, None
    if retail >= THRESHOLDS["retail_extreme_long"]:
        return -1, f"Retail asiri long (%{retail:.1f}) - counter sinyal"
    if retail <= THRESHOLDS["retail_extreme_short"]:
        return +1, f"Retail asiri short (%{retail:.1f}) - counter sinyal"
    return 0, None


def score_funding(d):
    fr = d.get("fundingRate")
    if fr is None: return 0, None
    if fr >= THRESHOLDS["funding_extreme_pos"]:
        return -1, f"Funding asiri pozitif ({fr:+.4f}%) - asiri boga"
    if fr <= THRESHOLDS["funding_extreme_neg"]:
        return +1, f"Funding asiri negatif ({fr:+.4f}%) - short squeeze fuel"
    return 0, None


def score_cvd(d):
    """CVD / Taker Buy-Sell Ratio skoru."""
    buy_pct = d.get("takerBuyPct")
    if buy_pct is None:
        return 0, None
    pr = d.get("priceChangePct")
    if buy_pct >= THRESHOLDS["cvd_buy_strong"]:
        # Saglam alim baskisi
        if pr is not None and pr > 0:
            return +1, f"CVD %{buy_pct:.1f} alim + fiyat artiyor = gercek alim onayi"
        elif pr is not None and pr < 0:
            return 0, f"CVD %{buy_pct:.1f} alim ama fiyat dusuyor = birikim olabilir"
        else:
            return +1, f"Taker alim baskisi (%{buy_pct:.1f})"
    if buy_pct <= THRESHOLDS["cvd_sell_strong"]:
        # Saglam satis baskisi
        if pr is not None and pr < 0:
            return -1, f"CVD %{buy_pct:.1f} satim + fiyat dusuyor = gercek satim onayi"
        elif pr is not None and pr > 0:
            return 0, f"CVD %{buy_pct:.1f} satim ama fiyat artiyor = sahte yukselis"
        else:
            return -1, f"Taker satim baskisi (%{buy_pct:.1f})"
    return 0, None


def score_killshot(d, tf=None):
    """Kullanicinin keskin kurali:
    Whale delta <= -3 + retail long yukseliyor = KESIN SHORT
    Whale delta >= +3 + retail short yukseliyor (long azaliyor) = KESIN LONG

    Bu kural tetiklendiginde +2 / -2 skor verir (normal ±1 yerine).
    TF bazli esik: 1h ve 15m daha hassas.
    """
    whale = d.get("whaleLongPct")
    retail = d.get("retailLongPct")
    retail_prev = d.get("retailLongPctPrev")

    if whale is None or retail is None:
        return 0, None

    delta = whale - retail

    if retail_prev is None:
        return 0, None

    retail_trend = retail - retail_prev

    # TF bazli esik secimi
    if tf == "1h":
        threshold = THRESHOLDS["killshot_retail_trend_1h"]
    elif tf == "15m":
        threshold = THRESHOLDS["killshot_retail_trend_15m"]
    else:
        threshold = THRESHOLDS["killshot_retail_trend"]

    # KESIN SHORT
    if delta <= THRESHOLDS["killshot_delta_short"] and retail_trend >= threshold:
        return -2, f"KESIN SHORT: Whale delta {delta:.1f} + retail %{retail:.1f} artiyor (+{retail_trend:.2f})"

    # KESIN LONG
    if delta >= THRESHOLDS["killshot_delta_long"] and retail_trend <= -threshold:
        return +2, f"KESIN LONG: Whale delta +{delta:.1f} + retail %{retail:.1f} azaliyor ({retail_trend:.2f})"

    return 0, None


def compute_tf_verdict(d, tf=None):
    if not d or not d.get("ok"): return None
    s_oi, r_oi = score_oi_price(d)
    s_w, r_w = score_whale(d)
    s_r, r_r = score_retail(d)
    s_f, r_f = score_funding(d)
    s_c, r_c = score_cvd(d)
    s_k, r_k = score_killshot(d, tf)  # tf'e gore esik secimi
    total = s_oi + s_w + s_r + s_f + s_c + s_k
    verdict = "LONG" if total >= 2 else ("SHORT" if total <= -2 else "FLAT")
    reasons = [r for r in [r_k, r_oi, r_w, r_r, r_f, r_c] if r]  # killshot once gelsin
    return {"scores": {"oiPrice": s_oi, "whale": s_w, "retail": s_r,
                        "funding": s_f, "cvd": s_c, "killshot": s_k,
                        "total": total},
            "reasons": reasons, "verdict": verdict}


def decide(symbol, tf_results, user_position, entry_price=None, current_price=None, ema_data=None):
    """Multi-TF agirlikli skor + pozisyon farkindaligi + 1h teyit + EMA + PnL."""
    weighted_total = 0.0
    weight_sum = 0.0
    all_reasons = []
    h1_verdict = None
    tf_verdicts = {}

    for tf in TIMEFRAMES:
        d = tf_results.get(tf)
        if not d or not d.get("ok"): continue
        v = compute_tf_verdict(d, tf)  # tf'i de gec
        if v is None: continue
        w = TF_WEIGHTS.get(tf, 1.0)
        weighted_total += v["scores"]["total"] * w
        weight_sum += w
        tf_verdicts[tf] = v["verdict"]
        if tf == "1h":
            h1_verdict = v["verdict"]
        for r in v["reasons"]:
            all_reasons.append(f"[{tf}] {r}")

    if weight_sum == 0:
        return {"action": "FLAT", "actionLabel": "VERI YOK",
                "subtitle": "Yeterli veri toplanamadi",
                "totalScore": 0.0, "reasons": [], "pnlPct": None}

    total = weighted_total

    # ============== 1H VETO ==============
    # Senin entry TF'in 1h. Eger genel skor LONG diyor ama 1h SHORT diyorsa
    # (veya tersi), skor yariya bolunur. 1h kapi bekcisi gibi davranir.
    veto_applied = False
    if h1_verdict is not None:
        if total > 0 and h1_verdict == "SHORT":
            total = total * 0.5
            veto_applied = "down"
            all_reasons.insert(0, f"[VETO] 1h SHORT verdict, genel skor yariya bolundu")
        elif total < 0 and h1_verdict == "LONG":
            total = total * 0.5
            veto_applied = "up"
            all_reasons.insert(0, f"[VETO] 1h LONG verdict, genel skor yariya bolundu")

    # ============== EMA KATKILARI ==============
    # 4h EMA50 rejim + 1h EMA7/30 cross. total skora eklenir.
    ema_regime = None
    ema_notes = []
    if ema_data:
        regime = ema_data.get("regime")
        cross_state = ema_data.get("crossState")
        fresh = ema_data.get("freshCross")
        ema_regime = regime

        # 1. 4h rejim - yumusak: skorun yonune gore guclendir/zayiflat
        # total > 0 (LONG egilim) + BULL rejim = uyumlu, guclendir
        # total > 0 + BEAR rejim = ters, zayiflat
        if regime == "BULL":
            if total > 0:
                total += THRESHOLDS["ema_regime_aligned"]
                ema_notes.append(f"[EMA] 4h BOGA rejimi (fiyat EMA50 ustunde +{ema_data.get('regimeDistance')}%), long destekli")
            elif total < 0:
                total += THRESHOLDS["ema_regime_aligned"]  # negatif skora +1 = zayiflatir
                ema_notes.append(f"[EMA] 4h BOGA rejimi ama short sinyali - zayiflatildi")
        elif regime == "BEAR":
            if total < 0:
                total -= THRESHOLDS["ema_regime_aligned"]
                ema_notes.append(f"[EMA] 4h AYI rejimi (fiyat EMA50 altinda {ema_data.get('regimeDistance')}%), short destekli")
            elif total > 0:
                total -= THRESHOLDS["ema_regime_aligned"]  # pozitif skordan -1 = zayiflatir
                ema_notes.append(f"[EMA] 4h AYI rejimi ama long sinyali - zayiflatildi")

        # 2. Taze 1h EMA7/30 kesisimi - guclu sinyal
        if fresh == "BULL":
            total += THRESHOLDS["ema_cross_fresh"]
            ema_notes.append(f"[EMA] TAZE 1h EMA7>EMA30 yukari kesti - guclu al sinyali")
        elif fresh == "BEAR":
            total -= THRESHOLDS["ema_cross_fresh"]
            ema_notes.append(f"[EMA] TAZE 1h EMA7<EMA30 asagi kesti - guclu sat sinyali")
        # 3. Surekli cross yonu (taze degilse, hafif bias)
        elif cross_state == "BULL":
            total += THRESHOLDS["ema_cross_state"]
            ema_notes.append(f"[EMA] 1h EMA7>EMA30 (boga momentumu)")
        elif cross_state == "BEAR":
            total -= THRESHOLDS["ema_cross_state"]
            ema_notes.append(f"[EMA] 1h EMA7<EMA30 (ayi momentumu)")

    # EMA notlarini sebeplerin basina ekle
    for note in reversed(ema_notes):
        all_reasons.insert(0, note)

    # PnL hesabi
    pnl_pct = None
    if entry_price and current_price and entry_price > 0:
        if user_position == "long":
            pnl_pct = (current_price - entry_price) / entry_price * 100
        elif user_position == "short":
            pnl_pct = (entry_price - current_price) / entry_price * 100

    # ============== POZISYON BAZLI KARAR MANTIGI ==============

    if user_position == "long":
        action, label, subtitle = _decide_for_long(total, h1_verdict, pnl_pct)
    elif user_position == "short":
        action, label, subtitle = _decide_for_short(total, h1_verdict, pnl_pct)
    else:  # flat
        action, label, subtitle = _decide_for_flat(total, h1_verdict)

    return {
        "action": action, "actionLabel": label, "subtitle": subtitle,
        "totalScore": total,
        "reasons": all_reasons[:12],
        "pnlPct": pnl_pct,
        "h1Verdict": h1_verdict,
        "emaRegime": ema_regime,
        "emaData": ema_data,
    }


def _decide_for_long(total, h1_verdict, pnl_pct):
    """LONG pozisyondayken: ters sinyalde kapat, ayni yonde tut. Ekleme yok."""
    if total <= -THRESHOLDS["counter_strong"]:
        if pnl_pct is not None and pnl_pct > 0:
            return "CLOSE_TP", "KAR AL - POZISYONU KAPAT", \
                   f"Guclu short sinyali (skor {total:.1f}), kardayken kapat (+{pnl_pct:.2f}%)"
        elif pnl_pct is not None and pnl_pct < 0:
            return "CLOSE_SL", "ZARARI KES - POZISYONU KAPAT", \
                   f"Guclu short sinyali (skor {total:.1f}), zarar kes ({pnl_pct:.2f}%)"
        else:
            return "CLOSE", "POZISYONU KAPAT", \
                   f"Guclu short sinyali olustu (skor {total:.1f}), long'u kapat"
    if total <= -THRESHOLDS["counter_medium"]:
        return "PARTIAL_CLOSE", "KISMI KAPAT %50", \
               f"Ters sinyal guc kazaniyor (skor {total:.1f}), riski azalt"
    if total <= -THRESHOLDS["counter_weak"]:
        return "TIGHTEN_STOP", "STOPU YAKLASTIR", \
               f"Hafif ters sinyal (skor {total:.1f}), stop'unu break-even'a cek"
    # Ayni yon veya notr: TUT
    pnl_text = f" (PnL {pnl_pct:+.2f}%)" if pnl_pct is not None else ""
    return "HOLD", "TUT", f"Long pozisyon korunuyor (skor {total:.1f}){pnl_text}"


def _decide_for_short(total, h1_verdict, pnl_pct):
    """SHORT pozisyondayken: ters sinyalde kapat, ayni yonde tut. Ekleme yok."""
    if total >= THRESHOLDS["counter_strong"]:
        if pnl_pct is not None and pnl_pct > 0:
            return "CLOSE_TP", "KAR AL - POZISYONU KAPAT", \
                   f"Guclu long sinyali (skor +{total:.1f}), kardayken kapat (+{pnl_pct:.2f}%)"
        elif pnl_pct is not None and pnl_pct < 0:
            return "CLOSE_SL", "ZARARI KES - POZISYONU KAPAT", \
                   f"Guclu long sinyali (skor +{total:.1f}), zarar kes ({pnl_pct:.2f}%)"
        else:
            return "CLOSE", "POZISYONU KAPAT", \
                   f"Guclu long sinyali olustu (skor +{total:.1f}), short'u kapat"
    if total >= THRESHOLDS["counter_medium"]:
        return "PARTIAL_CLOSE", "KISMI KAPAT %50", \
               f"Ters sinyal guc kazaniyor (skor +{total:.1f}), riski azalt"
    if total >= THRESHOLDS["counter_weak"]:
        return "TIGHTEN_STOP", "STOPU YAKLASTIR", \
               f"Hafif ters sinyal (skor +{total:.1f}), stop'unu break-even'a cek"
    # Ayni yon veya notr: TUT
    pnl_text = f" (PnL {pnl_pct:+.2f}%)" if pnl_pct is not None else ""
    return "HOLD", "TUT", f"Short pozisyon korunuyor (skor {total:.1f}){pnl_text}"


def _decide_for_flat(total, h1_verdict):
    """FLAT pozisyondayken giris karari - 1h teyit sarti var."""
    # Guclu LONG sinyali
    if total >= THRESHOLDS["long_open"]:
        if h1_verdict == "LONG":
            return "LONG_OPEN", "LONG AC", \
                   f"Guclu boga sinyali (skor +{total:.1f}) + 1h teyit verdi"
        else:
            return "WAIT", "BEKLE - 1H TEYIT YOK", \
                   f"Genel skor guclu (+{total:.1f}) ama 1h timeframe henuz teyit etmedi ({h1_verdict})"
    # Orta-LONG: bekle
    if total >= THRESHOLDS["long_hold"]:
        return "WAIT", "BEKLE", \
               f"Boga egilim var (skor +{total:.1f}) ama yeterli guclukte degil"
    # Guclu SHORT sinyali
    if total <= THRESHOLDS["short_open"]:
        if h1_verdict == "SHORT":
            return "SHORT_OPEN", "SHORT AC", \
                   f"Guclu ayi sinyali (skor {total:.1f}) + 1h teyit verdi"
        else:
            return "WAIT", "BEKLE - 1H TEYIT YOK", \
                   f"Genel skor guclu ({total:.1f}) ama 1h timeframe henuz teyit etmedi ({h1_verdict})"
    # Orta-SHORT: bekle
    if total <= THRESHOLDS["short_hold"]:
        return "WAIT", "BEKLE", \
               f"Ayi egilim var (skor {total:.1f}) ama yeterli guclukte degil"
    # Sinyal yok
    return "FLAT", "FLAT", "Net sinyal yok, beklemede kal"


def analyze_symbol(symbol, user_position, entry_price=None):
    sym = symbol.upper().replace("USDT", "") + "USDT"
    tf_results = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(binance_tf_data, sym, tf): tf for tf in TIMEFRAMES}
        for fut in as_completed(futures):
            tf = futures[fut]
            try: tf_results[tf] = fut.result()
            except Exception as e: tf_results[tf] = {"ok": False, "error": str(e)}
    exchanges = {}
    b1h = tf_results.get("1h")
    current_price = None
    if b1h and b1h.get("ok"):
        current_price = b1h.get("priceNow")
        exchanges["Binance"] = {"ok": True,
            "longPct": b1h["longPct"], "shortPct": b1h["shortPct"],
            "whaleLongPct": b1h.get("whaleLongPct"),
            "retailLongPct": b1h.get("retailLongPct"),
            "openInterest": b1h.get("oiNow"),
            "fundingRate": b1h.get("fundingRate"),
            "takerBuyPct": b1h.get("takerBuyPct")}
    else:
        exchanges["Binance"] = {"ok": False, "error": (b1h or {}).get("error", "Binance veri yok")}
    with ThreadPoolExecutor(max_workers=4) as pool:
        f_bb = pool.submit(bybit_summary, sym)
        f_okx = pool.submit(okx_summary, symbol)
        f_bg = pool.submit(bitget_summary, sym)
        f_ema = pool.submit(fetch_ema_signals, sym)
        try: exchanges["Bybit"] = f_bb.result()
        except Exception as e: exchanges["Bybit"] = {"ok": False, "error": str(e)}
        try: exchanges["OKX"] = f_okx.result()
        except Exception as e: exchanges["OKX"] = {"ok": False, "error": str(e)}
        try: exchanges["Bitget"] = f_bg.result()
        except Exception as e: exchanges["Bitget"] = {"ok": False, "error": str(e)}
        try: ema_data = f_ema.result()
        except Exception as e: ema_data = None
    timeframes_out = {}
    for tf, d in tf_results.items():
        if d and d.get("ok"):
            v = compute_tf_verdict(d, tf)
            timeframes_out[tf] = {**d, "scores": v["scores"], "verdict": v["verdict"]}
        else:
            timeframes_out[tf] = {"ok": False, "error": (d or {}).get("error", "no data")}
    decision = decide(symbol, tf_results, user_position, entry_price, current_price, ema_data)

    # Anlamli sinyal varsa DB'ye kaydet (sadece cron veya manuel istek)
    saved = False
    if SUPABASE_ENABLED:
        try:
            saved = save_signal_if_meaningful(symbol, decision, current_price, tf_results)
        except Exception as e:
            sys.stderr.write(f"save_signal hatasi: {e}\n")

    return {"ok": True, "symbol": symbol, "userPosition": user_position,
            "entryPrice": entry_price, "currentPrice": current_price,
            "decision": decision, "timeframes": timeframes_out, "exchanges": exchanges,
            "emaData": ema_data, "signalSaved": saved}


MANIFEST_JSON = json.dumps({
    "name": "L/S Decision Engine", "short_name": "L/S Engine",
    "start_url": "/", "display": "standalone",
    "background_color": "#0a0e0d", "theme_color": "#0a0e0d",
    "orientation": "portrait",
    "icons": [{"src": "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 192 192'%3E%3Crect fill='%230a0e0d' width='192' height='192'/%3E%3Ctext x='96' y='110' font-family='monospace' font-size='48' font-weight='bold' fill='%2300d09c' text-anchor='middle'%3EL/S%3C/text%3E%3C/svg%3E",
               "sizes": "192x192", "type": "image/svg+xml"}]
})


class LSHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        sys.stderr.write(f"  - {self.address_string()} - {format % args}\n")
    def _send(self, status, content_type, body_bytes):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body_bytes)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body_bytes)
    def _send_json(self, status, payload):
        self._send(status, "application/json; charset=utf-8", json.dumps(payload).encode("utf-8"))
    def _send_html(self, html):
        self._send(200, "text/html; charset=utf-8", html.encode("utf-8"))
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        q = urllib.parse.parse_qs(parsed.query)
        if path in ("/", "/index.html"):
            self._send_html(DASHBOARD_HTML)
            return
        if path == "/manifest.json":
            self._send(200, "application/json; charset=utf-8", MANIFEST_JSON.encode("utf-8"))
            return
        if path == "/healthz":
            self._send_json(200, {"ok": True})
            return
        if path == "/api/analyze":
            symbol = (q.get("symbol", [""])[0] or "").strip().upper()
            user_pos = (q.get("position", ["flat"])[0] or "flat").lower()
            if user_pos not in ("long", "short", "flat"): user_pos = "flat"
            entry_price = None
            try:
                ep_raw = q.get("entry", [""])[0]
                if ep_raw and ep_raw.strip():
                    entry_price = float(ep_raw)
                    if entry_price <= 0:
                        entry_price = None
            except (ValueError, TypeError):
                entry_price = None
            if not symbol:
                self._send_json(400, {"ok": False, "error": "symbol required"})
                return
            try:
                data = analyze_symbol(symbol, user_pos, entry_price)
                self._send_json(200, data)
            except urllib.error.HTTPError as e:
                self._send_json(200, {"ok": False, "error": f"upstream HTTP {e.code}"})
            except Exception as e:
                self._send_json(200, {"ok": False, "error": str(e)})
            return

        # Cron tetikleyici endpoint - cron-job.org buraya istek atar
        if path == "/api/cron-check":
            try:
                # BTC icin analiz yap, FLAT pozisyonla (pozisyondan bagimsiz sistem skoru)
                data = analyze_symbol("BTC", "flat", None)
                self._send_json(200, {
                    "ok": True,
                    "action": data.get("decision", {}).get("action"),
                    "score": data.get("decision", {}).get("totalScore"),
                    "saved": data.get("signalSaved", False),
                    "supabase": SUPABASE_ENABLED,
                })
            except Exception as e:
                self._send_json(200, {"ok": False, "error": str(e)})
            return

        # Sinyal gecmisi
        if path == "/api/signals":
            symbol = (q.get("symbol", ["BTC"])[0] or "BTC").strip().upper()
            try:
                limit = int(q.get("limit", ["50"])[0])
                limit = max(1, min(limit, 200))
            except ValueError:
                limit = 50
            try:
                history = get_signal_history(symbol, limit)
                self._send_json(200, {"ok": True, "symbol": symbol,
                                       "count": len(history), "signals": history,
                                       "supabaseEnabled": SUPABASE_ENABLED})
            except Exception as e:
                self._send_json(200, {"ok": False, "error": str(e),
                                       "supabaseEnabled": SUPABASE_ENABLED})
            return

        # Backtest - gecmis sinyalleri hesaplar
        if path == "/api/backtest":
            symbol = (q.get("symbol", ["BTC"])[0] or "BTC").strip().upper()
            try:
                days = int(q.get("days", ["7"])[0])
                days = max(1, min(days, 20))  # max 20 gun (API limiti)
            except ValueError:
                days = 7
            try:
                data = run_backtest(symbol, days)
                self._send_json(200, data)
            except Exception as e:
                sys.stderr.write(f"backtest hatasi: {e}\n")
                self._send_json(200, {"ok": False, "error": str(e)})
            return

        self._send_json(404, {"ok": False, "error": "not found"})


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    print(f"L/S Decision Engine v3 listening on {HOST}:{PORT}", flush=True)
    try:
        with ThreadedServer((HOST, PORT), LSHandler) as srv:
            srv.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.", flush=True)



# DASHBOARD HTML - dosya sonuna eklenecek
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
<script src="https://unpkg.com/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
<style>
  :root {
    --bg:#0a0e0d; --bg-2:#0f1413;
    --border:#1f2a28; --border-strong:#2a3a37;
    --text:#d4dcd9; --text-dim:#6e7976; --text-faint:#3f4845;
    --green:#00d09c; --red:#ff4d6d; --red-dim:#a82d44;
    --amber:#ffb83d; --accent:#6df5d4;
    --binance:#f3ba2f; --bybit:#ff4d6d; --okx:#ffffff; --bitget:#6df5d4;
  }
  * { box-sizing:border-box; margin:0; padding:0; -webkit-tap-highlight-color:transparent; }
  html, body { background:var(--bg); color:var(--text);
    font-family:'JetBrains Mono', monospace; font-size:13px; line-height:1.5;
    min-height:100vh; overflow-x:hidden; -webkit-font-smoothing:antialiased; }
  body::before { content:''; position:fixed; inset:0;
    background: radial-gradient(ellipse at top left, rgba(0,208,156,0.04), transparent 50%),
                radial-gradient(ellipse at bottom right, rgba(255,77,109,0.03), transparent 50%);
    pointer-events:none; z-index:0; }
  body::after { content:''; position:fixed; inset:0;
    background-image: linear-gradient(rgba(255,255,255,0.012) 1px, transparent 1px),
                      linear-gradient(90deg, rgba(255,255,255,0.012) 1px, transparent 1px);
    background-size:40px 40px; pointer-events:none; z-index:0; }
  .wrap { position:relative; z-index:1; max-width:1400px; margin:0 auto;
    padding:24px; padding-top:calc(24px + env(safe-area-inset-top));
    padding-bottom:calc(24px + env(safe-area-inset-bottom)); }
  header { display:flex; align-items:center; justify-content:space-between;
    padding-bottom:18px; border-bottom:1px solid var(--border); margin-bottom:24px;
    gap:16px; flex-wrap:wrap; }
  .logo { font-family:'Major Mono Display', monospace; font-size:22px;
    letter-spacing:0.04em; color:var(--text); }
  .logo span { color:var(--green); }
  .meta { display:flex; gap:20px; align-items:center; font-size:11px; color:var(--text-dim); }
  .meta .dot { width:6px; height:6px; border-radius:50%; background:var(--green);
    display:inline-block; margin-right:6px; box-shadow:0 0 6px var(--green);
    animation:pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1;} 50%{opacity:0.4;} }
  .controls { display:grid; grid-template-columns:1fr auto; gap:12px;
    margin-bottom:24px; padding:14px; background:var(--bg-2); border:1px solid var(--border); }
  .input-group { display:flex; flex-direction:column; gap:6px; }
  .input-group label { font-size:10px; letter-spacing:0.18em; color:var(--text-dim); text-transform:uppercase; }
  input[type="text"] { background:var(--bg); border:1px solid var(--border);
    color:var(--text); font-family:'JetBrains Mono',monospace; font-size:14px;
    padding:10px 12px; outline:none; text-transform:uppercase;
    -webkit-appearance:none; border-radius:0; }
  input[type="text"]:focus { border-color:var(--green); }
  button.run { background:var(--green); color:var(--bg); border:none; padding:0 24px;
    font-family:'JetBrains Mono',monospace; font-weight:700; font-size:13px;
    letter-spacing:0.12em; cursor:pointer; align-self:end; height:40px;
    -webkit-appearance:none; border-radius:0; }
  button.run:hover { background:var(--accent); }
  button.run:disabled { background:var(--border-strong); color:var(--text-dim); cursor:wait; }
  h2.section { font-size:11px; letter-spacing:0.22em; color:var(--text-dim);
    text-transform:uppercase; margin:32px 0 12px; padding-bottom:8px;
    border-bottom:1px solid var(--border); }
  h2.section .arrow { color:var(--green); margin-right:6px; }
  .decision { background:var(--bg-2); border:1px solid var(--border-strong);
    padding:24px; margin-bottom:24px; position:relative; overflow:hidden; }
  .decision::before { content:''; position:absolute; left:0; top:0; bottom:0; width:4px;
    background:var(--text-dim); transition:background 0.3s; }
  .decision.action-LONG_OPEN::before, .decision.action-LONG_HOLD::before { background:var(--green); }
  .decision.action-SHORT_OPEN::before, .decision.action-SHORT_HOLD::before { background:var(--red); }
  .decision.action-FLAT::before { background:var(--text-dim); }
  .decision.action-CLOSE::before, .decision.action-CLOSE_TP::before,
  .decision.action-CLOSE_SL::before { background:var(--amber); }
  .decision.action-PARTIAL_CLOSE::before { background:var(--amber); }
  .decision.action-TIGHTEN_STOP::before { background:#ff9d42; }
  .decision.action-HOLD::before { background:var(--green); }
  .decision.action-ADD_POSITION::before { background:#7cffb2; }
  .decision.action-WAIT::before { background:var(--text-dim); }
  .decision-head { display:flex; justify-content:space-between; align-items:baseline;
    margin-bottom:18px; gap:16px; flex-wrap:wrap; }
  .action-label { font-size:24px; font-weight:700; letter-spacing:0.05em; }
  .action-LONG_OPEN .action-label, .action-LONG_HOLD .action-label,
  .action-HOLD.long .action-label, .action-ADD_POSITION .action-label { color:var(--green); }
  .action-SHORT_OPEN .action-label, .action-SHORT_HOLD .action-label,
  .action-HOLD.short .action-label { color:var(--red); }
  .action-FLAT .action-label, .action-WAIT .action-label { color:var(--text-dim); }
  .action-CLOSE .action-label, .action-CLOSE_TP .action-label,
  .action-CLOSE_SL .action-label, .action-PARTIAL_CLOSE .action-label,
  .action-TIGHTEN_STOP .action-label { color:var(--amber); }
  .score-pill { font-size:11px; color:var(--text-dim); letter-spacing:0.08em; }
  .score-pill b { color:var(--text); font-size:14px; }
  .pnl-pill { font-size:11px; letter-spacing:0.08em; padding:4px 10px; border:1px solid var(--border-strong); }
  .pnl-pill b { font-size:13px; }
  .pnl-pill.pos b { color:var(--green); }
  .pnl-pill.neg b { color:var(--red); }
  .ema-pill { font-size:11px; letter-spacing:0.08em; padding:4px 10px; border:1px solid var(--border-strong); }
  .ema-pill b { font-size:12px; }
  .ema-pill.bull b { color:var(--green); }
  .ema-pill.bear b { color:var(--red); }
  .position-control { display:flex; gap:8px; margin-top:16px; flex-wrap:wrap; align-items:center; }
  .pos-btn { background:transparent; border:1px solid var(--border-strong);
    color:var(--text-dim); padding:8px 16px; font-family:inherit; font-size:11px;
    letter-spacing:0.1em; cursor:pointer; -webkit-appearance:none; border-radius:0;
    transition:all 0.15s; }
  .pos-btn:hover { color:var(--text); border-color:var(--text-dim); }
  .pos-btn.active.long  { background:var(--green); color:var(--bg); border-color:var(--green); }
  .pos-btn.active.short { background:var(--red); color:var(--bg); border-color:var(--red); }
  .pos-btn.active.flat  { background:var(--text-dim); color:var(--bg); border-color:var(--text-dim); }
  .pos-label { font-size:10px; color:var(--text-dim); margin-right:8px; letter-spacing:0.15em; }
  .entry-control { display:none; gap:8px; margin-top:12px; align-items:center; flex-wrap:wrap; }
  .entry-control.visible { display:flex; }
  .entry-control label { font-size:10px; letter-spacing:0.15em; color:var(--text-dim); }
  .entry-control input { background:var(--bg); border:1px solid var(--border-strong);
    color:var(--text); font-family:inherit; font-size:13px; padding:6px 10px;
    width:140px; outline:none; -webkit-appearance:none; border-radius:0; }
  .entry-control input:focus { border-color:var(--green); }
  .entry-control .price-info { font-size:10px; color:var(--text-dim); letter-spacing:0.1em; }
  .entry-control .price-info b { color:var(--text); }
  .reasoning { margin-top:18px; padding-top:16px; border-top:1px dashed var(--border-strong); }
  .reasoning .r-title { font-size:10px; letter-spacing:0.18em; color:var(--text-dim); margin-bottom:8px; }
  .reasoning ul { list-style:none; padding:0; }
  .reasoning li { font-size:12px; color:var(--text); padding:3px 0; padding-left:14px; position:relative; }
  .reasoning li::before { content:'>'; color:var(--green); position:absolute; left:0; }
  .matrix { background:var(--bg-2); border:1px solid var(--border); overflow:hidden; }
  .matrix-row { display:grid; grid-template-columns:60px repeat(6, 1fr) 100px;
    gap:1px; border-bottom:1px solid var(--border); background:var(--border); }
  .matrix-row:last-child { border-bottom:none; }
  .matrix-row.header { background:#000; }
  .matrix-cell { background:var(--bg-2); padding:10px 8px; font-size:11px;
    text-align:center; display:flex; align-items:center; justify-content:center; }
  .matrix-row.header .matrix-cell { color:var(--text-dim); letter-spacing:0.12em; font-size:10px; }
  .tf-label { color:var(--text); font-weight:700; letter-spacing:0.08em; }
  .cell-pos { color:var(--green); }
  .cell-neg { color:var(--red); }
  .cell-zero { color:var(--text-faint); }
  .verdict-cell { font-weight:700; letter-spacing:0.05em; font-size:11px; }
  .verdict-LONG  { color:var(--green); }
  .verdict-SHORT { color:var(--red); }
  .verdict-FLAT  { color:var(--text-dim); }
  .grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(280px,1fr));
    gap:1px; background:var(--border); border:1px solid var(--border); }
  .card { background:var(--bg-2); padding:18px; position:relative; border-top:2px solid transparent; }
  .card.ex-binance { border-top-color:var(--binance); }
  .card.ex-bybit   { border-top-color:var(--bybit); }
  .card.ex-okx     { border-top-color:var(--okx); }
  .card.ex-bitget  { border-top-color:var(--bitget); }
  .card-head { display:flex; justify-content:space-between; align-items:baseline; margin-bottom:14px; }
  .ex-name { font-size:14px; font-weight:700; letter-spacing:0.08em; }
  .ex-status { font-size:10px; color:var(--text-dim); }
  .ex-status.ok { color:var(--green); }
  .ex-status.err { color:var(--red); }
  .ratio-row { display:flex; justify-content:space-between; margin-top:8px; font-size:11px; }
  .ratio-row .l { color:var(--text-dim); }
  .ratio-row .v { color:var(--text); font-weight:500; }
  .pct-bar { margin-top:8px; height:3px; background:var(--red-dim); position:relative; overflow:hidden; }
  .pct-bar .fill { position:absolute; left:0; top:0; bottom:0; background:var(--green); }
  .divider { height:1px; background:var(--border); margin:12px 0 6px; }
  .v.fr-pos { color:var(--green); }
  .v.fr-neg { color:var(--red); }
  .err-msg { color:var(--red); font-size:11px; margin-top:8px; opacity:0.7; }

  /* TradingView grafik */
  .tv-wrap { background:var(--bg-2); border:1px solid var(--border); padding:1px; }
  .tv-chart { height:450px; width:100%; }

  /* Sinyal gecmisi */
  .signals-wrap { background:var(--bg-2); border:1px solid var(--border);
    max-height:360px; overflow-y:auto; }
  .signal-row { display:grid; grid-template-columns: 110px 130px 60px 1fr;
    gap:12px; padding:10px 14px; border-bottom:1px solid var(--border);
    font-size:11px; align-items:center; }
  .signal-row:last-child { border-bottom:none; }
  .signal-row:hover { background:rgba(255,255,255,0.02); }
  .sig-time { color:var(--text-dim); font-size:10px; letter-spacing:0.05em; }
  .sig-action { font-weight:700; letter-spacing:0.05em; font-size:11px; }
  .sig-action.long  { color:var(--green); }
  .sig-action.short { color:var(--red); }
  .sig-action.close { color:var(--amber); }
  .sig-action.partial, .sig-action.tighten { color:#ff9d42; }
  .sig-action.add { color:#7cffb2; }
  .sig-score { font-family:'JetBrains Mono', monospace; font-weight:500;
    text-align:right; font-size:11px; }
  .sig-score.pos { color:var(--green); }
  .sig-score.neg { color:var(--red); }
  .sig-meta { color:var(--text-dim); font-size:10px; }
  .sig-meta b { color:var(--text); }
  .signals-empty { padding:32px 20px; text-align:center; color:var(--text-faint); font-size:11px; }

  /* Backtest */
  .backtest-wrap { background:var(--bg-2); border:1px solid var(--border); }
  .backtest-controls { display:flex; gap:8px; align-items:center; padding:14px;
    border-bottom:1px solid var(--border); flex-wrap:wrap; }
  .bt-label { font-size:10px; color:var(--text-dim); letter-spacing:0.15em; }
  .bt-day-btn { background:transparent; border:1px solid var(--border-strong);
    color:var(--text-dim); padding:6px 14px; font-family:inherit; font-size:11px;
    cursor:pointer; -webkit-appearance:none; border-radius:0; }
  .bt-day-btn:hover { color:var(--text); border-color:var(--text-dim); }
  .bt-day-btn.active { background:var(--text-dim); color:var(--bg); border-color:var(--text-dim); }
  .bt-run { background:var(--green); color:var(--bg); border:none; padding:0 18px;
    font-family:inherit; font-weight:700; font-size:11px; letter-spacing:0.1em;
    cursor:pointer; height:32px; margin-left:auto; -webkit-appearance:none; border-radius:0; }
  .bt-run:hover { background:var(--accent); }
  .bt-run:disabled { background:var(--border-strong); color:var(--text-dim); cursor:wait; }

  .bt-summary { padding:14px; border-bottom:1px solid var(--border); }
  .bt-summary-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(120px, 1fr)); gap:10px; }
  .bt-stat { background:var(--bg); padding:8px 10px; border:1px solid var(--border); }
  .bt-stat-label { font-size:9px; color:var(--text-dim); letter-spacing:0.15em; text-transform:uppercase; }
  .bt-stat-value { font-size:14px; font-weight:600; margin-top:4px; }
  .bt-stat-value.pos { color:var(--green); }
  .bt-stat-value.neg { color:var(--red); }
  .bt-stat-sub { font-size:9px; color:var(--text-faint); margin-top:2px; }

  .bt-chart-wrap { padding:1px; }
  .bt-chart { height:380px; width:100%; }

  .bt-signals-list { max-height:300px; overflow-y:auto; }
  .bt-sig-row { display:grid; grid-template-columns: 100px 110px 60px 70px 70px 70px;
    gap:8px; padding:8px 14px; border-bottom:1px solid var(--border); font-size:10px;
    align-items:center; }
  .bt-sig-row.header { background:#000; color:var(--text-dim); letter-spacing:0.1em; }
  .bt-sig-row .pnl { font-weight:600; text-align:right; }
  .bt-sig-row .pnl.pos { color:var(--green); }
  .bt-sig-row .pnl.neg { color:var(--red); }

  .info { margin-top:32px; padding:16px; background:var(--bg-2);
    border:1px dashed var(--border-strong); font-size:11px; color:var(--text-dim); line-height:1.7; }
  .info b { color:var(--text); }
  .blink { animation:blink 1s infinite; }
  @keyframes blink { 50% { opacity:0.3; } }
  .loading-overlay { color:var(--text-faint); padding:40px 20px; text-align:center;
    font-size:12px; letter-spacing:0.15em; grid-column:1/-1; background:var(--bg-2); }
  @media (max-width:720px) {
    .wrap { padding:16px; }
    .controls { grid-template-columns:1fr; padding:12px; }
    button.run { height:46px; align-self:stretch; font-size:14px; }
    .action-label { font-size:22px; }
    .matrix-row { grid-template-columns:46px repeat(6, 1fr) 70px; }
    .matrix-cell { padding:8px 4px; font-size:10px; }
    .matrix-row.header .matrix-cell { font-size:9px; }
    .grid { grid-template-columns:1fr; }
    input[type="text"] { font-size:16px; }
    .tv-chart { height:380px; }
    .bt-chart { height:300px; }
    .bt-sig-row { grid-template-columns: 70px 1fr 50px 50px; gap:6px; padding:6px 10px; font-size:9px; }
    .bt-sig-row .pnl-1h { display:none; }
    .signal-row { grid-template-columns: 90px 1fr 60px; gap:8px; padding:8px 10px; font-size:10px; }
    .signal-row .sig-meta { display:none; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="logo">L/S<span>&middot;</span>DECISION<span>&middot;</span>ENGINE</div>
    <div class="meta">
      <span><span class="dot"></span>LIVE</span>
      <span id="clock">--:--:-- UTC</span>
    </div>
  </header>
  <div class="controls">
    <div class="input-group">
      <label>SEMBOL / SYMBOL</label>
      <input type="text" id="symbolInput" placeholder="orn: BTC, ETH, ON, ONDO" value="BTC" autocomplete="off" autocapitalize="characters">
    </div>
    <button class="run" id="runBtn">ANALIZ ET &gt;</button>
  </div>
  <h2 class="section"><span class="arrow">&#9656;</span>KARAR MOTORU</h2>
  <div class="decision action-FLAT" id="decisionPanel">
    <div class="decision-head">
      <div>
        <div class="action-label" id="actionLabel">--</div>
        <div style="font-size:11px;color:var(--text-dim);letter-spacing:0.1em;margin-top:4px" id="actionSubtitle">Analiz icin coin gir</div>
      </div>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <div class="ema-pill" id="emaPill" style="display:none">REJIM: <b id="emaRegimeValue">--</b></div>
        <div class="pnl-pill" id="pnlPill" style="display:none">PnL: <b id="pnlValue">--</b></div>
        <div class="score-pill">SKOR: <b id="totalScore">--</b></div>
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
      <input type="text" id="entryPriceInput" placeholder="orn: 77000" inputmode="decimal">
      <span class="price-info">Simdiki: <b id="currentPriceDisplay">--</b></span>
    </div>
    <div class="reasoning" id="reasoning" style="display:none">
      <div class="r-title">GEREKCE</div>
      <ul id="reasoningList"></ul>
    </div>
  </div>
  <h2 class="section"><span class="arrow">&#9656;</span>BTCUSDT.P 1H GRAFIK</h2>
  <div class="tv-wrap">
    <div id="tradingview_chart" class="tv-chart"></div>
  </div>

  <h2 class="section"><span class="arrow">&#9656;</span>MULTI-TIMEFRAME CONFLUENCE</h2>
  <div class="matrix" id="matrix">
    <div class="matrix-row header">
      <div class="matrix-cell">TF</div>
      <div class="matrix-cell">OI/PRICE</div>
      <div class="matrix-cell">WHALE</div>
      <div class="matrix-cell">RETAIL</div>
      <div class="matrix-cell">FUNDING</div>
      <div class="matrix-cell">CVD</div>
      <div class="matrix-cell">SKOR</div>
      <div class="matrix-cell">VERDICT</div>
    </div>
    <div id="matrixBody"></div>
  </div>
  <h2 class="section"><span class="arrow">&#9656;</span>SINYAL GECMISI</h2>
  <div class="signals-wrap" id="signalsWrap">
    <div class="loading-overlay">Yukleniyor...</div>
  </div>

  <h2 class="section"><span class="arrow">&#9656;</span>BACKTEST - GECMIS PERFORMANS</h2>
  <div class="backtest-wrap">
    <div class="backtest-controls">
      <span class="bt-label">GUN:</span>
      <button class="bt-day-btn" data-days="3">3</button>
      <button class="bt-day-btn active" data-days="7">7</button>
      <button class="bt-day-btn" data-days="14">14</button>
      <button class="bt-run" id="btRunBtn">BACKTEST CALISTIR &gt;</button>
    </div>
    <div class="bt-summary" id="btSummary" style="display:none"></div>
    <div class="bt-chart-wrap">
      <div id="btChart" class="bt-chart"></div>
    </div>
    <div class="bt-signals-list" id="btSignalsList"></div>
  </div>

  <h2 class="section"><span class="arrow">&#9656;</span>EXCHANGE BREAKDOWN (1h)</h2>
  <div class="grid" id="cards"></div>
  <div class="info">
    <b>v7 - BTC ICIN OPTIMIZE:</b> 6 metrik (OI/fiyat, whale-retail, retail, funding, CVD, killshot) + 4h EMA50 rejim + 1h EMA7/30 cross + multi-TF + pozisyon farkindaligi.<br><br>
    <b>NASIL CALISIR?</b> 3 TF (15m, 1h, 4h) skorlanir, agirlikli toplanir (4h x2.5, 1h x2.0, 15m x0.5). 1D timeframe sistem disindadir - manuel takip. 4h EMA50 boga/ayi rejimini belirler (rejime uygun sinyal guclenir). 1h EMA7/30 kesisimi al/sat sinyali verir (taze kesisim guclu, surekli yon hafif bias).<br><br>
    <b>POZISYON BAZLI TAVSIYELER:</b><br>
    &bull; <b>FLAT</b>: Sadece guclu sinyal + 1h teyit varsa LONG/SHORT AC, degilse BEKLE.<br>
    &bull; <b>LONG/SHORT pozisyonda</b>: Giris fiyatini gir, PnL hesaplanir. Sinyal ayni yonde ise TUT. Ters sinyal gucune gore: STOPU YAKLASTIR / KISMI KAPAT %50 / POZISYONU KAPAT (kar al / zarar kes).<br><br>
    <b>UYARI:</b> Bu arac finansal tavsiye degildir. Risk yonetimi senin sorumlulugundadir.
  </div>
</div>
<script>
const EXCHANGES = ['Binance', 'Bybit', 'OKX', 'Bitget'];
const TIMEFRAMES = ['15m', '1h', '4h'];
let lastFetch = null;
let userPosition = 'flat';

function cleanSymbol(raw) {
  return (raw || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
}
function fmtUSD(n) {
  if (n == null || isNaN(n)) return '\u2014';
  const abs = Math.abs(n);
  if (abs >= 1e9) return '$' + (n / 1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return '$' + (n / 1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return '$' + (n / 1e3).toFixed(2) + 'K';
  return '$' + n.toFixed(2);
}
function fmtFunding(pct) {
  if (pct == null || isNaN(pct)) return { text: '\u2014', cls: '' };
  const sign = pct >= 0 ? '+' : '';
  return { text: sign + pct.toFixed(4) + '%',
           cls: pct > 0 ? 'fr-pos' : (pct < 0 ? 'fr-neg' : '') };
}
function scoreCell(score) {
  if (score == null) return '<span class="cell-zero">\u2014</span>';
  if (score > 0) return `<span class="cell-pos">+${score}</span>`;
  if (score < 0) return `<span class="cell-neg">${score}</span>`;
  return '<span class="cell-zero">0</span>';
}
function renderDecision(symbol, decision, tfData, currentPrice) {
  const panel = document.getElementById('decisionPanel');
  panel.className = 'decision action-' + decision.action;
  // Renk i\xe7in mevcut pozisyonu da class'a ekle (HOLD durumunda renk i\xe7in)
  if (decision.action === 'HOLD') {
    panel.classList.add(userPosition);
  }
  document.getElementById('actionLabel').textContent = decision.actionLabel;
  document.getElementById('actionSubtitle').textContent = symbol + ' \u00b7 ' + decision.subtitle;
  document.getElementById('totalScore').textContent = decision.totalScore.toFixed(2);

  // PnL gosterimi
  const pnlPill = document.getElementById('pnlPill');
  const pnlValue = document.getElementById('pnlValue');
  if (decision.pnlPct != null) {
    pnlPill.style.display = '';
    const sign = decision.pnlPct >= 0 ? '+' : '';
    pnlValue.textContent = sign + decision.pnlPct.toFixed(2) + '%';
    pnlPill.classList.remove('pos', 'neg');
    pnlPill.classList.add(decision.pnlPct >= 0 ? 'pos' : 'neg');
  } else {
    pnlPill.style.display = 'none';
  }

  // EMA rejim gosterimi
  const emaPill = document.getElementById('emaPill');
  const emaRegimeValue = document.getElementById('emaRegimeValue');
  if (decision.emaRegime) {
    emaPill.style.display = '';
    emaPill.classList.remove('bull', 'bear');
    if (decision.emaRegime === 'BULL') {
      emaRegimeValue.textContent = 'BOGA';
      emaPill.classList.add('bull');
    } else {
      emaRegimeValue.textContent = 'AYI';
      emaPill.classList.add('bear');
    }
  } else {
    emaPill.style.display = 'none';
  }

  // Simdiki fiyat gosterimi
  const cpd = document.getElementById('currentPriceDisplay');
  if (currentPrice) {
    cpd.textContent = '$' + currentPrice.toLocaleString('en-US', {maximumFractionDigits: 2});
  } else {
    cpd.textContent = '--';
  }

  const rsn = document.getElementById('reasoning');
  if (decision.reasons && decision.reasons.length) {
    rsn.style.display = '';
    document.getElementById('reasoningList').innerHTML =
      decision.reasons.map(r => `<li>${r}</li>`).join('');
  } else {
    rsn.style.display = 'none';
  }
  const body = document.getElementById('matrixBody');
  body.innerHTML = TIMEFRAMES.map(tf => {
    const d = tfData[tf];
    if (!d || !d.ok) {
      return `<div class="matrix-row">
        <div class="matrix-cell tf-label">${tf}</div>
        <div class="matrix-cell cell-zero" style="grid-column: span 7">veri yok</div>
      </div>`;
    }
    return `<div class="matrix-row">
      <div class="matrix-cell tf-label">${tf}</div>
      <div class="matrix-cell">${scoreCell(d.scores.oiPrice)}</div>
      <div class="matrix-cell">${scoreCell(d.scores.whale)}</div>
      <div class="matrix-cell">${scoreCell(d.scores.retail)}</div>
      <div class="matrix-cell">${scoreCell(d.scores.funding)}</div>
      <div class="matrix-cell">${scoreCell(d.scores.cvd)}</div>
      <div class="matrix-cell"><b>${d.scores.total > 0 ? '+' : ''}${d.scores.total.toFixed(1)}</b></div>
      <div class="matrix-cell verdict-cell verdict-${d.verdict}">${d.verdict}</div>
    </div>`;
  }).join('');
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
      const oi = fmtUSD(r.openInterest);
      const whale = r.whaleLongPct;
      const retail = r.retailLongPct;
      const delta = (whale != null && retail != null) ? (whale - retail) : null;
      let whaleSection = '';
      if (whale != null && retail != null) {
        const deltaCls = delta > 0 ? 'fr-pos' : (delta < 0 ? 'fr-neg' : '');
        whaleSection = `
          <div class="divider"></div>
          <div class="ratio-row"><span class="l">WHALE LONG%</span><span class="v">${whale.toFixed(2)}%</span></div>
          <div class="ratio-row"><span class="l">RETAIL LONG%</span><span class="v">${retail.toFixed(2)}%</span></div>
          <div class="ratio-row"><span class="l">WHALE-RETAIL \u0394</span><span class="v ${deltaCls}">${delta > 0 ? '+' : ''}${delta.toFixed(2)}</span></div>
        `;
      }
      card.innerHTML = `
        <div class="card-head">
          <div class="ex-name">${ex.toUpperCase()}</div>
          <div class="ex-status ok">\u25CF ONLINE</div>
        </div>
        <div class="ratio-row"><span class="l">LONG ACCOUNTS</span><span class="v">${r.longPct.toFixed(2)}%</span></div>
        <div class="ratio-row"><span class="l">SHORT ACCOUNTS</span><span class="v">${r.shortPct.toFixed(2)}%</span></div>
        <div class="pct-bar"><div class="fill" style="width:${r.longPct}%"></div></div>
        ${whaleSection}
        <div class="divider"></div>
        <div class="ratio-row"><span class="l">OPEN INTEREST</span><span class="v">${oi}</span></div>
        <div class="ratio-row"><span class="l">FUNDING RATE</span><span class="v ${fr.cls}">${fr.text}</span></div>
      `;
    } else {
      card.innerHTML = `
        <div class="card-head">
          <div class="ex-name">${ex.toUpperCase()}</div>
          <div class="ex-status err">\u25CF NO DATA</div>
        </div>
        <div class="err-msg">${r?.error || 'veri yok'}</div>
      `;
    }
    grid.appendChild(card);
  });
}
async function run() {
  const rawInput = document.getElementById('symbolInput').value;
  const sym = cleanSymbol(rawInput);
  if (sym !== rawInput.trim().toUpperCase()) {
    document.getElementById('symbolInput').value = sym;
  }
  if (!sym) return;
  const btn = document.getElementById('runBtn');
  btn.disabled = true;
  btn.textContent = 'ANALIZ EDILIYOR...';
  document.getElementById('matrixBody').innerHTML = '<div class="loading-overlay blink">VERI TOPLANIYOR (multi-TF)...</div>';
  document.getElementById('cards').innerHTML = '<div class="loading-overlay blink">VERI TOPLANIYOR...</div>';

  // Entry price
  let entryQuery = '';
  if (userPosition !== 'flat') {
    const ep = parseFloat(document.getElementById('entryPriceInput').value);
    if (ep && ep > 0) {
      entryQuery = '&entry=' + ep;
    }
  }

  try {
    const r = await fetch('/api/analyze?symbol=' + encodeURIComponent(sym) +
                         '&position=' + encodeURIComponent(userPosition) + entryQuery);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    if (!data.ok) throw new Error(data.error || 'API hatasi');
    lastFetch = data;
    renderDecision(sym, data.decision, data.timeframes, data.currentPrice);
    renderExchanges(data.exchanges);
  } catch (e) {
    document.getElementById('actionLabel').textContent = 'HATA';
    document.getElementById('actionSubtitle').textContent = e.message;
    document.getElementById('matrixBody').innerHTML = '';
    document.getElementById('cards').innerHTML = '';
  } finally {
    btn.disabled = false;
    btn.textContent = 'ANALIZ ET >';
  }
}
function loadPosition() {
  try {
    const p = localStorage.getItem('userPosition');
    if (p && ['long', 'short', 'flat'].includes(p)) {
      userPosition = p;
    }
  } catch {}
  document.querySelectorAll('.pos-btn').forEach(b => {
    b.classList.remove('active', 'long', 'short', 'flat');
    if (b.dataset.pos === userPosition) {
      b.classList.add('active', userPosition);
    }
  });
  // Entry input gosterimi
  const entryCtl = document.getElementById('entryControl');
  if (userPosition === 'long' || userPosition === 'short') {
    entryCtl.classList.add('visible');
    // Daha onceden kaydedilmis entry varsa yukle
    try {
      const ep = localStorage.getItem('entryPrice_' + userPosition);
      if (ep) document.getElementById('entryPriceInput').value = ep;
    } catch {}
  } else {
    entryCtl.classList.remove('visible');
    document.getElementById('entryPriceInput').value = '';
  }
}

// Entry input degisikliginde localStorage'a kaydet
document.getElementById('entryPriceInput').addEventListener('input', e => {
  if (userPosition === 'long' || userPosition === 'short') {
    try { localStorage.setItem('entryPrice_' + userPosition, e.target.value); } catch {}
  }
});
document.getElementById('entryPriceInput').addEventListener('blur', () => {
  if (lastFetch) run();
});
document.getElementById('entryPriceInput').addEventListener('keydown', e => {
  if (e.key === 'Enter') { e.target.blur(); }
});

document.querySelectorAll('.pos-btn').forEach(b => {
  b.addEventListener('click', () => {
    userPosition = b.dataset.pos;
    try { localStorage.setItem('userPosition', userPosition); } catch {}
    loadPosition();
    if (lastFetch) run();
  });
});
document.getElementById('runBtn').addEventListener('click', run);
document.getElementById('symbolInput').addEventListener('keydown', e => {
  if (e.key === 'Enter') { e.target.blur(); run(); }
});
function tick() {
  const d = new Date();
  const z = n => String(n).padStart(2, '0');
  document.getElementById('clock').textContent =
    `${z(d.getUTCHours())}:${z(d.getUTCMinutes())}:${z(d.getUTCSeconds())} UTC`;
}
setInterval(tick, 1000); tick();
setInterval(() => { if (!document.hidden && lastFetch) run(); }, 60000);
document.addEventListener('visibilitychange', () => {
  if (!document.hidden && lastFetch) run();
});

// ============ TRADINGVIEW WIDGET ============
function initTradingView() {
  if (typeof TradingView === 'undefined') return;
  new TradingView.widget({
    autosize: true,
    symbol: "BINANCE:BTCUSDT.P",
    interval: "60",
    timezone: "Europe/Istanbul",
    theme: "dark",
    style: "1",
    locale: "tr",
    toolbar_bg: "#0a0e0d",
    enable_publishing: false,
    hide_side_toolbar: false,
    allow_symbol_change: false,
    container_id: "tradingview_chart",
    studies: ["RSI@tv-basicstudies", "Volume@tv-basicstudies"],
    show_popup_button: false,
  });
}

// ============ SINYAL GECMISI ============
function actionToCls(action) {
  if (!action) return '';
  if (action.includes('LONG_OPEN')) return 'long';
  if (action.includes('SHORT_OPEN')) return 'short';
  if (action.includes('CLOSE')) return 'close';
  if (action.includes('PARTIAL')) return 'partial';
  if (action.includes('TIGHTEN')) return 'tighten';
  if (action.includes('ADD')) return 'add';
  return '';
}

function fmtSignalTime(ts) {
  if (!ts) return '--';
  try {
    const d = new Date(ts);
    const now = new Date();
    const diff = (now - d) / 1000; // saniye
    if (diff < 60) return `${Math.floor(diff)}s once`;
    if (diff < 3600) return `${Math.floor(diff/60)}dk once`;
    if (diff < 86400) return `${Math.floor(diff/3600)}sa once`;
    if (diff < 604800) return `${Math.floor(diff/86400)}g once`;
    return d.toLocaleDateString('tr-TR', {day:'2-digit', month:'2-digit', hour:'2-digit', minute:'2-digit'});
  } catch { return ts.substring(0, 16); }
}

async function loadSignalHistory() {
  const wrap = document.getElementById('signalsWrap');
  try {
    const r = await fetch('/api/signals?symbol=BTC&limit=50');
    const data = await r.json();
    if (!data.ok) throw new Error(data.error || 'API hatasi');

    if (!data.supabaseEnabled) {
      wrap.innerHTML = '<div class="signals-empty">Supabase yapilandirilmamis - sinyal kaydi yok</div>';
      return;
    }
    if (!data.signals || data.signals.length === 0) {
      wrap.innerHTML = '<div class="signals-empty">Hic sinyal yok. Sistem yeni baslatildi, sinyaller olustukca burada gorunecek.</div>';
      return;
    }
    wrap.innerHTML = data.signals.map(s => {
      const cls = actionToCls(s.action);
      const score = +s.total_score;
      const scoreCls = score > 0 ? 'pos' : (score < 0 ? 'neg' : '');
      const scoreText = (score >= 0 ? '+' : '') + score.toFixed(2);
      const priceText = s.price ? '$' + (+s.price).toLocaleString('en-US', {maximumFractionDigits: 0}) : '--';
      const ks = [];
      if (s.killshot_15m) ks.push('15m');
      if (s.killshot_1h) ks.push('1h');
      if (s.killshot_4h) ks.push('4h');
      const ksText = ks.length ? `<b>KILLSHOT:</b> ${ks.join(', ')}` : '';
      return `<div class="signal-row">
        <span class="sig-time">${fmtSignalTime(s.ts)}</span>
        <span class="sig-action ${cls}">${s.action_label || s.action}</span>
        <span class="sig-score ${scoreCls}">${scoreText}</span>
        <span class="sig-meta">${priceText} ${ksText ? '\u00b7 ' + ksText : ''}</span>
      </div>`;
    }).join('');
  } catch (e) {
    wrap.innerHTML = `<div class="signals-empty">Yuklenirken hata: ${e.message}</div>`;
  }
}

// ============ BACKTEST ============
let btSelectedDays = 7;
let btChart = null;
let btCandleSeries = null;

document.querySelectorAll('.bt-day-btn').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('.bt-day-btn').forEach(x => x.classList.remove('active'));
    b.classList.add('active');
    btSelectedDays = +b.dataset.days;
  });
});

document.getElementById('btRunBtn').addEventListener('click', runBacktest);

function fmtBtTime(ts) {
  const d = new Date(ts);
  return d.toLocaleString('tr-TR', {month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit'});
}

function initBtChart() {
  if (btChart) return;
  const el = document.getElementById('btChart');
  btChart = LightweightCharts.createChart(el, {
    width: el.clientWidth,
    height: el.clientHeight,
    layout: {
      background: { color: '#0f1413' },
      textColor: '#d4dcd9',
      fontFamily: 'JetBrains Mono, monospace',
    },
    grid: {
      vertLines: { color: '#14201d' },
      horzLines: { color: '#14201d' },
    },
    rightPriceScale: { borderColor: '#1f2a28' },
    timeScale: { borderColor: '#1f2a28', timeVisible: true, secondsVisible: false },
    crosshair: { mode: 1 },
  });
  btCandleSeries = btChart.addLineSeries({
    color: '#6df5d4',
    lineWidth: 2,
    priceLineVisible: false,
    lastValueVisible: false,
  });

  // Resize listener
  window.addEventListener('resize', () => {
    if (btChart && el) btChart.applyOptions({ width: el.clientWidth });
  });
}

async function runBacktest() {
  const btn = document.getElementById('btRunBtn');
  btn.disabled = true;
  btn.textContent = 'CALISIYOR...';

  try {
    const r = await fetch(`/api/backtest?symbol=BTC&days=${btSelectedDays}`);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const data = await r.json();
    if (!data.ok) throw new Error(data.error || 'API hatasi');

    renderBacktest(data);
  } catch (e) {
    document.getElementById('btSummary').style.display = 'block';
    document.getElementById('btSummary').innerHTML = `<div style="color:var(--red)">Hata: ${e.message}</div>`;
  } finally {
    btn.disabled = false;
    btn.textContent = 'BACKTEST CALISTIR >';
  }
}

function renderBacktest(data) {
  // Ozet
  const s = data.summary;
  const sumEl = document.getElementById('btSummary');
  sumEl.style.display = 'block';

  function statHtml(label, info) {
    if (!info) return `<div class="bt-stat"><div class="bt-stat-label">${label}</div><div class="bt-stat-value" style="color:var(--text-faint)">--</div></div>`;
    const wrCls = info.winRate >= 50 ? 'pos' : 'neg';
    const avgCls = info.avgPnl >= 0 ? 'pos' : 'neg';
    return `<div class="bt-stat">
      <div class="bt-stat-label">${label}</div>
      <div class="bt-stat-value ${wrCls}">${info.winRate}%</div>
      <div class="bt-stat-sub"><span class="${avgCls}">${info.avgPnl >= 0 ? '+' : ''}${info.avgPnl}%</span> avg / ${info.wins}/${info.total}</div>
    </div>`;
  }

  sumEl.innerHTML = `
    <div class="bt-summary-grid">
      <div class="bt-stat">
        <div class="bt-stat-label">TOPLAM SINYAL</div>
        <div class="bt-stat-value">${s.totalSignals}</div>
        <div class="bt-stat-sub">${data.days} gun, ${data.dataPoints1h} 1h nokta</div>
      </div>
      <div class="bt-stat">
        <div class="bt-stat-label">LONG / SHORT</div>
        <div class="bt-stat-value">${s.longOpen} / ${s.shortOpen}</div>
      </div>
      ${statHtml('LONG @ 1H', s.long_1h)}
      ${statHtml('LONG @ 4H', s.long_4h)}
      ${statHtml('LONG @ 24H', s.long_24h)}
      ${statHtml('SHORT @ 1H', s.short_1h)}
      ${statHtml('SHORT @ 4H', s.short_4h)}
      ${statHtml('SHORT @ 24H', s.short_24h)}
    </div>
  `;

  // Grafik
  initBtChart();
  if (data.candles && data.candles.length) {
    // LightweightCharts time formatinda: saniye (UNIX timestamp)
    const lineData = data.candles.map(c => ({ time: c.time, value: c.close }));
    btCandleSeries.setData(lineData);

    // Sinyal markerlari
    const markers = (data.signals || []).map(sig => {
      const time = Math.floor(sig.ts / 1000);
      let position, color, shape, text;
      if (sig.action === 'LONG_OPEN') {
        position = 'belowBar'; color = '#00d09c'; shape = 'arrowUp'; text = 'L';
      } else if (sig.action === 'SHORT_OPEN') {
        position = 'aboveBar'; color = '#ff4d6d'; shape = 'arrowDown'; text = 'S';
      } else {
        return null;
      }
      return { time, position, color, shape, text };
    }).filter(m => m).sort((a, b) => a.time - b.time);

    btCandleSeries.setMarkers(markers);
    btChart.timeScale().fitContent();
  }

  // Sinyal listesi
  const listEl = document.getElementById('btSignalsList');
  if (!data.signals || data.signals.length === 0) {
    listEl.innerHTML = '<div class="signals-empty">Bu donemde anlamli sinyal yok</div>';
    return;
  }

  function pnlCell(p, extraClass) {
    if (p == null) return `<span class="pnl ${extraClass || ''}" style="color:var(--text-faint)">--</span>`;
    const cls = p >= 0 ? 'pos' : 'neg';
    const sign = p >= 0 ? '+' : '';
    return `<span class="pnl ${cls} ${extraClass || ''}">${sign}${p}%</span>`;
  }

  listEl.innerHTML = '<div class="bt-sig-row header">' +
    '<div>TIME</div><div>ACTION</div><div>SCORE</div>' +
    '<div class="pnl-1h">+1H</div><div>+4H</div><div>+24H</div></div>' +
    data.signals.reverse().map(sig => {
      const cls = actionToCls(sig.action);
      const scoreCls = sig.totalScore >= 0 ? 'pos' : 'neg';
      return `<div class="bt-sig-row">
        <span class="sig-time">${fmtBtTime(sig.ts)}</span>
        <span class="sig-action ${cls}">${sig.actionLabel}</span>
        <span class="sig-score ${scoreCls}">${sig.totalScore >= 0 ? '+' : ''}${sig.totalScore}</span>
        ${pnlCell(sig.pnl1h, 'pnl-1h')}
        ${pnlCell(sig.pnl4h)}
        ${pnlCell(sig.pnl24h)}
      </div>`;
    }).join('');
}

// Init
loadPosition();
run();
// TradingView 200ms sonra yukle (tv.js async)
setTimeout(initTradingView, 200);
// Sinyal gecmisini yukle ve her 60 saniyede bir tazele
loadSignalHistory();
setInterval(() => { if (!document.hidden) loadSignalHistory(); }, 60000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
