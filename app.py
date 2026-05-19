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
USER_AGENT = "Mozilla/5.0 LSDecisionEngine/4.0"

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
    "cvd_buy_strong":  55.0,
    "cvd_sell_strong": 45.0,
    # Pozisyon tavsiye esikleri
    "counter_weak":     1.8,
    "counter_medium":   2.7,
    "counter_strong":   4.5,
    "same_strong":      4.5,
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

    # 6) CVD / Taker Buy-Sell Ratio
    # buySellRatio = taker buy volume / taker sell volume
    # buyVol/(buyVol+sellVol) = taker buy yuzdesi
    taker_buy_pct = None
    cvd_url = f"https://fapi.binance.com/futures/data/takerlongshortRatio?symbol={sym}&period={p}&limit=1"
    cvd_data = safe(lambda: http_get(cvd_url), [])
    if cvd_data and len(cvd_data) >= 1:
        try:
            buy_vol = float(cvd_data[-1].get("buyVol") or 0)
            sell_vol = float(cvd_data[-1].get("sellVol") or 0)
            total = buy_vol + sell_vol
            if total > 0:
                taker_buy_pct = buy_vol / total * 100
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


def decide(symbol, tf_results, user_position, entry_price=None, current_price=None):
    """Multi-TF agirlikli skor + pozisyon farkindaligi + 1h teyit + PnL."""
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
        "reasons": all_reasons[:10],
        "pnlPct": pnl_pct,
        "h1Verdict": h1_verdict,
    }


def _decide_for_long(total, h1_verdict, pnl_pct):
    """LONG pozisyondayken karar verir."""
    # Karsi sinyal (negatif skor) gucu
    if total <= -THRESHOLDS["counter_strong"]:
        # Cok guclu ters sinyal - tamamen kapat
        if pnl_pct is not None and pnl_pct > 0:
            return "CLOSE_TP", "KAR AL - TAMAMEN KAPAT", \
                   f"Guclu short sinyali (skor {total:.1f}), kardayken kapat (+{pnl_pct:.2f}%)"
        elif pnl_pct is not None and pnl_pct < -2:
            return "CLOSE_SL", "ZARARI KES - TAMAMEN KAPAT", \
                   f"Guclu short sinyali (skor {total:.1f}), zarar artiyor ({pnl_pct:.2f}%)"
        else:
            return "CLOSE", "TAMAMEN KAPAT", \
                   f"Guclu short sinyali olustu (skor {total:.1f})"
    if total <= -THRESHOLDS["counter_medium"]:
        # Orta ters sinyal - yarisini kapat
        return "PARTIAL_CLOSE", "KISMI KAPAT %50", \
               f"Ters sinyal guc kazaniyor (skor {total:.1f}), riski azalt"
    if total <= -THRESHOLDS["counter_weak"]:
        # Hafif ters sinyal - stop yaklastir
        return "TIGHTEN_STOP", "STOPU YAKLASTIR", \
               f"Hafif ters sinyal (skor {total:.1f}), stop'unu break-even'a cek"
    if total >= THRESHOLDS["same_strong"] and h1_verdict == "LONG":
        # Ayni yon guclu sinyal - pozisyon artirma firsati
        return "ADD_POSITION", "POZISYON ARTIR", \
               f"Long yonde guclu onay (skor {total:.1f}), DCA / piramitleme firsati"
    # Geri kalan: pozisyonu tut
    return "HOLD", "TUT", f"Long pozisyon korunuyor (skor {total:.1f})"


def _decide_for_short(total, h1_verdict, pnl_pct):
    """SHORT pozisyondayken karar verir."""
    # Karsi sinyal (pozitif skor) gucu
    if total >= THRESHOLDS["counter_strong"]:
        if pnl_pct is not None and pnl_pct > 0:
            return "CLOSE_TP", "KAR AL - TAMAMEN KAPAT", \
                   f"Guclu long sinyali (skor +{total:.1f}), kardayken kapat (+{pnl_pct:.2f}%)"
        elif pnl_pct is not None and pnl_pct < -2:
            return "CLOSE_SL", "ZARARI KES - TAMAMEN KAPAT", \
                   f"Guclu long sinyali (skor +{total:.1f}), zarar artiyor ({pnl_pct:.2f}%)"
        else:
            return "CLOSE", "TAMAMEN KAPAT", \
                   f"Guclu long sinyali olustu (skor +{total:.1f})"
    if total >= THRESHOLDS["counter_medium"]:
        return "PARTIAL_CLOSE", "KISMI KAPAT %50", \
               f"Ters sinyal guc kazaniyor (skor +{total:.1f}), riski azalt"
    if total >= THRESHOLDS["counter_weak"]:
        return "TIGHTEN_STOP", "STOPU YAKLASTIR", \
               f"Hafif ters sinyal (skor +{total:.1f}), stop'unu break-even'a cek"
    if total <= -THRESHOLDS["same_strong"] and h1_verdict == "SHORT":
        return "ADD_POSITION", "POZISYON ARTIR", \
               f"Short yonde guclu onay (skor {total:.1f}), DCA / piramitleme firsati"
    return "HOLD", "TUT", f"Short pozisyon korunuyor (skor {total:.1f})"


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
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_bb = pool.submit(bybit_summary, sym)
        f_okx = pool.submit(okx_summary, symbol)
        f_bg = pool.submit(bitget_summary, sym)
        try: exchanges["Bybit"] = f_bb.result()
        except Exception as e: exchanges["Bybit"] = {"ok": False, "error": str(e)}
        try: exchanges["OKX"] = f_okx.result()
        except Exception as e: exchanges["OKX"] = {"ok": False, "error": str(e)}
        try: exchanges["Bitget"] = f_bg.result()
        except Exception as e: exchanges["Bitget"] = {"ok": False, "error": str(e)}
    timeframes_out = {}
    for tf, d in tf_results.items():
        if d and d.get("ok"):
            v = compute_tf_verdict(d, tf)
            timeframes_out[tf] = {**d, "scores": v["scores"], "verdict": v["verdict"]}
        else:
            timeframes_out[tf] = {"ok": False, "error": (d or {}).get("error", "no data")}
    decision = decide(symbol, tf_results, user_position, entry_price, current_price)
    return {"ok": True, "symbol": symbol, "userPosition": user_position,
            "entryPrice": entry_price, "currentPrice": current_price,
            "decision": decision, "timeframes": timeframes_out, "exchanges": exchanges}


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
  <h2 class="section"><span class="arrow">&#9656;</span>EXCHANGE BREAKDOWN (1h)</h2>
  <div class="grid" id="cards"></div>
  <div class="info">
    <b>v4 - BTC ICIN OPTIMIZE:</b> 5 metrik (OI/fiyat, whale-retail, retail asiri pozisyon, funding, CVD) + multi-TF + pozisyon farkindaligi.<br><br>
    <b>NASIL CALISIR?</b> 3 TF (15m, 1h, 4h) skorlanir, agirlikli toplanir (4h x2.5, 1h x2.0, 15m x0.5). 1D timeframe sistem disindadir - kullanici manuel/gozle takip eder.<br><br>
    <b>POZISYON BAZLI TAVSIYELER:</b><br>
    &bull; <b>FLAT</b>: Sadece guclu sinyal + 1h teyit varsa LONG/SHORT AC.<br>
    &bull; <b>LONG/SHORT pozisyonda</b>: Giris fiyatini gir, sistem PnL hesaplar. Ters sinyal gucune gore: STOPU YAKLASTIR / KISMI KAPAT %50 / TAMAMEN KAPAT (kar al / zarari kes). Ayni yon guclu sinyalde: POZISYON ARTIR.<br><br>
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
loadPosition();
run();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
