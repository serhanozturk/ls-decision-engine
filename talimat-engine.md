# SERHAN — LS DECISION ENGINE (Proje Talimatlari)

## ==========================================================
## #1 KIRMIZI CIZGI — BINANCE BAN (HER SEYIN ONUNDE)
## ==========================================================
BINANCE BAN BIZIM KIRMIZI CIZGIMIZDIR. Kod yazarken, guncellerken veya
ozellik eklerken HER ZAMAN once "bu degisiklik ban yedirir mi?" diye sor.
Ban riski olan hicbir kod, ozellik ne kadar degerli olursa olsun, YAZILMAZ.

Kod yazmadan ONCE ban kontrol listesi:
1. Bu degisiklik Binance'e kac yeni istek ekler?
2. Endpoint weight'i ne? Genel limit: 2400 weight/dakika/IP.
3. Yeni endpoint ekliyorsam weight hesabi yapildi mi?
4. Ban riski belirsizse: ONCE soyle, hesaplayip ondan sonra yaz.

## KIMLIK VE ILETISIM
- Turkce yanit ver. Net, oz, dogrudan ol — gereksiz uzatma.
- Sistem etiketleri / kod ciktilari ASCII-safe olmali (Turkce ozel karakter YOK).
- Swing trader (cogunlukla 1g-1hafta, 1h'de acilis). Perpetual futures'ta deneyimli.

## EN ONEMLI KURAL — IZIN OLMADAN KOD YAZMA
- "yaz", "basla", "devam" gibi ACIK onay gelmeden KOD YAZMA/degistirme.
- Once tasarimi tartis, netlestir, soru sor. Onay gelince yaz.
- Tek seferde bir soru sor; ask_user_input ile secenekli sor (mobil kolayligi).

## SADECE DEGISECEK YERE DOKUN
- Bir duzeltme/ekleme yaparken SADECE o degisiklikle ilgili satirlari degistir.
- "Iyilestirmek/temizlemek" icin istenmeden degisiklik YAPMA.
- Degisiklik oncesi: bu satir baska neyi etkiler? Yan etki var mi? KONTROL ET
  (orn v13'te kline limiti degisti ama fiyat/hacim/kisa EMA sondan aldigi icin
  ETKILENMEDI — once bunu dogruladik, sonra yazdik).
- Degisiklikten sonra: degismemesi gereken seyler AYNI MI test et.

## BU CHAT: SADECE ENGINE
Bu chat yalnizca Engine icin kullanilir.
Screener ve Terminal AYRI chatlerde, AYRI talimatlarda konusulur — ASLA karistirma.

## GITHUB = GERCEK KAYNAK (kod isine baslamadan once CEK)
- Repo PUBLIC: https://github.com/serhanozturk/ls-decision-engine
- Guncel kod:    https://raw.githubusercontent.com/serhanozturk/ls-decision-engine/main/Engine.py
- Guncel talimat: https://raw.githubusercontent.com/serhanozturk/ls-decision-engine/main/talimat-engine.md
- Kod degisikligine baslamadan ONCE Engine.py'yi bu adresten cek (curl ile) —
  project knowledge'daki kopya ESKI olabilir, GitHub'daki deploy edilen gercek koddur.
- Talimatta suphe varsa talimati da GitHub'dan cek.
- Claude token ile YAZAMAZ; ama Chrome uzerinden (kullanici GitHub'da oturum acikken,
  kullanici onayiyla) dosya olusturup/duzenleyip commit EDEBILIR. Varsayilan: kullanici
  commit eder; kullanici isterse "GitHub'a sen yukle" der, Claude Chrome ile yapar.

## PROJE BILGILERI
- **Dosya:** Engine.py (isim ASLA degismez)
- **Repo:** ls-decision-engine
- **Altyapi:** Hetzner VPS (Falkenstein), Coolify, Dockerfile deploy
- **Link:** http://5.75.226.135:8768
- **Env:** SUPABASE_URL, SUPABASE_KEY (Python client /rest/v1/ kendi ekler — URL'e EKLEME), PORT=8768
- **Start:** python Engine.py
- **Guncel surum:** v13

### Surum gecmisi (ozet)
- v11: STALE korumasi, kontrat OI
- v12: ban cap kok duzeltme + thread guvenligi + kline guard + snapshot izolasyonu + canli fiyat durustlugu
- v13: EMA365 kok duzeltme — kline limiti 400→1500 (EMA365 dogru isinmasi icin)

## NE YAPAR
BTC perpetual futures icin piyasa rejimi siniflandirmasi ve trading sinyali uretimi.
- Timeframe: H1
- Rejim: EMA365/EMA50, 2 mum dogrulama
- Sinyal tipleri: LONG, TEPKI LONG, GUCLU TEPKI LONG ve SHORT varyantlari
- 5 kosullu boga/ayi puanlama sistemi
- EMA7/30 kesisim giris/cikis
- Supabase'e snapshot + signal kaydeder

## EMA WARMUP KURALI (kritik)
- limit=400 → sadece 399 kapanmis mum → EMA365 icin yetersiz isinma → YANLIS deger
- limit=1500 kullan (Binance tek istekte max). EMA365 dogru isinir.
- Fiyat/EMA7/EMA30/EMA50/hacim sondan okunur → limit artisi bunlari ETKILEMEZ.
- TradingView farki: limit=400'de EMA365 sapma buyuk, 1500'de minimal — DOGRU budur.

## SINYAL MANTIGI — ACIK KONU (KOD YAZMADAN KARAR VER)
emaCrossDown aslinda "kesisim" degil "KONUM" olcuyor:
- Mevcut: EMA7 son 2 mumdur EMA30 ALTINDA mi? → dusus surerken surekli True
- Sonuc: "GUCLU SHORT AC" fiyat surekli dususteyse tekrar tekrar tetiklenir
- Karar verilecek:
  (a) SHORT sadece TAZE kesisim aninda (EMA7 EMA30'u YENI kesince)
  (b) Mevcut "EMA7 altta + skor" mantigi dogru — degistirilmeyecek
- Ayrica: dedup (ayni action tekrar yazilmaz) dogru mu? Her durum degisikligi
  kaydedilsin mi?
KOD YAZMADAN ONCE tasarim karari verilecek.

## SUPABASE
- URL env: SUPABASE_URL (orn https://xyz.supabase.co — /rest/v1/ EKLEME, client ekler)
- signals tablosu: action, price, regime, score, timestamp vb.
- snapshot tablosu: ayri schema

## CRON CIZELGESI (cron-job.org)
- Snapshot: /api/snapshot-save → dakika 2,32
- BTC Check: /api/cron-check → dakika 7,37
- Keep-alive: GEREK YOK (Hetzner'de uyku/suspend yok)
- cron-job.org bazen :00 kabul etmez; :01 gibi kaydir
- Cron URL'leri: http://5.75.226.135:8768/api/snapshot-save ve http://5.75.226.135:8768/api/cron-check

## BINANCE API KURALLARI
- **Ban tracker zorunlu:** 418/429 yiyince yerel takip, Binance'e gitmeyi kes.
  Retry-After header VARSA tam suresine uy (cap yok); yoksa default 30dk.
  Header'i kisa cap'e KIRPMA — uzun banlarda erken istek atip bani UZATIR.
- **Endpoint weight'leri:**
  - klines (H1, limit=1500) = weight 10 (limit/100, min 1)
  - premiumIndex symbol'SUZ = 10
  - exchangeInfo = 1
- **Genel weight limiti:** 2400 weight/dakika/IP.
- **Eskalasyonlu ban:** 2dk → 3 gune kadar.
- **Ban kalkma:** Coolify'dan servisi durdur → sure dolar → yeniden baslat.
  Banliyken istek atmak sureyi UZATIR.
- **IP izolasyonu:** Hetzner VPS = sabit, kendi IP, paylasimli havuz riski yok.
  Screener farkli VPS'te (178.104.143.245), Engine ayri VPS'te (5.75.226.135) — IP izole.

## TEKNIK STANDARTLAR
- Python STANDART KUTUPHANE ONLY. pip install YOK. requirements.txt yok.
- HTML/CSS/JS Engine.py icine gomulu (tek dosya).
- Tum borsa cagrilari: ban tracker + cache + retry zorunlu.
- Thread guvenligi: global state Lock ile korunmali.
- HTML gomerken JS emoji'leri: \U0001F680 gibi 8-hane veya dogrudan emoji kullan.
- **GECE/GUNDUZ MODU STANDART:** CSS degiskenleri (:root koyu + body.light acik),
  header'da tema butonu (ay/gunes ikonu), localStorage sakla, varsayilan KOYU.
  Grafik varsa tema-duyarli olmali.

## VERI DOGRULAMA AKISI (snapshot kontrolu istenince)
- **OI (kontrat):** sistem oi_change vs Binance (kontrat_now - kontrat_prev)/prev. Onceki mum sart.
- **CVD:** sistem cvd vs Binance taker_buy/(buy+sell)*100.
- **Retail:** sistem global vs Hyblock retail. ZAMAN ESLESMESI kritik (TR vs UTC karistirma).
- **W/R:** sistem (whale-global) vs Hyblock wr-delta.
- STALE: ardisik fiyat+cvd tekrari = bayat. v11+ sonrasi sifir olmali.
- TR = UTC+3. Sistem PREVIOUS (kapanmis) mumu gosterir; terminal CANLI mumu.
- Son dogrulama (v11, 130 snapshot): OI 6/6, CVD 5/5, Retail sapma 0.29, W/R 0.32 — SAGLIKLI.

## IS AKISI ("yaz" gelince)
1. Guncel kodu GitHub'dan cek (project knowledge KULLANMA — eski olabilir):
   curl -s https://raw.githubusercontent.com/serhanozturk/ls-decision-engine/main/Engine.py -o /home/claude/Engine.py
2. Sadece degisecek satirlari duzenle (str_replace)
3. python3 -m py_compile ile syntax kontrol
4. Gercek server testi (http.client localhost) — Binance 403/418 container'da NORMAL
5. /mnt/user-data/outputs/Engine.py'ye kopyala
6. present_files ile sun

## DEPLOY (kullaniciya hatirlat)
- GitHub ls-decision-engine reposu → Engine.py → tumunu sec → sil → yeni kodu yapistir → TEK commit
- Coolify otomatik deploy eder (Dockerfile mevcut, repo'da var)
- Deploy sonrasi loglarda "vXX listening" gor = dogrulama
- Erisim: http://5.75.226.135:8768

## DOCKERFILE (repo'da mevcut, degistirme)
```
FROM python:3.12-slim
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Engine.py .
EXPOSE 8768
CMD ["python", "Engine.py"]
```

## BEKLEYEN / ACIK KONULAR (oncelik sirasi)
1. **dataFresh=false iken sinyal uretme (EN YUKSEK ONCELIK):** ban/bayat veri
   durumunda karar motoru eski cache ile REBOUND_LONG gibi sinyal uretmeye devam
   ediyor. dataFresh=false ise karar "BEKLE - VERI BAYAT" olmali.
2. TEPKI LONG min skor: su an bull 0/5 ile bile tetikleniyor (normal giris 3/5
   isterken trende ters tepki 0/5'le aciliyor). Min 1-2 esik tartisilacak.
3. emaCrossDown cross vs konum: mevcut mantik "kesisim" degil "KONUM" olcuyor
   (EMA7 2 mumdur altta = surekli True). Taze kesisim ayrimi tartisilacak.
4. cron-check hep "flat" calisir → CLOSE_TP/CLOSE_SL sinyal gecmisine ASLA
   dusmez (sadece kullanici dashboard'da long/short seciliyken uretilir).
   Bilincli mi karar verilecek.
5. whale2ago/retail2ago alan adlari yaniltici: icerik 1 mum oncesi (isim v11'den
   kalma). Fonksiyonel hata degil, dogrulama sirasinda karistirma.
6. Telegram bildirimi: guclu sinyaller + TEPKI/DUZELTME + kapanislar, detayli mesaj
   (skor/fiyat/rejim/W-R/OI/CVD/mum), dedup'a bagli. Token Coolify env'de, KODA gomulmez.
7. Bot execution modulu (tasarim tamam, implementasyon bekliyor): Architecture
   Option A — Engine.py icine eklenecek. Binance Futures Hedge Mode, 10x kaldirac,
   100 USDT ana / 50 USDT ters-hedge kademeleri, EMA365 hard stop + KAPAT sinyali
   cift mekanizma, ayri /api/bot-check endpoint (snapshot cron'u ile ayni takvim).
