#!/usr/bin/env python3
"""
Horizon Scraper
Skenira oglase nekretnina sa Oglasi.me i Patuljak.me objavljene od
ponedeljka tekuće sedmice, filtrira posrednike i šalje email sa vlasnicima.

Instalacija : pip install requests beautifulsoup4 lxml resend
Pokretanje  : python horizon_scraper.py
"""

import re
import time
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────────────────────
# KONFIGURACIJA
# ──────────────────────────────────────────────────────────────

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "re_KKvP9n9w_4kuVE1Cjk2mvvEqn4sXfhcfm")
EMAIL_TO       = "officehorizon.nekretnine@gmail.com"
# EMAIL_FROM mora biti sa verifikovanog domena na Resend nalogu
EMAIL_FROM     = os.environ.get("EMAIL_FROM", "onboarding@resend.dev")

BROKER_KEYWORDS     = ["nekretnine", "real estate", "realty",
                       "agencija", "agency", "d.o.o", "doo", "invest", "promet",
                       "property", "montenegro", "agent",
                       "proart", "prestige", "millennium", "toka",
                       "bestate4me", "bestate"]
BROKER_MIN_LISTINGS = 3     # ≥ 3 oglasa → posrednik

# Blacklista telefona — uvijek se filtriraju bez obzira na broj oglasa
BROKER_PHONES = {
    "+38267580584",   # profil /11943/
    "+38267447444",   # profil /37898/
    "+38268150115",   # profil /57706/
    "+38267347963",
}

# Blacklista po imenu — tačno podudaranje (case-insensitive)
BROKER_NAMES = {
    "nemanja krstović",
}

# Grad koji skeniramo (prazan string = svi gradovi)
FILTER_CITY = "Podgorica"

MAX_PAGES      = 15         # max stranica po sajtu
DELAY_LISTING  = 1.5        # pauza između posjeta pojedinačnim oglasima (sek)
DELAY_PROFILE  = 1.0        # pauza između posjeta profilima
MAX_OLD_IN_ROW = 5          # zaustavi paginaciju nakon N uzastopnih starih oglasa

# ──────────────────────────────────────────────────────────────
# CUTOFF DATUM  (zadnjih 24 sata)
# ──────────────────────────────────────────────────────────────

CUTOFF = datetime.now() - timedelta(hours=24)

# ──────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)
log.info("Cutoff datum: %s (zadnjih 24h)", CUTOFF.strftime("%d.%m.%Y %H:%M"))

http = requests.Session()
http.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "bs,hr,sr;q=0.9,en;q=0.8",
})

# ──────────────────────────────────────────────────────────────
# PARSIRANJE DATUMA
# ──────────────────────────────────────────────────────────────

_RELATIVE = [
    (re.compile(r"prije\s+(\d+)\s+sek"),       lambda n: timedelta(seconds=n)),
    (re.compile(r"prije\s+(\d+)\s+min"),        lambda n: timedelta(minutes=n)),
    (re.compile(r"prije\s+(\d+)\s+h\b"),        lambda n: timedelta(hours=n)),
    (re.compile(r"prije\s+(\d+)\s+dan"),        lambda n: timedelta(days=n)),
    (re.compile(r"prije\s+(\d+)\s+sedmic"),     lambda n: timedelta(weeks=n)),
    (re.compile(r"prije\s+(\d+)\s+mjes"),       lambda n: timedelta(days=n * 30)),
    (re.compile(r"prije\s+(\d+)\s+godin"),      lambda n: timedelta(days=n * 365)),
]
_ABSOLUTE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})")


def parse_date(text: str) -> datetime | None:
    """Pretvara tekstualni datum u datetime. Vraća None ako ne može."""
    text = text.lower().strip()
    now  = datetime.now()

    for pattern, delta_fn in _RELATIVE:
        m = pattern.search(text)
        if m:
            return now - delta_fn(int(m.group(1)))

    m = _ABSOLUTE.search(text)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        try:
            return datetime(y, mo, d)
        except ValueError:
            return None

    return None


def is_recent(text: str) -> bool | None:
    """True = oglas od ponedeljka ili noviji. None = datum nije pronađen."""
    dt = parse_date(text)
    if dt is None:
        return None
    return dt >= CUTOFF

# ──────────────────────────────────────────────────────────────
# POMOĆNE FUNKCIJE
# ──────────────────────────────────────────────────────────────

def is_broker(name: str, listing_count: int, phone: str = "") -> bool:
    name_l = name.lower().strip()
    if name_l in BROKER_NAMES:
        return True
    if any(kw in name_l for kw in BROKER_KEYWORDS):
        return True
    if phone and phone.replace(" ", "") in {p.replace(" ", "") for p in BROKER_PHONES}:
        return True
    return listing_count >= BROKER_MIN_LISTINGS


def clean_price(raw: str) -> str:
    return re.sub(r"\s+", " ", raw.replace("\xa0", " ")).strip()


def get(url: str) -> requests.Response | None:
    try:
        r = http.get(url, timeout=15)
        r.raise_for_status()
        return r
    except requests.RequestException as e:
        log.warning("  GET failed: %s  →  %s", url, e)
        return None

# ──────────────────────────────────────────────────────────────
# SLANJE EMAILA (Resend API)
# ──────────────────────────────────────────────────────────────

def send_email(leads: list[dict]) -> None:
    datum = datetime.now().strftime("%d.%m.%Y")

    if not leads:
        log.info("Nema novih vlasnika — email se ne šalje.")
        return

    rows = "\n".join(
        f"""        <tr>
          <td style="padding:8px;border:1px solid #ddd">{lead['ime']}</td>
          <td style="padding:8px;border:1px solid #ddd">{lead['cijena']}</td>
          <td style="padding:8px;border:1px solid #ddd">{lead['lokacija']}</td>
          <td style="padding:8px;border:1px solid #ddd">
            <a href="{lead['oglas_link']}">{lead['oglas_link']}</a>
          </td>
        </tr>"""
        for lead in leads
    )

    html = f"""<!DOCTYPE html>
<html lang="bs"><body style="font-family:Arial,sans-serif;color:#333">
<h2 style="color:#1a1a1a">Horizon Scraper — {datum}</h2>
<p>Pronađeno <strong>{len(leads)}</strong> vlasnik(a) u zadnjih 24 sata (od {CUTOFF.strftime('%d.%m.%Y %H:%M')}):</p>
<table style="border-collapse:collapse;width:100%;font-size:14px">
  <thead>
    <tr style="background:#f4f4f4">
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Ime vlasnika</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Cijena</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Lokacija</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Link oglasa</th>
    </tr>
  </thead>
  <tbody>
{rows}
  </tbody>
</table>
<p style="color:#888;font-size:12px;margin-top:24px">
  Izvor: Oglasi.me + Patuljak.me &nbsp;|&nbsp; Horizon Scraper
</p>
</body></html>"""

    text_lines = [f"Horizon Scraper — {datum}", f"Vlasnici ({len(leads)}):", ""]
    for lead in leads:
        text_lines.append(f"Ime:      {lead['ime']}")
        text_lines.append(f"Cijena:   {lead['cijena']}")
        text_lines.append(f"Lokacija: {lead['lokacija']}")
        text_lines.append(f"Oglas:    {lead['oglas_link']}")
        text_lines.append("")

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from":    EMAIL_FROM,
            "to":      [EMAIL_TO],
            "subject": f"Horizon Scraper — {len(leads)} vlasnik(a) — {datum}",
            "html":    html,
            "text":    "\n".join(text_lines),
        },
        timeout=30,
    )

    if resp.ok:
        log.info("✓  Email poslan na %s  (%d vlasnika).", EMAIL_TO, len(leads))
    else:
        log.error("✗  Greška pri slanju emaila: %s  %s", resp.status_code, resp.text)

# ──────────────────────────────────────────────────────────────
# OGLASI.ME
# ──────────────────────────────────────────────────────────────

OGLASI_BASE = "https://www.oglasi.me"


def _oglasi_cards(soup: BeautifulSoup) -> list[tuple[str, str, str]]:
    seen: set[str] = set()
    out:  list[tuple[str, str, str]] = []

    for card in soup.find_all("div", class_=re.compile(r"\boglasi-item-tekst\b")):
        link = card.find("a", class_="oglasi-item-heading", href=True)
        if not link:
            continue
        url = urljoin(OGLASI_BASE, link["href"].split("?")[0])
        if url in seen:
            continue
        seen.add(url)

        vreme     = card.find("div", class_="oglasi-vreme")
        date_text = vreme.get_text(strip=True) if vreme else ""

        mesto_tag = card.find("div", class_="oglasi-mesto")
        grad      = mesto_tag.get_text(strip=True) if mesto_tag else ""

        out.append((url, date_text, grad))

    return out


def _oglasi_user_count(user_id: str) -> int:
    r = get(f"{OGLASI_BASE}/korisnik/{user_id}")
    if r is None:
        return 1
    text = BeautifulSoup(r.text, "lxml").get_text()
    m    = re.search(r"(\d+)\s*oglas", text, re.I)
    return int(m.group(1)) if m else 1


def _parse_oglasi_listing(url: str) -> dict | None:
    r = get(url)
    if r is None:
        return None

    soup = BeautifulSoup(r.text, "lxml")

    page_text = soup.get_text(" ")
    date_m    = _ABSOLUTE.search(page_text)
    if date_m:
        listing_dt = parse_date(date_m.group(0))
        if listing_dt is not None and listing_dt < CUTOFF:
            log.debug("  [star oglas] %s  datum: %s", url, date_m.group(0))
            return None

    name_tag = soup.select_one("p.sidebar-user-status-name")
    if not name_tag:
        return None
    raw_name = name_tag.get_text(" ", strip=True)
    name     = re.sub(r"\s*\(.*?\)", "", raw_name).strip()
    uid_m    = re.search(r"\((\w+)\)", raw_name)
    user_id  = uid_m.group(1) if uid_m else None

    loc_tag  = soup.select_one("a.ad-breadcrumbs__link[href*='grad-']")
    lokacija = loc_tag.get_text(strip=True) if loc_tag else ""

    price_tag = soup.select_one("div.cena p")
    cijena    = clean_price(price_tag.get_text()) if price_tag else ""

    return {
        "ime":        name,
        "oglas_link": url,
        "lokacija":   lokacija,
        "cijena":     cijena,
        "izvor":      "Oglasi.me",
        "_uid":       user_id,
    }


def run_oglasi() -> list[dict]:
    log.info("═" * 60)
    log.info("  OGLASI.ME  (od %s)", CUTOFF.strftime("%d.%m.%Y"))
    log.info("═" * 60)

    queue:      list[str]       = []
    old_in_row: int             = 0
    uid_count:  dict[str, int]  = defaultdict(int)

    for page in range(1, MAX_PAGES + 1):
        r = get(f"{OGLASI_BASE}/nekretnine/all/{page}")
        if r is None:
            break

        soup  = BeautifulSoup(r.text, "lxml")
        cards = _oglasi_cards(soup)
        if not cards:
            log.info("  str. %d: prazno, zaustavljam.", page)
            break

        page_new = 0
        for url, date_text, grad in cards:
            if FILTER_CITY and FILTER_CITY.lower() not in grad.lower():
                continue

            recent = is_recent(date_text)
            if recent is False:
                old_in_row += 1
            else:
                old_in_row = 0
                queue.append(url)
                page_new += 1

        log.info("  str. %d: %d novih/%s od %d kartica",
                 page, page_new, FILTER_CITY or "svi gradovi", len(cards))

        if old_in_row >= MAX_OLD_IN_ROW:
            log.info("  %d uzastopnih starih oglasa, završavam paginaciju.", old_in_row)
            break

        time.sleep(DELAY_LISTING)

    log.info("URLs za obraditi: %d", len(queue))

    raw: list[dict] = []

    for i, url in enumerate(queue, 1):
        log.info("  [%4d/%d] %s", i, len(queue), url)
        lead = _parse_oglasi_listing(url)
        if lead:
            uid = lead["_uid"] or lead["ime"]
            uid_count[uid] += 1
            raw.append(lead)
        time.sleep(DELAY_LISTING)

    for uid, cnt in list(uid_count.items()):
        if BROKER_MIN_LISTINGS - 1 <= cnt <= BROKER_MIN_LISTINGS + 1:
            if uid and re.match(r"^[A-Z]{2}\d+$", uid):
                uid_count[uid] = max(cnt, _oglasi_user_count(uid))
                time.sleep(DELAY_PROFILE)

    leads = []
    for lead in raw:
        uid   = lead.pop("_uid", None) or lead["ime"]
        count = uid_count[uid]
        if is_broker(lead["ime"], count):
            log.info("  [posrednik] %-28s (%d oglasa)", lead["ime"], count)
        else:
            leads.append(lead)

    log.info("Oglasi.me → %d vlasnika prošlo filter", len(leads))
    return leads

# ──────────────────────────────────────────────────────────────
# PATULJAK.ME
# ──────────────────────────────────────────────────────────────

PATULJAK_BASE = "https://www.patuljak.me"


def _patuljak_index_urls(page: int) -> list[str]:
    r = get(f"{PATULJAK_BASE}/c/nekretnine/namjena-sve/strana-{page}")
    if r is None:
        return []
    soup = BeautifulSoup(r.text, "lxml")
    return list({
        urljoin(PATULJAK_BASE, a["href"].split("?")[0])
        for a in soup.find_all("a", href=re.compile(r"^/oglas/"))
    })


def _patuljak_count_from_href(href: str) -> int:
    m = re.search(r"/profil/\d+/[^/]+/(\d+)/", href)
    return int(m.group(1)) if m else 1


def _parse_patuljak_listing(url: str) -> dict | None:
    r = get(url)
    if r is None:
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    page_text = soup.get_text(" ")

    dm = re.search(r"datum[:\s]+(\d{1,2}\.\d{1,2}\.\d{4})", page_text, re.I)
    if dm:
        listing_dt = parse_date(dm.group(1))
        if listing_dt is not None and listing_dt < CUTOFF:
            log.debug("  [star oglas] %s  datum: %s", url, dm.group(1))
            return "old"

    seller_div = soup.select_one("div.product_full__broj_tel")
    if not seller_div:
        return None

    name_tag = seller_div.select_one("h2[itemprop='name']")
    name     = name_tag.get_text(strip=True) if name_tag else "Nepoznat"

    profile_a     = seller_div.find("a", href=re.compile(r"/profil/"))
    listing_count = _patuljak_count_from_href(profile_a["href"]) if profile_a else 1

    lokacija = ""
    for li in soup.select("ul[itemprop='additionalProperty'] li"):
        spans = li.find_all("span")
        if len(spans) == 2 and spans[0].get_text(strip=True).lower() == "grad":
            lokacija = spans[1].get_text(strip=True)
            break

    if FILTER_CITY and FILTER_CITY.lower() not in lokacija.lower():
        log.debug("  [drugi grad] %s  →  %s", url, lokacija)
        return None

    price_tag = soup.select_one("div.product_full__cijena span[itemprop='price']")
    cijena    = clean_price(price_tag.get_text()) if price_tag else ""

    return {
        "ime":        name,
        "oglas_link": url,
        "lokacija":   lokacija,
        "cijena":     cijena,
        "izvor":      "Patuljak.me",
        "_count":     listing_count,
    }


def run_patuljak() -> list[dict]:
    log.info("═" * 60)
    log.info("  PATULJAK.ME  (od %s)", CUTOFF.strftime("%d.%m.%Y"))
    log.info("═" * 60)

    queue:      list[str] = []
    old_in_row: int       = 0

    for page in range(1, MAX_PAGES + 1):
        urls = _patuljak_index_urls(page)
        if not urls:
            log.info("  str. %d: prazno, zaustavljam.", page)
            break
        log.info("  str. %d: %d oglasa u listi", page, len(urls))
        queue.extend(urls)
        time.sleep(DELAY_LISTING)

    log.info("URLs za obraditi: %d", len(queue))

    leads = []
    for i, url in enumerate(queue, 1):
        log.info("  [%4d/%d] %s", i, len(queue), url)
        result = _parse_patuljak_listing(url)
        if result == "old":
            old_in_row += 1
        elif result is None:
            pass
        else:
            old_in_row = 0
            count = result.pop("_count", 1)
            if is_broker(result["ime"], count):
                log.info("  [posrednik] %-28s (%d oglasa)", result["ime"], count)
            else:
                leads.append(result)

        if old_in_row >= MAX_OLD_IN_ROW:
            log.info("  %d uzastopnih starih oglasa, završavam.", old_in_row)
            break

        time.sleep(DELAY_LISTING)

    log.info("Patuljak.me → %d vlasnika prošlo filter", len(leads))
    return leads

# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main() -> None:
    log.info("▶  Horizon Scraper  %s", datetime.now().strftime("%Y-%m-%d %H:%M"))

    all_leads = run_oglasi() + run_patuljak()

    # Deduplikacija po linku oglasa
    seen: set[str] = set()
    unique_leads: list[dict] = []
    for lead in all_leads:
        link = lead.get("oglas_link", "")
        if link not in seen:
            seen.add(link)
            unique_leads.append(lead)

    log.info("═" * 60)
    log.info("  Ukupno vlasnika za email: %d", len(unique_leads))
    log.info("═" * 60)

    send_email(unique_leads)

    log.info("■  Gotovo.")


if __name__ == "__main__":
    main()
