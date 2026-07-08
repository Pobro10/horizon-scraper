#!/usr/bin/env python3
"""
Horizon Scraper
Skenira oglase nekretnina sa Oglasi.me i Patuljak.me (zadnjih 48h),
filtrira posrednike i šalje email sa vlasnicima.

Instalacija : pip install requests beautifulsoup4 lxml
Pokretanje  : python horizon_scraper.py
"""

import re
import csv
import time
import json
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString

# ──────────────────────────────────────────────────────────────
# KONFIGURACIJA
# ──────────────────────────────────────────────────────────────

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "re_KKvP9n9w_4kuVE1Cjk2mvvEqn4sXfhcfm")
EMAIL_TO       = "officehorizon.nekretnine@gmail.com"
EMAIL_FROM     = os.environ.get("EMAIL_FROM", "onboarding@resend.dev")

# True samo za run u 10:30 UTC (GitHub Actions postavlja ovu varijablu)
SEND_EMAIL = os.environ.get("SEND_EMAIL", "true").lower() in ("1", "true", "yes")

# ── Broker blacklista (učitava se iz brokers.json) ─────────────
_BROKERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "brokers.json")
try:
    with open(_BROKERS_FILE, encoding="utf-8") as _f:
        _B = json.load(_f)
except FileNotFoundError:
    _B = {}

BROKER_KEYWORDS_STRONG = _B.get("keywords_strong", [
    "nekretnine", "real estate", "realty", "agencija", "agency",
    "property", "agent", "invest", "promet", "d.o.o", "montenegro",
    "proart", "prestige", "millennium", "bestate4me", "bestate",
    "globus", "dm nekretnine", "my place", "prizma s",
])
BROKER_KEYWORDS_WEAK   = _B.get("keywords_weak", ["did", "doo", "toka"])
BROKER_PHONES       = set(_B.get("phones", [
    "+38267580584", "+38267447444", "+38268150115", "+38267347963",
]))
BROKER_NAMES        = {n.lower() for n in _B.get("names", ["nemanja krstović"])}
BROKER_MIN_LISTINGS = _B.get("min_listings", 3)

# ── Opšte postavke ─────────────────────────────────────────────
FILTER_CITY      = "Podgorica"
MAX_PAGES        = 15
REALITICA_PAGES  = 2
DELAY_LISTING  = 1.5
DELAY_PROFILE  = 1.0
MAX_OLD_IN_ROW = 5
RETRY_COUNT    = 3    # HTTP retry pokušaji po zahtjevu
RETRY_BACKOFF  = 2    # eksponencijalni backoff (sek)

# Upozorenje ako udio neparsiranih oglasa pređe prag (ili queue prazan)
SELECTOR_FAIL_RATIO = 0.20

# ──────────────────────────────────────────────────────────────
# CUTOFF DATUM  (zadnjih 48 sati)
# ──────────────────────────────────────────────────────────────

CUTOFF = datetime.now() - timedelta(hours=48)

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
log.info("Cutoff datum : %s (zadnjih 48h)", CUTOFF.strftime("%d.%m.%Y %H:%M"))
log.info("SEND_EMAIL   : %s", SEND_EMAIL)

# ──────────────────────────────────────────────────────────────
# AUDIT LOG
# ──────────────────────────────────────────────────────────────

_AUDIT_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "audit_log.csv")
_AUDIT_COLS  = ["datum_pokretanja", "izvor", "ime", "link", "status", "razlog"]
_RUN_START   = datetime.now().strftime("%Y-%m-%d %H:%M")

def _audit_log(izvor: str, ime: str, link: str, status: str, razlog: str = "") -> None:
    write_header = not os.path.exists(_AUDIT_FILE)
    with open(_AUDIT_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(_AUDIT_COLS)
        w.writerow([_RUN_START, izvor, ime, link, status, razlog])

# ──────────────────────────────────────────────────────────────
# MEMORIJA POSLATIH OGLASA (sent.json)
# ──────────────────────────────────────────────────────────────

SENT_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sent.json")
SENT_MAX_DAYS = 4   # zapisi stariji od ovoga se brišu pri svakom upisu


def load_sent() -> dict[str, str]:
    """Čita {link: iso_timestamp poslatog mejla}; prazan dict ako fajl ne postoji ili je neispravan."""
    try:
        with open(SENT_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        log.warning("sent.json nije dict — krećem sa praznom memorijom.")
    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, OSError) as e:
        log.warning("sent.json neispravan (%s) — krećem sa praznom memorijom.", e)
    return {}


def save_sent(sent: dict[str, str]) -> None:
    tmp = SENT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(sent, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SENT_FILE)
    log.info("Memorija poslatih sačuvana u sent.json (%d linkova).", len(sent))


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
    (re.compile(r"prije\s+(\d+)\s+sek"),   lambda n: timedelta(seconds=n)),
    (re.compile(r"prije\s+(\d+)\s+min"),   lambda n: timedelta(minutes=n)),
    (re.compile(r"prije\s+(\d+)\s+h\b"),   lambda n: timedelta(hours=n)),
    (re.compile(r"prije\s+(\d+)\s+dan"),   lambda n: timedelta(days=n)),
    (re.compile(r"prije\s+(\d+)\s+sedmic"), lambda n: timedelta(weeks=n)),
    (re.compile(r"prije\s+(\d+)\s+mjes"),  lambda n: timedelta(days=n * 30)),
    (re.compile(r"prije\s+(\d+)\s+godin"), lambda n: timedelta(days=n * 365)),
]
_ABSOLUTE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{2,4})")


def parse_date(text: str) -> datetime | None:
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
    dt = parse_date(text)
    if dt is None:
        return None
    return dt >= CUTOFF

# ──────────────────────────────────────────────────────────────
# POMOĆNE FUNKCIJE
# ──────────────────────────────────────────────────────────────

def is_broker(name: str, listing_count: int, phone: str = "") -> tuple[str, str]:
    name_l = name.lower().strip()
    if name_l in BROKER_NAMES:
        return ("posrednik", "ime na listi")
    strong = next((kw for kw in BROKER_KEYWORDS_STRONG if kw in name_l), None)
    if strong:
        return ("posrednik", f"agencija: {strong}")
    if phone and phone.replace(" ", "") in {p.replace(" ", "") for p in BROKER_PHONES}:
        return ("posrednik", "telefon na listi")
    weak = next((kw for kw in BROKER_KEYWORDS_WEAK if kw in name_l), None)
    if weak:
        return ("provjeri", f"možda agencija: {weak}")
    if listing_count >= BROKER_MIN_LISTINGS:
        return ("provjeri", f"{listing_count} oglasa")
    return ("vlasnik", "")


def clean_price(raw: str) -> str:
    return re.sub(r"\s+", " ", raw.replace("\xa0", " ")).strip()


def get(url: str) -> requests.Response | None:
    """HTTP GET sa automatskim retry (eksponencijalni backoff)."""
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = http.get(url, timeout=15)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt < RETRY_COUNT:
                wait = RETRY_BACKOFF ** attempt
                log.warning("  GET pokušaj %d/%d nije uspio: %s → %s (retry za %ds)",
                             attempt, RETRY_COUNT, url, e, wait)
                time.sleep(wait)
            else:
                log.warning("  GET nije uspio (%d/%d): %s → %s",
                             attempt, RETRY_COUNT, url, e)
                return None

# ──────────────────────────────────────────────────────────────
# SLANJE EMAILA (Resend API)
# ──────────────────────────────────────────────────────────────

def _resend_post(subject: str, html: str, text: str) -> bool:
    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from":    EMAIL_FROM,
            "to":      [EMAIL_TO],
            "subject": subject,
            "html":    html,
            "text":    text,
        },
        timeout=30,
    )
    return resp.ok


def send_email(leads: list[dict], review_leads: list[dict] = []) -> bool:
    """Vraća True samo ako je email stvarno poslan (Resend potvrdio uspjeh)."""
    if not SEND_EMAIL:
        log.info("SEND_EMAIL=false — email se preskače (artifact sačuvan).")
        return False

    datum = datetime.now().strftime("%d.%m.%Y")

    if not leads and not review_leads:
        log.info("Nema novih vlasnika — email se ne šalje.")
        return False

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

    review_block = ""
    if review_leads:
        review_rows = "\n".join(
            f"""        <tr>
          <td style="padding:8px;border:1px solid #ddd">{lead['ime']}</td>
          <td style="padding:8px;border:1px solid #ddd">{lead['cijena']}</td>
          <td style="padding:8px;border:1px solid #ddd">{lead['lokacija']}</td>
          <td style="padding:8px;border:1px solid #ddd">
            <a href="{lead['oglas_link']}">{lead['oglas_link']}</a>
          </td>
          <td style="padding:8px;border:1px solid #ddd">{lead.get('_razlog','')}</td>
        </tr>"""
            for lead in review_leads
        )
        review_block = f"""
<h3 style="color:#b8860b;margin-top:32px">Za ručnu provjeru ({len(review_leads)})</h3>
<table style="border-collapse:collapse;width:100%;font-size:14px">
  <thead>
    <tr style="background:#fff3cd">
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Ime</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Cijena</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Lokacija</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Link oglasa</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Razlog</th>
    </tr>
  </thead>
  <tbody>
{review_rows}
  </tbody>
</table>"""

    html = f"""<!DOCTYPE html>
<html lang="bs"><body style="font-family:Arial,sans-serif;color:#333">
<h2 style="color:#1a1a1a">Horizon Scraper — {datum}</h2>
<p>Pronađeno <strong>{len(leads)}</strong> vlasnik(a) u zadnjih 48 sati (od {CUTOFF.strftime('%d.%m.%Y %H:%M')}):</p>
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
{review_block}
<p style="color:#888;font-size:12px;margin-top:24px">
  Izvor: Oglasi.me + Patuljak.me &nbsp;|&nbsp; Horizon Scraper
</p>
</body></html>"""

    text_lines = [f"Horizon Scraper — {datum}",
                  f"Vlasnici ({len(leads)}):", ""]
    for lead in leads:
        text_lines += [
            f"Ime:      {lead['ime']}",
            f"Cijena:   {lead['cijena']}",
            f"Lokacija: {lead['lokacija']}",
            f"Oglas:    {lead['oglas_link']}",
            "",
        ]
    if review_leads:
        text_lines += [f"--- Za ručnu provjeru ({len(review_leads)}) ---", ""]
        for lead in review_leads:
            text_lines += [
                f"Ime:      {lead['ime']}",
                f"Cijena:   {lead['cijena']}",
                f"Lokacija: {lead['lokacija']}",
                f"Oglas:    {lead['oglas_link']}",
                f"Razlog:   {lead.get('_razlog','')}",
                "",
            ]

    subject = f"Horizon Scraper — {len(leads)} vlasnik(a)"
    if review_leads:
        subject += f" + {len(review_leads)} provjera"
    subject += f" — {datum}"

    ok = _resend_post(subject=subject, html=html, text="\n".join(text_lines))
    if ok:
        log.info("✓  Email poslan na %s  (%d vlasnika, %d provjera).",
                 EMAIL_TO, len(leads), len(review_leads))
    else:
        log.error("✗  Greška pri slanju emaila.")
    return ok


def send_warning_email(sajt: str, fail_count: int, total: int) -> None:
    datum = datetime.now().strftime("%d.%m.%Y %H:%M")
    subject = f"⚠️ Horizon Scraper — selektori pokvareni na {sajt}"
    body = (
        f"<h3>Upozorenje — {sajt}</h3>"
        f"<p>Datum: {datum}</p>"
        f"<p>{fail_count} od {total} oglasa nije moglo biti parsirano jer "
        f"selektori više ne pronalaze elemente na stranici.</p>"
        f"<p>HTML struktura sajta je vjerovatno promijenjena. "
        f"Molimo provjerite skriptu.</p>"
    )
    text = (
        f"UPOZORENJE — {sajt}\n"
        f"Datum: {datum}\n"
        f"{fail_count}/{total} oglasa nije parsirano — selektori su pokvareni.\n"
        f"Provjerite HTML strukturu sajta i ažurirajte skriptu."
    )
    ok = _resend_post(subject=subject, html=body, text=text)
    if ok:
        log.warning("⚠  Upozorenje email poslan: %s (%d/%d selector grešaka).",
                    sajt, fail_count, total)
    else:
        log.error("✗  Greška pri slanju upozorenja za %s.", sajt)


def save_leads_json(leads: list[dict]) -> None:
    path = "leads_today.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(leads, f, ensure_ascii=False, indent=2)
    log.info("Leads sačuvani u %s (%d stavki).", path, len(leads))

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


def _parse_oglasi_listing(url: str) -> dict | str | None:
    """
    Vraća:
      dict            — uspješno parsirani oglas
      "old"           — oglas je stariji od cutoff datuma
      None            — HTTP greška (stranica nedostupna)
      "selector_fail" — selektori više ne rade (HTML se promijenio)
    """
    r = get(url)
    if r is None:
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    page_text = soup.get_text(" ")

    # Oglasi.me daje samo datum bez sata — poredimo po datumu, bez vremenske komponente
    date_m = _ABSOLUTE.search(page_text)
    if date_m:
        listing_dt = parse_date(date_m.group(0))
        if listing_dt is not None and listing_dt.date() < CUTOFF.date():
            log.debug("  [star oglas] %s  datum: %s", url, date_m.group(0))
            return "old"

    # ── Ime oglašivača — probaj više selektora ──────────────────
    name_tag = (
        soup.select_one("p.sidebar-user-status-name") or
        soup.select_one("div.korisnik span") or
        soup.select_one("div.korisnik")
    )
    if not name_tag:
        log.debug("  [selector_fail/oglasi] %s", url)
        return "selector_fail"

    raw_name = name_tag.get_text(" ", strip=True)
    name     = re.sub(r"\s*\(.*?\)", "", raw_name).strip()
    uid_m    = re.search(r"\((\w+)\)", raw_name)
    user_id  = uid_m.group(1) if uid_m else None

    # ── Lokacija ────────────────────────────────────────────────
    loc_tag  = (
        soup.select_one("a.ad-breadcrumbs__link[href*='grad-']") or
        soup.select_one("a[href*='/grad-']")
    )
    lokacija = loc_tag.get_text(strip=True) if loc_tag else ""

    # ── Cijena ──────────────────────────────────────────────────
    price_tag = soup.select_one("div.cena p") or soup.select_one("div.cena")
    cijena    = clean_price(price_tag.get_text()) if price_tag else ""

    return {
        "ime":        name,
        "oglas_link": url,
        "lokacija":   lokacija,
        "cijena":     cijena,
        "izvor":      "Oglasi.me",
        "_uid":       user_id,
    }


def run_oglasi() -> tuple[list[dict], list[dict]]:
    log.info("═" * 60)
    log.info("  OGLASI.ME  (od %s)", CUTOFF.strftime("%d.%m.%Y %H:%M"))
    log.info("═" * 60)

    queue:      list[str]      = []
    old_in_row: int            = 0
    uid_count:  dict[str, int] = defaultdict(int)

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

    raw:            list[dict] = []
    selector_fails: int        = 0

    for i, url in enumerate(queue, 1):
        log.info("  [%4d/%d] %s", i, len(queue), url)
        result = _parse_oglasi_listing(url)
        if result == "selector_fail":
            selector_fails += 1
            _audit_log("Oglasi.me", "", url, "neparsiran", "selector_fail")
        elif result == "old":
            _audit_log("Oglasi.me", "", url, "star", "van cutoffa")
        elif result is None:
            _audit_log("Oglasi.me", "", url, "skip", "HTTP greška")
        else:
            uid = result["_uid"] or result["ime"]
            uid_count[uid] += 1
            raw.append(result)
        time.sleep(DELAY_LISTING)

    if len(queue) == 0:
        log.warning("⚠  Oglasi.me: 0 oglasa pronađeno — stranica vjerovatno promijenjena!")
        send_warning_email("Oglasi.me", 0, 0)
    elif selector_fails / len(queue) > SELECTOR_FAIL_RATIO:
        log.warning("⚠  Oglasi.me: %d/%d oglasa nije moglo biti parsirano — selektori pokvareni!",
                    selector_fails, len(queue))
        send_warning_email("Oglasi.me", selector_fails, len(queue))
    elif selector_fails > 0:
        log.info("ℹ  Oglasi.me: %d/%d oglasa nije parsirano (ispod praga, bez mejla).",
                 selector_fails, len(queue))

    for uid, cnt in list(uid_count.items()):
        if BROKER_MIN_LISTINGS - 1 <= cnt <= BROKER_MIN_LISTINGS + 1:
            if uid and re.match(r"^[A-Z]{2}\d+$", uid):
                uid_count[uid] = max(cnt, _oglasi_user_count(uid))
                time.sleep(DELAY_PROFILE)

    leads:        list[dict] = []
    review_leads: list[dict] = []
    for lead in raw:
        uid   = lead.pop("_uid", None) or lead["ime"]
        count = uid_count[uid]
        status, razlog = is_broker(lead["ime"], count)
        if status == "posrednik":
            log.info("  [posrednik] %-28s (%s)", lead["ime"], razlog)
            _audit_log("Oglasi.me", lead["ime"], lead["oglas_link"], "posrednik", razlog)
        elif status == "provjeri":
            lead["_razlog"] = razlog
            review_leads.append(lead)
            log.info("  [provjeri]  %-28s (%s)", lead["ime"], razlog)
            _audit_log("Oglasi.me", lead["ime"], lead["oglas_link"], "provjeri", razlog)
        else:
            leads.append(lead)
            _audit_log("Oglasi.me", lead["ime"], lead["oglas_link"], "poslat", "")

    log.info("Oglasi.me → %d vlasnika, %d za provjeru", len(leads), len(review_leads))
    return leads, review_leads

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


def _parse_patuljak_listing(url: str) -> dict | str | None:
    """
    Vraća:
      dict            — uspješno parsirani oglas
      "old"           — oglas je stariji od CUTOFF
      None            — oglas iz drugog grada ili HTTP greška
      "selector_fail" — selektori više ne rade (HTML se promijenio)
    """
    r = get(url)
    if r is None:
        return None

    soup      = BeautifulSoup(r.text, "lxml")
    page_text = soup.get_text(" ")

    # ── Datum + sat (npr. "datum: 05.05.2026 20:31") ────────────
    dm = re.search(
        r"datum[:\s]+(\d{1,2}\.\d{1,2}\.\d{4})(?:\s+(\d{1,2}:\d{2}))?",
        page_text, re.I,
    )
    if dm:
        date_str = dm.group(1)
        time_str = dm.group(2)
        if time_str:
            try:
                listing_dt = datetime.strptime(f"{date_str} {time_str}", "%d.%m.%Y %H:%M")
            except ValueError:
                listing_dt = parse_date(date_str)
        else:
            listing_dt = parse_date(date_str)
        if listing_dt is not None and listing_dt < CUTOFF:
            log.debug("  [star oglas] %s  datum: %s", url, date_str)
            return "old"

    # ── Seller div — probaj više selektora ─────────────────────
    seller_div = (
        soup.select_one("div.product_full--opis---seller") or
        soup.select_one("div.product_full__broj_tel")
    )
    if not seller_div:
        log.debug("  [selector_fail/patuljak] %s", url)
        return "selector_fail"

    # ── Ime oglašivača ──────────────────────────────────────────
    name_tag = (
        seller_div.select_one("h6 a") or
        seller_div.select_one("h2[itemprop='name']")
    )
    name = name_tag.get_text(strip=True) if name_tag else "Nepoznat"

    profile_a     = seller_div.find("a", href=re.compile(r"/profil/"))
    listing_count = _patuljak_count_from_href(profile_a["href"]) if profile_a else 1

    # ── Lokacija ────────────────────────────────────────────────
    lokacija = ""
    for li in soup.select("ul.product_full__info li, ul[itemprop='additionalProperty'] li"):
        spans = li.find_all("span")
        if len(spans) == 2 and spans[0].get_text(strip=True).lower() == "grad":
            lokacija = spans[1].get_text(strip=True)
            break

    if FILTER_CITY and FILTER_CITY.lower() not in lokacija.lower():
        log.debug("  [drugi grad] %s  →  %s", url, lokacija)
        return None

    # ── Cijena ──────────────────────────────────────────────────
    price_tag = soup.select_one("div.product_full__cijena")
    if price_tag:
        raw_price = re.sub(r"(?i)cijena\s*:\s*", "", price_tag.get_text(strip=True))
        cijena    = clean_price(raw_price)
    else:
        cijena = ""

    return {
        "ime":        name,
        "oglas_link": url,
        "lokacija":   lokacija,
        "cijena":     cijena,
        "izvor":      "Patuljak.me",
        "_count":     listing_count,
    }


def run_patuljak() -> tuple[list[dict], list[dict]]:
    log.info("═" * 60)
    log.info("  PATULJAK.ME  (od %s)", CUTOFF.strftime("%d.%m.%Y %H:%M"))
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

    leads:          list[dict] = []
    review_leads:   list[dict] = []
    selector_fails: int        = 0

    for i, url in enumerate(queue, 1):
        log.info("  [%4d/%d] %s", i, len(queue), url)
        result = _parse_patuljak_listing(url)

        if result == "old":
            old_in_row += 1
            _audit_log("Patuljak.me", "", url, "star", "van cutoffa")
        elif result == "selector_fail":
            selector_fails += 1
            _audit_log("Patuljak.me", "", url, "neparsiran", "selector_fail")
        elif result is None:
            _audit_log("Patuljak.me", "", url, "skip", "HTTP greška ili drugi grad")
        else:
            old_in_row = 0
            count = result.pop("_count", 1)
            status, razlog = is_broker(result["ime"], count)
            if status == "posrednik":
                log.info("  [posrednik] %-28s (%s)", result["ime"], razlog)
                _audit_log("Patuljak.me", result["ime"], result["oglas_link"], "posrednik", razlog)
            elif status == "provjeri":
                result["_razlog"] = razlog
                review_leads.append(result)
                log.info("  [provjeri]  %-28s (%s)", result["ime"], razlog)
                _audit_log("Patuljak.me", result["ime"], result["oglas_link"], "provjeri", razlog)
            else:
                leads.append(result)
                _audit_log("Patuljak.me", result["ime"], result["oglas_link"], "poslat", "")

        if old_in_row >= MAX_OLD_IN_ROW:
            log.info("  %d uzastopnih starih oglasa, završavam.", old_in_row)
            break

        time.sleep(DELAY_LISTING)

    if len(queue) == 0:
        log.warning("⚠  Patuljak.me: 0 oglasa pronađeno — stranica vjerovatno promijenjena!")
        send_warning_email("Patuljak.me", 0, 0)
    elif selector_fails / len(queue) > SELECTOR_FAIL_RATIO:
        log.warning("⚠  Patuljak.me: %d/%d oglasa nije moglo biti parsirano — selektori pokvareni!",
                    selector_fails, len(queue))
        send_warning_email("Patuljak.me", selector_fails, len(queue))
    elif selector_fails > 0:
        log.info("ℹ  Patuljak.me: %d/%d oglasa nije parsirano (ispod praga, bez mejla).",
                 selector_fails, len(queue))

    log.info("Patuljak.me → %d vlasnika, %d za provjeru", len(leads), len(review_leads))
    return leads, review_leads

# ──────────────────────────────────────────────────────────────
# REALITICA.COM
# ──────────────────────────────────────────────────────────────

REALITICA_BASE = "https://www.realitica.com"


def _realitica_parse_card(thumb) -> dict | None:
    info_div = thumb.find_next_sibling("div")
    if not info_div:
        return None

    a = thumb.find("a", href=True)
    if not a:
        return None
    link = a["href"]
    if not link.startswith("http"):
        link = REALITICA_BASE + link

    name = ""
    for child in info_div.children:
        if isinstance(child, NavigableString) and child.strip():
            name = child.strip()
            break

    cijena = ""
    for strong in info_div.find_all("strong"):
        if "€" in strong.text:
            cijena = strong.text.strip()
            break

    lines, current = [], []
    for child in info_div.children:
        if getattr(child, "name", None) == "br":
            lines.append("".join(current).strip())
            current = []
        elif hasattr(child, "get_text"):
            current.append(child.get_text(strip=True))
        else:
            current.append(str(child).strip())
    lines.append("".join(current).strip())
    lines = [l for l in lines if l]
    lokacija = next((l for l in lines if "Podgorica" in l or "Crna Gora" in l), "")

    if not name:
        return None
    return {"ime": name, "oglas_link": link, "lokacija": lokacija,
            "cijena": cijena, "izvor": "Realitica.com"}


def run_realitica() -> tuple[list[dict], list[dict]]:
    log.info("═" * 60)
    log.info("  REALITICA.COM")
    log.info("═" * 60)

    leads:        list[dict]     = []
    review_leads: list[dict]     = []
    raw:          list[dict]     = []
    name_count:   dict[str, int] = {}

    for for_param, label in [("Prodaja", "prodaja"), ("DuziNajam", "najam")]:
        try:
            for page in range(1, REALITICA_PAGES + 1):
                if page == 1:
                    url = (
                        f"{REALITICA_BASE}/index.php?for={for_param}"
                        f"&lng=hr&opa=Podgorica&qob=p-new&since-day=p-7day"
                    )
                else:
                    url = (
                        f"{REALITICA_BASE}/?cur_page={page - 1}&for={for_param}"
                        f"&lng=hr&opa=Podgorica&qob=p-new&since-day=p-7day"
                    )
                r = get(url)
                if r is None:
                    log.warning("  Realitica [%s] str. %d: HTTP greška, preskačem.",
                                label, page)
                    continue

                soup   = BeautifulSoup(r.text, "lxml")
                thumbs = soup.select("div.thumb_div")
                log.info("  [%s] str. %d: %d kartica", label, page, len(thumbs))

                for thumb in thumbs:
                    card = _realitica_parse_card(thumb)
                    if card:
                        card["izvor"] = f"Realitica.com ({label})"
                        raw.append(card)
                        name_count[card["ime"]] = name_count.get(card["ime"], 0) + 1

                time.sleep(DELAY_LISTING)

        except Exception as e:
            log.error("Realitica.com [%s] greška — preskačem: %s", label, e)

    for lead in raw:
        log.debug("  [raw] %-30s | %s | %s",
                  lead["ime"], lead["cijena"], lead["lokacija"])

    for lead in raw:
        count = name_count[lead["ime"]]
        status, razlog = is_broker(lead["ime"], count)
        if status == "posrednik":
            log.info("  [posrednik] %-28s (%s)", lead["ime"], razlog)
            _audit_log(lead["izvor"], lead["ime"], lead["oglas_link"], "posrednik", razlog)
        elif status == "provjeri":
            lead["_razlog"] = razlog
            review_leads.append(lead)
            log.info("  [provjeri]  %-28s (%s)", lead["ime"], razlog)
            _audit_log(lead["izvor"], lead["ime"], lead["oglas_link"], "provjeri", razlog)
        else:
            leads.append(lead)
            log.info("  [vlasnik]   %-28s (%s)", lead["ime"], lead["izvor"])
            _audit_log(lead["izvor"], lead["ime"], lead["oglas_link"], "poslat", "")

    log.info("Realitica.com → %d vlasnika, %d za provjeru", len(leads), len(review_leads))
    return leads, review_leads


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main() -> None:
    log.info("▶  Horizon Scraper  %s", datetime.now().strftime("%Y-%m-%d %H:%M"))

    leads_o, review_o = run_oglasi()
    leads_p, review_p = run_patuljak()
    leads_r, review_r = run_realitica()
    all_leads    = leads_o + leads_p + leads_r
    all_review   = review_o + review_p + review_r

    # Deduplikacija po linku oglasa
    seen:         set[str]   = set()
    unique_leads: list[dict] = []
    for lead in all_leads:
        link = lead.get("oglas_link", "")
        if link not in seen:
            seen.add(link)
            unique_leads.append(lead)

    seen_r:        set[str]   = set()
    unique_review: list[dict] = []
    for lead in all_review:
        link = lead.get("oglas_link", "")
        if link not in seen_r:
            seen_r.add(link)
            unique_review.append(lead)

    dupes = len(all_leads) - len(unique_leads)
    if dupes:
        log.info("Deduplikacija: uklonjeno %d duplikata.", dupes)

    # ── Memorija poslatih: izbaci oglase koji su već bili u mejlu ──
    sent    = load_sent()
    pre_len = len(unique_leads) + len(unique_review)

    def _u_memoriji(lead: dict) -> bool:
        ts = sent.get(lead["oglas_link"])
        if ts is None:
            return False
        _audit_log(lead["izvor"], lead["ime"], lead["oglas_link"],
                   "preskocen_memorija", f"u sent.json od {ts[:16].replace('T', ' ')}")
        return True

    unique_leads  = [l for l in unique_leads  if not _u_memoriji(l)]
    unique_review = [l for l in unique_review if not _u_memoriji(l)]
    skipped = pre_len - len(unique_leads) - len(unique_review)
    if skipped:
        log.info("Memorija: preskočeno %d već poslatih oglasa.", skipped)

    log.info("═" * 60)
    log.info("  Ukupno vlasnika: %d  |  Za provjeru: %d", len(unique_leads), len(unique_review))
    log.info("═" * 60)

    save_leads_json(unique_leads)
    email_sent = send_email(unique_leads, unique_review)

    # Memorija se upisuje SAMO nakon uspješno poslatog mejla — i samo
    # ono što je stvarno bilo u mejlu. Pad slanja → memorija netaknuta.
    if email_sent:
        now_iso = datetime.now().isoformat()
        for lead in unique_leads + unique_review:
            sent[lead["oglas_link"]] = now_iso

        granica = datetime.now() - timedelta(days=SENT_MAX_DAYS)

        def _fresh(ts: str) -> bool:
            try:
                return datetime.fromisoformat(ts) >= granica
            except ValueError:
                return False

        sent = {k: v for k, v in sent.items() if _fresh(v)}
        save_sent(sent)

    log.info("■  Gotovo.")


if __name__ == "__main__":
    main()
