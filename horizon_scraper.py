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
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from collections import defaultdict
from html import escape
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString

# ──────────────────────────────────────────────────────────────
# KONFIGURACIJA
# ──────────────────────────────────────────────────────────────

# Ključ dolazi ISKLJUČIVO iz env varijable (GitHub Secrets) - nikad u kodu.
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
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
BROKER_NAMES        = {n.lower() for n in _B.get("names", ["nemanja krstović"])}
BROKER_MIN_LISTINGS = _B.get("min_listings", 3)
PHONE_MIN_LISTINGS  = _B.get("phone_min_listings", 3)   # broj na 3+ oglasa = sumnjiv


def normalize_phone(raw: str) -> str | None:
    """Svodi crnogorski broj na oblik 3826XXXXXXX (samo cifre).
    Prihvata +382..., 382..., 06x..., 6x...; ostalo odbacuje."""
    digits = re.sub(r"\D", "", raw or "")
    if digits.startswith("382"):
        rest = digits[3:]
    elif digits.startswith("0"):
        rest = digits[1:]
    else:
        rest = digits
    # crnogorski mobilni/fiksni: 6-9 na početku, ukupno 8-9 cifara iza 382
    if not (7 <= len(rest) <= 9 and rest[:1] in "23456789"):
        return None
    return "382" + rest


def format_phone(p: str) -> str:
    """3826XXXXXXX -> +382 6X XXX XXX (za mejl)."""
    if p.startswith("382"):
        rest = p[3:]
        return f"+382 {rest[:2]} {rest[2:5]} {rest[5:]}".strip()
    return p


BROKER_PHONES = {np for p in _B.get("phones", []) if (np := normalize_phone(p))}

# Regex za brojeve u slobodnom tekstu opisa (067 123 456, +382 67 123-456...)
_PHONE_IN_TEXT = re.compile(r"(?:\+?\s*382|0)\s*6[0-9](?:[\s./-]?\d){5,7}")

# ── Opšte postavke ─────────────────────────────────────────────
FILTER_CITY      = "Podgorica"
MAX_PAGES        = 15
REALITICA_PAGES  = 2
DELAY_LISTING  = 1.0
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

_AUDIT_LOCK = threading.Lock()


def _audit_log(izvor: str, ime: str, link: str, status: str, razlog: str = "") -> None:
    with _AUDIT_LOCK:
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
SENT_MAX_DAYS = 8   # mora biti > 7 (Realitica koristi since-day=p-7day), inače se stari oglasi ponovo šalju


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

# ──────────────────────────────────────────────────────────────
# KEŠ OBRAĐENIH OGLASA (seen.json)
# Svaki obrađeni URL se pamti sa statusom, pa se detalj stranica skida
# samo JEDNOM umjesto u sva 4 dnevna runa. Vlasnici i "provjeri" čuvaju
# i lead podatke, da bi ušli u mejl i iz keša (mejl šalje samo 1 run).
# ──────────────────────────────────────────────────────────────

SEEN_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seen.json")
SEEN_MAX_DAYS = 8   # isto kao SENT_MAX_DAYS: > 7 zbog Realitice

_SEEN_LOCK = threading.Lock()
SEEN: dict[str, dict] = {}


def load_seen() -> dict[str, dict]:
    try:
        with open(SEEN_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        log.warning("seen.json nije dict — krećem sa praznim kešom.")
    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, OSError) as e:
        log.warning("seen.json neispravan (%s) — krećem sa praznim kešom.", e)
    return {}


def _save_seen_locked() -> int:
    """Snima keš na disk. Poziva se ISKLJUČIVO sa već držanim _SEEN_LOCK."""
    granica = datetime.now() - timedelta(days=SEEN_MAX_DAYS)

    def _fresh(entry: dict) -> bool:
        try:
            return datetime.fromisoformat(entry.get("ts", "")) >= granica
        except ValueError:
            return False

    pruned = {k: v for k, v in SEEN.items() if _fresh(v)}
    tmp = SEEN_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(pruned, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SEEN_FILE)
    return len(pruned)


def save_seen() -> None:
    with _SEEN_LOCK:
        n = _save_seen_locked()
    log.info("Keš obrađenih sačuvan u seen.json (%d zapisa).", n)


def seen_get(url: str) -> dict | None:
    with _SEEN_LOCK:
        entry = SEEN.get(url)
    # lead keširan starijom verzijom koda (bez polja "telefoni") se tretira
    # kao nekeširan, da jednom bude skinut ponovo sa telefonom i opisom
    if entry and entry.get("lead") is not None and "telefoni" not in entry["lead"]:
        return None
    return entry


# ──────────────────────────────────────────────────────────────
# REGISTAR TELEFONA (phones.json)
# Broj viđen na PHONE_MIN_LISTINGS+ različitih oglasa (kroz sajtove i
# vrijeme) = vrlo vjerovatno posrednik. Lista se sama gradi svakim runom.
# ──────────────────────────────────────────────────────────────

PHONES_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "phones.json")
PHONES_MAX_DAYS = 30

_PHONES_LOCK = threading.Lock()
PHONES: dict[str, dict] = {}   # {broj: {"urls": {url: iso_ts}, "imena": [..]}}


def load_phones() -> dict[str, dict]:
    try:
        with open(PHONES_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
    except FileNotFoundError:
        pass
    except (json.JSONDecodeError, OSError) as e:
        log.warning("phones.json neispravan (%s) — krećem sa praznim registrom.", e)
    return {}


def save_phones() -> None:
    granica = (datetime.now() - timedelta(days=PHONES_MAX_DAYS)).isoformat()
    with _PHONES_LOCK:
        for entry in PHONES.values():
            entry["urls"] = {u: ts for u, ts in entry["urls"].items() if ts >= granica}
        pruned = {p: e for p, e in PHONES.items() if e["urls"]}
        tmp = PHONES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(pruned, f, ensure_ascii=False, indent=2)
        os.replace(tmp, PHONES_FILE)
    log.info("Registar telefona sačuvan u phones.json (%d brojeva).", len(pruned))


def phones_record(url: str, ime: str, phones: list[str]) -> None:
    with _PHONES_LOCK:
        for p in phones:
            e = PHONES.setdefault(p, {"urls": {}, "imena": []})
            e["urls"][url] = datetime.now().isoformat()
            if ime and ime not in e["imena"]:
                e["imena"] = (e["imena"] + [ime])[-5:]


def phones_count(phone: str) -> int:
    with _PHONES_LOCK:
        e = PHONES.get(phone)
        return len(e["urls"]) if e else 0


_seen_dirty = 0


def seen_record(url: str, status: str, lead: dict | None = None) -> None:
    """Upis u keš; na svakih 50 novih zapisa snima na disk, da prekinut
    run (timeout, pad runnera) ne izgubi cio napredak."""
    global _seen_dirty
    with _SEEN_LOCK:
        SEEN[url] = {"ts": datetime.now().isoformat(), "status": status, "lead": lead}
        _seen_dirty += 1
        if _seen_dirty >= 50:
            _seen_dirty = 0
            _save_seen_locked()


def seen_set_ai(url: str, ai: dict) -> None:
    """Dopiše AI presudu u keširani lead, da se isti oglas ne klasifikuje
    (i ne plaća) dvaput."""
    with _SEEN_LOCK:
        e = SEEN.get(url)
        if e and e.get("lead"):
            e["lead"]["_ai"] = ai


# requests.Session nije garantovano thread-safe, pa svaki thread (sajt)
# dobija svoju sesiju preko threading.local().
_TLS = threading.local()


def _session() -> requests.Session:
    s = getattr(_TLS, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "bs,hr,sr;q=0.9,en;q=0.8",
        })
        _TLS.session = s
    return s

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

def is_broker(name: str, listing_count: int, phones: list[str] | None = None) -> tuple[str, str]:
    name_l = name.lower().strip()
    phones = phones or []
    if name_l in BROKER_NAMES:
        return ("posrednik", "ime na listi")
    strong = next((kw for kw in BROKER_KEYWORDS_STRONG if kw in name_l), None)
    if strong:
        return ("posrednik", f"agencija: {strong}")
    hit = next((p for p in phones if p in BROKER_PHONES), None)
    if hit:
        return ("posrednik", f"telefon na listi ({format_phone(hit)})")
    # broj viđen na više oglasa kroz sajtove/vrijeme -> sumnjiv, ali ne
    # automatski posrednik (vlasnik legalno moze imati 2 oglasa)
    for p in phones:
        n = phones_count(p)
        if n >= PHONE_MIN_LISTINGS:
            return ("provjeri", f"telefon na {n} oglasa ({format_phone(p)})")
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
            r = _session().get(url, timeout=15)
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
# AI ČITANJE OPISA
# Čita opis oglasa kao čovjek i presuđuje vlasnik/agencija/nesigurno.
# Dva provajdera: ANTHROPIC_API_KEY (Claude Haiku, plaćeni) ima prednost;
# inače GEMINI_API_KEY (Google, besplatni nivo); bez ijednog se preskače.
# ──────────────────────────────────────────────────────────────

AI_MODEL     = "claude-haiku-4-5"
GEMINI_MODEL = "gemini-2.5-flash"
GEMINI_DELAY = 6.5   # besplatni nivo: ~10 zahtjeva u minuti

AI_SYSTEM = (
    "Pomažeš agenciji za nekretnine u Podgorici da razdvoji oglase privatnih "
    "vlasnika od oglasa agencija i posrednika. Na osnovu imena oglašivača i "
    "teksta oglasa procijeni ko oglašava.\n"
    "Znakovi agencije: šifra ili ID oglasa, 'u ponudi' / 'u našoj ponudi', "
    "pominjanje provizije, profesionalno formatiran tekst sa mnogo stavki, "
    "pominjanje više različitih nekretnina, poziv na kancelariju ili sajt, "
    "naziv firme umjesto ličnog imena.\n"
    "Znakovi vlasnika: 'bez posrednika', 'agencije isključene', 'prodajem "
    "svoj stan', lični ton, običan neformalan tekst.\n"
    "Ako nema dovoljno signala, presuda je 'nesigurno'. "
    "Razlog napiši u JEDNOJ kratkoj rečenici na crnogorskom."
)

_AI_SCHEMA = {
    "type": "object",
    "properties": {
        "presuda": {"type": "string", "enum": ["vlasnik", "agencija", "nesigurno"]},
        "razlog":  {"type": "string"},
    },
    "required": ["presuda", "razlog"],
    "additionalProperties": False,
}


def _ai_prompt(lead: dict) -> str:
    return f"Ime oglašivača: {lead['ime']}\nOpis oglasa:\n{lead['_opis']}"


def _ai_anthropic(client, lead: dict) -> dict:
    resp = client.messages.create(
        model=AI_MODEL,
        max_tokens=200,
        system=AI_SYSTEM,
        messages=[{"role": "user", "content": _ai_prompt(lead)}],
        output_config={"format": {"type": "json_schema", "schema": _AI_SCHEMA}},
    )
    text = next(b.text for b in resp.content if b.type == "text")
    return json.loads(text)


def _ai_gemini(api_key: str, lead: dict) -> dict:
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    body = {
        "systemInstruction": {"parts": [{"text": AI_SYSTEM}]},
        "contents": [{"parts": [{"text": _ai_prompt(lead)}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {
                    "presuda": {"type": "STRING",
                                "enum": ["vlasnik", "agencija", "nesigurno"]},
                    "razlog":  {"type": "STRING"},
                },
                "required": ["presuda", "razlog"],
            },
            # gasi "razmišljanje" (inače pojede maxOutputTokens pa tekst bude prazan)
            "thinkingConfig": {"thinkingBudget": 0},
            "maxOutputTokens": 300,
        },
    }
    # ključ ide u header, NIKAD u URL (URL završava u logovima)
    headers = {"x-goog-api-key": api_key}
    r = requests.post(url, headers=headers, json=body, timeout=60)
    if r.status_code == 429:
        time.sleep(30)
        r = requests.post(url, headers=headers, json=body, timeout=60)
    r.raise_for_status()
    text = r.json()["candidates"][0]["content"]["parts"][0]["text"]
    return json.loads(text)


def ai_classify(leads: list[dict]) -> None:
    """Dopiše lead["_ai"] leadovima koji imaju opis, a još nemaju presudu.
    Svaka greška se samo loguje — mejl ide i bez AI kolone."""
    kandidati = [l for l in leads if l.get("_opis") and not l.get("_ai")]
    if not kandidati:
        return

    anth_key = os.environ.get("ANTHROPIC_API_KEY", "")
    gem_key  = os.environ.get("GEMINI_API_KEY", "")

    client = None
    if anth_key:
        try:
            import anthropic
            client = anthropic.Anthropic()
            provider = "Claude Haiku"
        except ImportError:
            log.warning("Paket 'anthropic' nije instaliran — probam Gemini.")
            anth_key = ""
    if not anth_key:
        if gem_key:
            provider = "Gemini (besplatni)"
        else:
            log.info("AI čitač preskočen: ni ANTHROPIC_API_KEY ni GEMINI_API_KEY nisu postavljeni.")
            return

    log.info("AI čitač (%s): %d oglasa za klasifikaciju.", provider, len(kandidati))
    uspjelo = 0
    for i, lead in enumerate(kandidati):
        try:
            if anth_key:
                lead["_ai"] = _ai_anthropic(client, lead)
            else:
                if i:
                    time.sleep(GEMINI_DELAY)   # poštuj limit besplatnog nivoa
                lead["_ai"] = _ai_gemini(gem_key, lead)
            uspjelo += 1
        except Exception as e:
            log.warning("AI klasifikacija nije uspjela za %s: %s", lead["oglas_link"], e)
    log.info("AI čitač: klasifikovano %d/%d oglasa.", uspjelo, len(kandidati))

# ──────────────────────────────────────────────────────────────
# SLANJE EMAILA (Resend API)
# ──────────────────────────────────────────────────────────────

def _resend_post(subject: str, html: str, text: str) -> bool:
    if not RESEND_API_KEY:
        log.error("RESEND_API_KEY nije postavljen - email se ne šalje.")
        return False
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


def _tel_cell(lead: dict) -> str:
    return ", ".join(format_phone(p) for p in lead.get("telefoni") or [])


def _ai_cell(lead: dict) -> str:
    ai = lead.get("_ai") or {}
    if not ai.get("presuda"):
        return ""
    return f"{ai['presuda']}: {ai.get('razlog', '')}".rstrip(": ")


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
          <td style="padding:8px;border:1px solid #ddd">{escape(lead['ime'])}</td>
          <td style="padding:8px;border:1px solid #ddd;white-space:nowrap">{escape(_tel_cell(lead))}</td>
          <td style="padding:8px;border:1px solid #ddd">{escape(lead['cijena'])}</td>
          <td style="padding:8px;border:1px solid #ddd">{escape(lead['lokacija'])}</td>
          <td style="padding:8px;border:1px solid #ddd">
            <a href="{escape(lead['oglas_link'])}">{escape(lead['oglas_link'])}</a>
          </td>
          <td style="padding:8px;border:1px solid #ddd">{escape(_ai_cell(lead))}</td>
        </tr>"""
        for lead in leads
    )

    review_block = ""
    if review_leads:
        review_rows = "\n".join(
            f"""        <tr>
          <td style="padding:8px;border:1px solid #ddd">{escape(lead['ime'])}</td>
          <td style="padding:8px;border:1px solid #ddd;white-space:nowrap">{escape(_tel_cell(lead))}</td>
          <td style="padding:8px;border:1px solid #ddd">{escape(lead['cijena'])}</td>
          <td style="padding:8px;border:1px solid #ddd">{escape(lead['lokacija'])}</td>
          <td style="padding:8px;border:1px solid #ddd">
            <a href="{escape(lead['oglas_link'])}">{escape(lead['oglas_link'])}</a>
          </td>
          <td style="padding:8px;border:1px solid #ddd">{escape(lead.get('_razlog',''))}</td>
          <td style="padding:8px;border:1px solid #ddd">{escape(_ai_cell(lead))}</td>
        </tr>"""
            for lead in review_leads
        )
        review_block = f"""
<h3 style="color:#b8860b;margin-top:32px">Za ručnu provjeru ({len(review_leads)})</h3>
<table style="border-collapse:collapse;width:100%;font-size:14px">
  <thead>
    <tr style="background:#fff3cd">
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Ime</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Telefon</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Cijena</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Lokacija</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Link oglasa</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Razlog</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">AI</th>
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
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Telefon</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Cijena</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Lokacija</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">Link oglasa</th>
      <th style="padding:10px;border:1px solid #ddd;text-align:left">AI</th>
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
            f"Telefon:  {_tel_cell(lead)}",
            f"Cijena:   {lead['cijena']}",
            f"Lokacija: {lead['lokacija']}",
            f"Oglas:    {lead['oglas_link']}",
            f"AI:       {_ai_cell(lead)}",
            "",
        ]
    if review_leads:
        text_lines += [f"--- Za ručnu provjeru ({len(review_leads)}) ---", ""]
        for lead in review_leads:
            text_lines += [
                f"Ime:      {lead['ime']}",
                f"Telefon:  {_tel_cell(lead)}",
                f"Cijena:   {lead['cijena']}",
                f"Lokacija: {lead['lokacija']}",
                f"Oglas:    {lead['oglas_link']}",
                f"Razlog:   {lead.get('_razlog','')}",
                f"AI:       {_ai_cell(lead)}",
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


def send_warning_email(sajt: str, fail_count: int, total: int, detalj: str = "") -> None:
    # Upozorenja idu SAMO iz runa koji šalje dnevni mejl, inače ista uzbuna
    # stiže do 4x dnevno (a i lokalni testovi bi je slali).
    if not SEND_EMAIL:
        log.info("Upozorenje za %s se ne šalje (SEND_EMAIL=false).", sajt)
        return
    datum = datetime.now().strftime("%d.%m.%Y %H:%M")
    subject = f"⚠️ Horizon Scraper — selektori pokvareni na {sajt}"
    detalj_html = (f"<p>Početak odgovora servera:</p><pre>{escape(detalj)}</pre>"
                   if detalj else "")
    body = (
        f"<h3>Upozorenje — {sajt}</h3>"
        f"<p>Datum: {datum}</p>"
        f"<p>{fail_count} od {total} oglasa nije moglo biti parsirano jer "
        f"selektori više ne pronalaze elemente na stranici.</p>"
        f"<p>HTML struktura sajta je vjerovatno promijenjena. "
        f"Molimo provjerite skriptu.</p>"
        f"{detalj_html}"
    )
    text = (
        f"UPOZORENJE — {sajt}\n"
        f"Datum: {datum}\n"
        f"{fail_count}/{total} oglasa nije parsirano — selektori su pokvareni.\n"
        f"Provjerite HTML strukturu sajta i ažurirajte skriptu.\n"
        + (f"Početak odgovora servera:\n{detalj}\n" if detalj else "")
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

    soup = BeautifulSoup(r.text, "lxml")

    # ── Datum objave — ciljani element div.oglasi-datum, ne cijeli tekst
    #    stranice (u opisu može stajati bilo koji datum i lažno oboriti oglas).
    #    Oglasi.me daje samo datum bez sata — poredimo po datumu.
    date_tag = soup.select_one("div.oglasi-datum p") or soup.select_one("div.oglasi-datum")
    if date_tag:
        date_m = _ABSOLUTE.search(date_tag.get_text(" ", strip=True))
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

    # ID korisnika stoji u posebnom elementu (npr. "(HE7477)"), ne u imenu
    if user_id is None:
        uname_tag = soup.select_one("p.sidebar-user-status-username")
        if uname_tag:
            um = re.search(r"\((\w+)\)", uname_tag.get_text("", strip=True))
            user_id = um.group(1) if um else None

    # ── Lokacija ────────────────────────────────────────────────
    loc_tag  = (
        soup.select_one("a.ad-breadcrumbs__link[href*='grad-']") or
        soup.select_one("a[href*='/grad-']")
    )
    lokacija = loc_tag.get_text(strip=True) if loc_tag else ""

    # ── Cijena ──────────────────────────────────────────────────
    price_tag = soup.select_one("div.cena p") or soup.select_one("div.cena")
    cijena    = clean_price(price_tag.get_text()) if price_tag else ""

    # ── Telefon(i) — stranica nosi ugrađeni JSON oglašivača:
    #    "phones":[{"phone":"38267015777","viber":true,...}]
    telefoni: list[str] = []
    pm = re.search(r'"phones"\s*:\s*\[(.*?)\]', r.text)
    if pm:
        for m in re.finditer(r'"phone"\s*:\s*"([^"]+)"', pm.group(1)):
            np_ = normalize_phone(m.group(1))
            if np_ and np_ not in telefoni:
                telefoni.append(np_)

    # ── Opis oglasa (za AI klasifikaciju + brojevi upisani u tekst) ──
    opis_tag = soup.select_one("div.oglasi-opis-text") or soup.select_one("div.oglasi-opis")
    opis = opis_tag.get_text(" ", strip=True)[:1500] if opis_tag else ""
    for m in _PHONE_IN_TEXT.finditer(opis):
        np_ = normalize_phone(m.group(0))
        if np_ and np_ not in telefoni:
            telefoni.append(np_)

    return {
        "ime":        name,
        "oglas_link": url,
        "lokacija":   lokacija,
        "cijena":     cijena,
        "izvor":      "Oglasi.me",
        "telefoni":   telefoni,
        "_opis":      opis,
        "_uid":       user_id,
    }


def run_oglasi() -> tuple[list[dict], list[dict]]:
    log.info("═" * 60)
    log.info("  OGLASI.ME  (od %s)", CUTOFF.strftime("%d.%m.%Y %H:%M"))
    log.info("═" * 60)

    queue:       list[str]      = []
    old_in_row:  int            = 0
    total_cards: int            = 0
    uid_count:   dict[str, int] = defaultdict(int)

    for page in range(1, MAX_PAGES + 1):
        r = get(f"{OGLASI_BASE}/nekretnine/all/{page}")
        if r is None:
            break

        soup  = BeautifulSoup(r.text, "lxml")
        cards = _oglasi_cards(soup)
        if not cards:
            log.info("  str. %d: prazno, zaustavljam.", page)
            break
        total_cards += len(cards)

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
    fetched:        int        = 0
    cache_hits:     int        = 0

    for i, url in enumerate(queue, 1):
        cached = seen_get(url)
        if cached is not None:
            # već obrađen u ranijem runu — bez skidanja i bez pauze
            cache_hits += 1
            if cached["status"] in ("vlasnik", "provjeri") and cached.get("lead"):
                lead = dict(cached["lead"])
                lead["_fresh"] = False
                uid = lead.get("_uid") or lead["ime"]
                uid_count[uid] += 1
                raw.append(lead)
            continue

        log.info("  [%4d/%d] %s", i, len(queue), url)
        result = _parse_oglasi_listing(url)
        fetched += 1
        if result == "selector_fail":
            selector_fails += 1
            # keširaj i neuspjeh: obrisani oglasi vraćaju 200 sa generičkom
            # stranicom i inače bi se skidali iznova svaki run
            seen_record(url, "neparsiran")
            _audit_log("Oglasi.me", "", url, "neparsiran", "selector_fail")
        elif result == "old":
            seen_record(url, "star")
            _audit_log("Oglasi.me", "", url, "star", "van cutoffa")
        elif result is None:
            _audit_log("Oglasi.me", "", url, "skip", "HTTP greška")
        else:
            result["_fresh"] = True
            uid = result["_uid"] or result["ime"]
            uid_count[uid] += 1
            raw.append(result)
        time.sleep(DELAY_LISTING)

    if cache_hits:
        log.info("  keš: %d/%d oglasa preskočeno (već obrađeni).", cache_hits, len(queue))

    # Upozorenje samo kad index NE DAJE NIJEDNU karticu (markup promijenjen).
    # Prazan queue je normalan kad prosto nema novih oglasa iz Podgorice.
    if total_cards == 0:
        log.warning("⚠  Oglasi.me: 0 kartica na indexu — stranica vjerovatno promijenjena!")
        send_warning_email("Oglasi.me", 0, 0)
    elif fetched and selector_fails / fetched > SELECTOR_FAIL_RATIO:
        log.warning("⚠  Oglasi.me: %d/%d oglasa nije moglo biti parsirano — selektori pokvareni!",
                    selector_fails, fetched)
        send_warning_email("Oglasi.me", selector_fails, fetched)
    elif selector_fails > 0:
        log.info("ℹ  Oglasi.me: %d/%d oglasa nije parsirano (ispod praga, bez mejla).",
                 selector_fails, fetched)

    for uid, cnt in list(uid_count.items()):
        if BROKER_MIN_LISTINGS - 1 <= cnt <= BROKER_MIN_LISTINGS + 1:
            if uid and re.match(r"^[A-Z]{2}\d+$", uid):
                uid_count[uid] = max(cnt, _oglasi_user_count(uid))
                time.sleep(DELAY_PROFILE)

    leads:        list[dict] = []
    review_leads: list[dict] = []
    for lead in raw:
        fresh = lead.pop("_fresh", False)
        uid   = lead.get("_uid") or lead["ime"]
        count = uid_count[uid]
        if fresh:
            phones_record(lead["oglas_link"], lead["ime"], lead.get("telefoni") or [])
        status, razlog = is_broker(lead["ime"], count, lead.get("telefoni"))

        # svježe skinut oglas ide u keš sa presudom; lead se čuva samo za
        # vlasnika/provjeru (posrednik se ubuduće preskače bez skidanja)
        if fresh:
            stored = dict(lead) if status in ("vlasnik", "provjeri") else None
            seen_record(lead["oglas_link"], status, stored)

        lead.pop("_uid", None)
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
    # dict.fromkeys: deduplikacija koja ČUVA redosljed sa stranice.
    # Regex traži bar jedno slovo/broj u slugu: index sadrži i mrtav link
    # "/oglas/--" koji bi inače trošio 3 HTTP pokušaja svakog runa.
    return list(dict.fromkeys(
        urljoin(PATULJAK_BASE, a["href"].split("?")[0])
        for a in soup.find_all("a", href=re.compile(r"^/oglas/[^\"]*[a-z0-9]", re.I))
    ))


def _patuljak_count_from_href(href: str) -> int:
    m = re.search(r"/profil/\d+/[^/]+/(\d+)/", href)
    return int(m.group(1)) if m else 1


def _parse_patuljak_listing(url: str) -> dict | str | None:
    """
    Vraća:
      dict            — uspješno parsirani oglas
      "old"           — oglas je stariji od CUTOFF
      "drugi_grad"    — oglas nije iz FILTER_CITY (keširati, ne skidati opet)
      None            — HTTP greška (pokušati opet u sljedećem runu)
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
        return "drugi_grad"

    # ── Cijena ──────────────────────────────────────────────────
    price_tag = soup.select_one("div.product_full__cijena")
    if price_tag:
        raw_price = re.sub(r"(?i)cijena\s*:\s*", "", price_tag.get_text(strip=True))
        cijena    = clean_price(raw_price)
    else:
        cijena = ""

    # ── Opis ("Detaljan opis:") — SAMO taj blok, ne cijela stranica:
    #    u HTML-u postoji zakomentarisan stari popup sa tuđim brojem telefona
    #    koji bi regex preko cijele stranice pokupio za svaki oglas.
    opis = ""
    h2 = soup.find("h2", string=re.compile(r"Detaljan opis", re.I))
    if h2 is not None and h2.parent is not None:
        opis = h2.parent.get_text(" ", strip=True)
        opis = re.sub(r"^\s*Detaljan opis:\s*", "", opis)[:1500]

    # Telefon prodavca je iza logina; hvatamo samo brojeve koje oglašivač
    # sam napiše u tekstu opisa.
    telefoni: list[str] = []
    for m in _PHONE_IN_TEXT.finditer(opis):
        np_ = normalize_phone(m.group(0))
        if np_ and np_ not in telefoni:
            telefoni.append(np_)

    return {
        "ime":        name,
        "oglas_link": url,
        "lokacija":   lokacija,
        "cijena":     cijena,
        "izvor":      "Patuljak.me",
        "telefoni":   telefoni,
        "_opis":      opis,
        "_count":     listing_count,
    }


def run_patuljak() -> tuple[list[dict], list[dict]]:
    log.info("═" * 60)
    log.info("  PATULJAK.ME  (od %s)", CUTOFF.strftime("%d.%m.%Y %H:%M"))
    log.info("═" * 60)

    queue:   list[str] = []
    u_queue: set[str]  = set()

    for page in range(1, MAX_PAGES + 1):
        urls = _patuljak_index_urls(page)
        if not urls:
            log.info("  str. %d: prazno, zaustavljam.", page)
            break
        novi = sum(1 for u in urls if seen_get(u) is None)
        log.info("  str. %d: %d oglasa u listi (%d novih)", page, len(urls), novi)
        # dedup KROZ strane: izdvojeni (plaćeni) oglasi stoje na svakoj strani
        # pa bi bez ovoga isti URL ušao u queue i po 15 puta
        for u in urls:
            if u not in u_queue:
                u_queue.add(u)
                queue.append(u)
        # stranica bez ijednog novog oglasa: sve dalje je sigurno već viđeno
        if novi == 0:
            log.info("  str. %d: sve već obrađeno, prekidam paginaciju.", page)
            break
        time.sleep(DELAY_LISTING)

    log.info("URLs za obraditi: %d", len(queue))

    leads:          list[dict] = []
    review_leads:   list[dict] = []
    selector_fails: int        = 0
    fetched:        int        = 0
    cache_hits:     int        = 0

    def _klasifikuj(lead: dict, count: int, fresh: bool) -> None:
        if fresh:
            phones_record(lead["oglas_link"], lead["ime"], lead.get("telefoni") or [])
        status, razlog = is_broker(lead["ime"], count, lead.get("telefoni"))
        if fresh:
            stored = None
            if status in ("vlasnik", "provjeri"):
                stored = dict(lead)
                stored["_count"] = count
            seen_record(lead["oglas_link"], status, stored)
        if status == "posrednik":
            log.info("  [posrednik] %-28s (%s)", lead["ime"], razlog)
            _audit_log("Patuljak.me", lead["ime"], lead["oglas_link"], "posrednik", razlog)
        elif status == "provjeri":
            lead["_razlog"] = razlog
            review_leads.append(lead)
            log.info("  [provjeri]  %-28s (%s)", lead["ime"], razlog)
            _audit_log("Patuljak.me", lead["ime"], lead["oglas_link"], "provjeri", razlog)
        else:
            leads.append(lead)
            _audit_log("Patuljak.me", lead["ime"], lead["oglas_link"], "poslat", "")

    for i, url in enumerate(queue, 1):
        cached = seen_get(url)
        if cached is not None:
            # već obrađen — bez skidanja i bez pauze
            cache_hits += 1
            if cached["status"] in ("vlasnik", "provjeri") and cached.get("lead"):
                lead = dict(cached["lead"])
                _klasifikuj(lead, lead.pop("_count", 1), fresh=False)
            continue

        log.info("  [%4d/%d] %s", i, len(queue), url)
        result = _parse_patuljak_listing(url)
        fetched += 1

        if result == "old":
            seen_record(url, "star")
            _audit_log("Patuljak.me", "", url, "star", "van cutoffa")
        elif result == "selector_fail":
            selector_fails += 1
            # keširaj i neuspjeh (obrisani oglasi: 200 + generička stranica)
            seen_record(url, "neparsiran")
            _audit_log("Patuljak.me", "", url, "neparsiran", "selector_fail")
        elif result == "drugi_grad":
            seen_record(url, "drugi_grad")
            _audit_log("Patuljak.me", "", url, "skip", "drugi grad")
        elif result is None:
            _audit_log("Patuljak.me", "", url, "skip", "HTTP greška")
        else:
            count = result.pop("_count", 1)
            _klasifikuj(result, count, fresh=True)

        # NEMA prekida na "N uzastopnih starih": na vrhu indexa stoje stari
        # IZDVOJENI (plaćeni) oglasi pa bi prekid preskočio svježe ispod njih.
        # Keš garantuje da se svaki oglas ionako skida samo jednom.
        time.sleep(DELAY_LISTING)

    if cache_hits:
        log.info("  keš: %d/%d oglasa preskočeno (već obrađeni).", cache_hits, len(queue))

    if len(queue) == 0:
        log.warning("⚠  Patuljak.me: 0 oglasa pronađeno — stranica vjerovatno promijenjena!")
        send_warning_email("Patuljak.me", 0, 0)
    elif fetched and selector_fails / fetched > SELECTOR_FAIL_RATIO:
        log.warning("⚠  Patuljak.me: %d/%d oglasa nije moglo biti parsirano — selektori pokvareni!",
                    selector_fails, fetched)
        send_warning_email("Patuljak.me", selector_fails, fetched)
    elif selector_fails > 0:
        log.info("ℹ  Patuljak.me: %d/%d oglasa nije parsirano (ispod praga, bez mejla).",
                 selector_fails, fetched)

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
    total_thumbs: int            = 0
    zadnji_html:  str            = ""

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

                zadnji_html = r.text
                soup   = BeautifulSoup(r.text, "lxml")
                thumbs = soup.select("div.thumb_div")
                total_thumbs += len(thumbs)
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

    if total_thumbs == 0:
        log.warning("⚠  Realitica.com: 0 kartica na indexu — stranica vjerovatno promijenjena!")
        log.warning("Realitica odgovor (prvih 300): %r", zadnji_html[:300])
        send_warning_email("Realitica.com", 0, 0, detalj=zadnji_html[:600])

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

    global SEEN, PHONES
    SEEN = load_seen()
    PHONES = load_phones()
    log.info("Keš obrađenih: %d zapisa | registar telefona: %d brojeva.",
             len(SEEN), len(PHONES))

    # Sva 3 sajta paralelno — svaki u svom threadu sa svojom HTTP sesijom.
    # Pad jednog sajta ne obara ostale.
    rezultati: dict[str, tuple[list[dict], list[dict]]] = {}
    poslovi = {"Oglasi.me": run_oglasi, "Patuljak.me": run_patuljak,
               "Realitica.com": run_realitica}
    with ThreadPoolExecutor(max_workers=len(poslovi)) as ex:
        futures = {ex.submit(fn): ime for ime, fn in poslovi.items()}
        for fut in as_completed(futures):
            ime = futures[fut]
            try:
                rezultati[ime] = fut.result()
            except Exception as e:
                log.error("✗  %s: neočekivana greška — sajt preskočen: %s", ime, e)
                rezultati[ime] = ([], [])

    save_seen()
    save_phones()

    leads_o, review_o = rezultati["Oglasi.me"]
    leads_p, review_p = rezultati["Patuljak.me"]
    leads_r, review_r = rezultati["Realitica.com"]
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

    # ── AI čitanje opisa: samo oglasi koji stvarno idu u mejl ──────
    ai_classify(unique_leads + unique_review)

    # AI presuda "agencija" seli vlasnika u rubriku za ručnu provjeru
    # (ne odbacuje se: AI može pogriješiti, Marko presuđuje).
    jos_vlasnici: list[dict] = []
    for l in unique_leads:
        ai = l.get("_ai") or {}
        if ai.get("presuda") == "agencija":
            l["_razlog"] = "AI kaže agencija"
            unique_review.append(l)
        else:
            jos_vlasnici.append(l)
    unique_leads = jos_vlasnici

    # presude u keš, da se isti oglas ne klasifikuje dvaput
    for l in unique_leads + unique_review:
        if l.get("_ai"):
            seen_set_ai(l["oglas_link"], l["_ai"])
    save_seen()

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
