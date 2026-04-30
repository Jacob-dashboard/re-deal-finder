"""
Gmail alert scraper — pulls Zenlist & LoopNet alert emails and converts each
listing into a Deal record.

Two transports are supported:
  1. Gmail API + OAuth2  (preferred)  — set GMAIL_USE_API=True and provide
     credentials.json + token.json (auto-generated on first run)
  2. IMAP + App Password (fallback)   — set GMAIL_USE_API=False and provide
     a 16-char Gmail App Password in config.GMAIL_APP_PASSWORD

Both transports feed the same parser layer.

Two specialized parsers ship out of the box:
  - Zenlist: parses listing cards (address / price / beds / baths / sqft / MLS#)
  - LoopNet: parses commercial alert cards (property name / address / price /
    cap rate / units / sqft / listing URL)

Plus a generic fallback that pulls any address + dollar-amount pair it can find,
so a new alert sender will still produce *something* the pipeline can score.
"""

from __future__ import annotations

import base64
import email
import imaplib
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.header import decode_header
from email.message import Message
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scraper import Deal
import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Email envelope (transport-agnostic)
# ---------------------------------------------------------------------------

@dataclass
class FetchedEmail:
    """One email fetched from Gmail, transport-agnostic."""
    msg_id: str
    sender: str
    subject: str
    date: str
    html: str
    text: str

    @classmethod
    def from_rfc822(cls, msg_id: str, raw_bytes: bytes) -> "FetchedEmail":
        msg: Message = email.message_from_bytes(raw_bytes)
        return cls(
            msg_id=msg_id,
            sender=_decode_header(msg.get("From", "")),
            subject=_decode_header(msg.get("Subject", "")),
            date=msg.get("Date", ""),
            html=_extract_part(msg, "text/html"),
            text=_extract_part(msg, "text/plain"),
        )


def _decode_header(value: str) -> str:
    if not value:
        return ""
    parts = decode_header(value)
    out = []
    for chunk, enc in parts:
        if isinstance(chunk, bytes):
            try:
                out.append(chunk.decode(enc or "utf-8", errors="replace"))
            except LookupError:
                out.append(chunk.decode("utf-8", errors="replace"))
        else:
            out.append(chunk)
    return "".join(out)


def _extract_part(msg: Message, mime: str) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == mime:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    try:
                        return payload.decode(charset, errors="replace")
                    except LookupError:
                        return payload.decode("utf-8", errors="replace")
        return ""
    if msg.get_content_type() == mime:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            try:
                return payload.decode(charset, errors="replace")
            except LookupError:
                return payload.decode("utf-8", errors="replace")
    return ""


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

ADDRESS_RE = re.compile(
    r"\b(\d{1,6}\s+(?:[NSEW]\.?\s+)?[A-Z][A-Za-z0-9.\-' ]{2,40}"
    r"\s+(?:St|Street|Ave|Avenue|Blvd|Boulevard|Rd|Road|Dr|Drive|Ln|Lane|"
    r"Way|Pl|Place|Ct|Court|Ter|Terrace|Pkwy|Parkway|Hwy|Highway))\b",
    re.IGNORECASE,
)
PRICE_RE   = re.compile(r"\$\s?([\d,]+(?:\.\d+)?)\s*(?:M|MM|K)?", re.IGNORECASE)
UNITS_RE   = re.compile(r"(\d+)\s*(?:units?|unit|apartments?)\b", re.IGNORECASE)
SQFT_RE    = re.compile(r"([\d,]+)\s*(?:sq\.?\s*ft|sqft|sf)\b", re.IGNORECASE)
CAP_RE     = re.compile(r"([\d.]+)\s*%\s*(?:cap|cap rate)?", re.IGNORECASE)
ZIP_RE     = re.compile(r"\b(60[0-6]\d{2})\b")  # Chicago/Cook ZIPs
MLS_RE     = re.compile(r"MLS\s*#?\s*([A-Z0-9]{6,12})", re.IGNORECASE)
DOM_RE     = re.compile(r"(\d+)\s*(?:days?\s*on\s*market|DOM)", re.IGNORECASE)


def _to_float(s: str) -> Optional[float]:
    if not s:
        return None
    cleaned = s.replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _parse_price_token(token: str) -> Optional[float]:
    """Handle '$1.2M' / '$1,200,000' / '$695K' uniformly."""
    m = re.search(r"\$\s?([\d,.]+)\s*(M|MM|K)?", token, re.IGNORECASE)
    if not m:
        return None
    base = _to_float(m.group(1))
    if base is None:
        return None
    suffix = (m.group(2) or "").upper()
    if suffix in ("M", "MM"):
        return base * 1_000_000
    if suffix == "K":
        return base * 1_000
    return base


# ---------------------------------------------------------------------------
# Zenlist parser
# ---------------------------------------------------------------------------

def parse_zenlist_email(fe: FetchedEmail) -> list[Deal]:
    """
    Parse a Zenlist alert email into Deal records.

    Zenlist HTML alerts use a card-per-listing layout. We try a few selectors
    for the listing block, then for each block extract the fields we can find.
    Falls back to a per-link scan if the structured selectors miss.
    """
    if not fe.html:
        return []

    soup = BeautifulSoup(fe.html, "lxml")
    deals: list[Deal] = []

    # Each Zenlist listing is rendered as an <a> block wrapping a card; the
    # cards historically use a few different class hooks across template
    # versions, so try a sequence of selectors.
    cards = (
        soup.select("a[href*='zenlist.com/homes/']")
        or soup.select("a[href*='zenlist.com/listing']")
        or soup.select("td.listing-card, div.listing-card, table.listing")
    )

    for card in cards:
        deal = _zenlist_card_to_deal(card)
        if deal:
            deals.append(deal)

    # If structured parsing got nothing, fall back to generic parser
    if not deals:
        deals = parse_generic_email(fe, source="zenlist", channel="on_market")

    # De-dup within this email by (address, price)
    seen = set()
    unique = []
    for d in deals:
        key = ((d.address or "").upper().strip(), int(d.price or 0))
        if key in seen:
            continue
        seen.add(key)
        unique.append(d)

    logger.info("Zenlist: parsed %d listings from email '%s'", len(unique), fe.subject[:60])
    return unique


def _zenlist_card_to_deal(card) -> Optional[Deal]:
    text = card.get_text(separator=" ", strip=True)
    if not text or len(text) < 5:
        return None

    deal = Deal(
        source="gmail_zenlist",
        channel="on_market",
        asset_class="multifamily",
    )

    # URL — Zenlist links are <a href=...> directly on the card
    href = card.get("href") if hasattr(card, "get") else None
    if not href:
        link = card.find("a", href=True)
        href = link["href"] if link else None
    if href:
        deal.url = href.strip()

    # Address
    m = ADDRESS_RE.search(text)
    if m:
        deal.address = m.group(1).strip()

    # ZIP
    z = ZIP_RE.search(text)
    if z:
        deal.zip_code = z.group(1)

    # Price (largest dollar amount in the card)
    price_candidates = [
        _parse_price_token(p.group(0)) for p in PRICE_RE.finditer(text)
    ]
    price_candidates = [p for p in price_candidates if p and p >= 50_000]
    if price_candidates:
        deal.price = max(price_candidates)

    # Units / sqft / cap / DOM / MLS
    if (m := UNITS_RE.search(text)):
        deal.units = int(m.group(1))
    if (m := SQFT_RE.search(text)):
        deal.sqft = int(m.group(1).replace(",", ""))
    if (m := CAP_RE.search(text)):
        cap = _to_float(m.group(1))
        if cap and cap < 30:  # sanity — discard 100% etc.
            deal.cap_rate = cap
    if (m := DOM_RE.search(text)):
        deal.days_on_market = int(m.group(1))
    if (m := MLS_RE.search(text)):
        deal.external_id = m.group(1)

    # Price per unit
    if deal.price and deal.units and deal.units > 0:
        deal.price_per_unit = deal.price / deal.units

    deal.city = "Chicago"
    deal.state = "IL"
    deal.raw = {"snippet": text[:400]}

    # Reject if we got almost nothing — generic parser will pick up the slack
    if not (deal.address or deal.price):
        return None

    return deal


# ---------------------------------------------------------------------------
# LoopNet parser (email alerts)
# ---------------------------------------------------------------------------

def parse_loopnet_email(fe: FetchedEmail) -> list[Deal]:
    """
    Parse a LoopNet alert email into Deal records.

    LoopNet alerts ship as HTML with one listing per row/table-cell. Each row
    typically contains: property name, address, price, # units, sqft, cap rate,
    and a link back to loopnet.com/Listing/...
    """
    if not fe.html:
        return []

    soup = BeautifulSoup(fe.html, "lxml")
    deals: list[Deal] = []

    cards = (
        soup.select("a[href*='loopnet.com/Listing']")
        or soup.select("a[href*='loopnet.com/listing']")
        or soup.select("table.listing, td.listing, div.listing")
    )

    for card in cards:
        deal = _loopnet_card_to_deal(card)
        if deal:
            deals.append(deal)

    if not deals:
        deals = parse_generic_email(fe, source="loopnet", channel="on_market")

    seen = set()
    unique = []
    for d in deals:
        key = ((d.address or "").upper().strip(), int(d.price or 0))
        if key in seen:
            continue
        seen.add(key)
        unique.append(d)

    logger.info("LoopNet email: parsed %d listings from '%s'", len(unique), fe.subject[:60])
    return unique


def _loopnet_card_to_deal(card) -> Optional[Deal]:
    text = card.get_text(separator=" ", strip=True)
    if not text or len(text) < 5:
        return None

    deal = Deal(
        source="gmail_loopnet",
        channel="on_market",
        asset_class="multifamily",
    )

    href = card.get("href") if hasattr(card, "get") else None
    if not href:
        link = card.find("a", href=True)
        href = link["href"] if link else None
    if href:
        deal.url = urljoin("https://www.loopnet.com", href.strip())

    if (m := ADDRESS_RE.search(text)):
        deal.address = m.group(1).strip()
    if (m := ZIP_RE.search(text)):
        deal.zip_code = m.group(1)

    price_candidates = [
        _parse_price_token(p.group(0)) for p in PRICE_RE.finditer(text)
    ]
    price_candidates = [p for p in price_candidates if p and p >= 50_000]
    if price_candidates:
        deal.price = max(price_candidates)

    if (m := UNITS_RE.search(text)):
        deal.units = int(m.group(1))
    if (m := SQFT_RE.search(text)):
        deal.sqft = int(m.group(1).replace(",", ""))
    if (m := CAP_RE.search(text)):
        cap = _to_float(m.group(1))
        if cap and cap < 30:
            deal.cap_rate = cap

    # LoopNet alerts often spell out "mixed-use" / "apartment building"
    lower = text.lower()
    if "mixed-use" in lower or "mixed use" in lower:
        deal.asset_class = "mixed-use"
    elif "apartment" in lower:
        deal.asset_class = "multifamily"

    if deal.price and deal.units and deal.units > 0:
        deal.price_per_unit = deal.price / deal.units

    deal.city = "Chicago"
    deal.state = "IL"
    deal.raw = {"snippet": text[:400]}

    if not (deal.address or deal.price):
        return None

    return deal


# ---------------------------------------------------------------------------
# Generic fallback parser
# ---------------------------------------------------------------------------

def parse_generic_email(fe: FetchedEmail, source: str = "gmail_generic",
                        channel: str = "on_market") -> list[Deal]:
    """
    Last-resort parser. Walks plain text + HTML, finds (address, price)
    pairs in proximity, and emits a Deal per pair.

    This is intentionally lossy — it's better to surface a bare-bones deal than
    to drop alert email content the pipeline could otherwise score on address
    alone.
    """
    text = ""
    if fe.html:
        text = BeautifulSoup(fe.html, "lxml").get_text(separator="\n", strip=True)
    text = text or fe.text or ""
    if not text:
        return []

    # Walk line-by-line and group into "blocks" separated by blank lines.
    blocks = re.split(r"\n\s*\n", text)
    deals: list[Deal] = []

    for block in blocks:
        if len(block) < 10:
            continue

        addr_m = ADDRESS_RE.search(block)
        if not addr_m:
            continue

        deal = Deal(
            source=source,
            channel=channel,
            asset_class="multifamily",
            address=addr_m.group(1).strip(),
            city="Chicago",
            state="IL",
        )

        if (m := ZIP_RE.search(block)):
            deal.zip_code = m.group(1)

        prices = [_parse_price_token(p.group(0)) for p in PRICE_RE.finditer(block)]
        prices = [p for p in prices if p and p >= 50_000]
        if prices:
            deal.price = max(prices)

        if (m := UNITS_RE.search(block)):
            deal.units = int(m.group(1))
        if (m := SQFT_RE.search(block)):
            deal.sqft = int(m.group(1).replace(",", ""))
        if (m := CAP_RE.search(block)):
            cap = _to_float(m.group(1))
            if cap and cap < 30:
                deal.cap_rate = cap

        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units

        deal.raw = {"snippet": block[:300], "subject": fe.subject}
        deals.append(deal)

    return deals


# ---------------------------------------------------------------------------
# Sender → parser routing
# ---------------------------------------------------------------------------

PARSERS = [
    ("zenlist",  parse_zenlist_email),
    ("loopnet",  parse_loopnet_email),
]


def parse_email(fe: FetchedEmail) -> list[Deal]:
    """Route an email to its sender-specific parser, or fall back to generic."""
    sender_lower = (fe.sender or "").lower()
    subject_lower = (fe.subject or "").lower()

    for token, parser in PARSERS:
        if token in sender_lower or token in subject_lower:
            return parser(fe)

    # Unknown sender — try generic
    return parse_generic_email(fe)


# ---------------------------------------------------------------------------
# IMAP transport
# ---------------------------------------------------------------------------

class GmailIMAPClient:
    """Thin IMAP wrapper with one search-by-sender method."""

    def __init__(self, email_addr: str, app_password: str,
                 imap_server: str = "imap.gmail.com"):
        if not email_addr or not app_password:
            raise ValueError(
                "Gmail IMAP requires both email_addr and app_password "
                "(set GMAIL_EMAIL and GMAIL_APP_PASSWORD in config.py)"
            )
        self.email_addr = email_addr
        self.app_password = app_password
        self.imap_server = imap_server
        self.mail: Optional[imaplib.IMAP4_SSL] = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.disconnect()

    def connect(self) -> None:
        logger.info("Gmail IMAP: connecting to %s as %s", self.imap_server, self.email_addr)
        self.mail = imaplib.IMAP4_SSL(self.imap_server)
        self.mail.login(self.email_addr, self.app_password)
        self.mail.select("inbox")

    def disconnect(self) -> None:
        if self.mail:
            try:
                self.mail.close()
            except Exception:
                pass
            try:
                self.mail.logout()
            except Exception:
                pass
            self.mail = None

    def search_from(self, sender_keyword: str, since_days: int = 1) -> list[FetchedEmail]:
        if not self.mail:
            raise RuntimeError("Not connected — call connect() first")
        since = (datetime.now() - timedelta(days=since_days)).strftime("%d-%b-%Y")
        criteria = f'(FROM "{sender_keyword}" SINCE {since})'
        typ, data = self.mail.search(None, criteria)
        if typ != "OK" or not data or not data[0]:
            return []
        ids = data[0].split()
        results: list[FetchedEmail] = []
        for msg_id in ids:
            typ, msg_data = self.mail.fetch(msg_id, "(RFC822)")
            if typ != "OK" or not msg_data or not msg_data[0]:
                continue
            raw = msg_data[0][1]
            results.append(FetchedEmail.from_rfc822(msg_id.decode(), raw))
        logger.info("Gmail IMAP: '%s' → %d emails (last %d days)",
                    sender_keyword, len(results), since_days)
        return results


# ---------------------------------------------------------------------------
# Gmail API transport
# ---------------------------------------------------------------------------

class GmailAPIClient:
    """
    Gmail API wrapper. Lazily imports google libs so the IMAP path remains
    usable without google-auth installed.
    """

    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

    def __init__(self, credentials_path: str, token_path: str):
        self.credentials_path = credentials_path
        self.token_path = token_path
        self._service = None

    def connect(self):
        try:
            from googleapiclient.discovery import build
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            from google_auth_oauthlib.flow import InstalledAppFlow
        except ImportError as e:
            raise ImportError(
                "Gmail API requires google libs. Install with:\n"
                "  pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib"
            ) from e

        creds = None
        if Path(self.token_path).exists():
            creds = Credentials.from_authorized_user_file(self.token_path, self.SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                if not Path(self.credentials_path).exists():
                    raise FileNotFoundError(
                        f"OAuth credentials file not found: {self.credentials_path}\n"
                        "See GMAIL_SETUP.md for how to download credentials.json from "
                        "the Google Cloud Console."
                    )
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_path, self.SCOPES
                )
                creds = flow.run_local_server(port=0)
            with open(self.token_path, "w") as f:
                f.write(creds.to_json())

        self._service = build("gmail", "v1", credentials=creds)
        logger.info("Gmail API: connected")

    def search_from(self, sender_keyword: str, since_days: int = 1) -> list[FetchedEmail]:
        if not self._service:
            self.connect()

        after = (datetime.now() - timedelta(days=since_days)).strftime("%Y/%m/%d")
        query = f'from:{sender_keyword} after:{after}'
        resp = self._service.users().messages().list(
            userId="me", q=query, maxResults=100
        ).execute()
        messages = resp.get("messages", [])

        out: list[FetchedEmail] = []
        for m in messages:
            full = self._service.users().messages().get(
                userId="me", id=m["id"], format="raw"
            ).execute()
            raw = base64.urlsafe_b64decode(full["raw"].encode("ASCII"))
            out.append(FetchedEmail.from_rfc822(m["id"], raw))

        logger.info("Gmail API: '%s' → %d emails (last %d days)",
                    sender_keyword, len(out), since_days)
        return out


# ---------------------------------------------------------------------------
# Top-level scraper interface (matches other scrapers)
# ---------------------------------------------------------------------------

def _build_client():
    """Pick the right transport based on config."""
    use_api = getattr(config, "GMAIL_USE_API", False)
    if use_api:
        return GmailAPIClient(
            credentials_path=getattr(config, "GMAIL_CREDENTIALS_PATH", "credentials.json"),
            token_path=getattr(config, "GMAIL_TOKEN_PATH", "token.json"),
        )
    email_addr = getattr(config, "GMAIL_EMAIL", "") or os.environ.get("GMAIL_EMAIL", "")
    app_pw     = getattr(config, "GMAIL_APP_PASSWORD", "") or os.environ.get("GMAIL_APP_PASSWORD", "")
    if not (email_addr and app_pw):
        raise RuntimeError(
            "Gmail IMAP not configured — set GMAIL_EMAIL and GMAIL_APP_PASSWORD in "
            "config.py (or env). See GMAIL_SETUP.md for how to generate an App Password."
        )
    return GmailIMAPClient(email_addr, app_pw)


def parse_html_file(path: str, source: str = "gmail_zenlist") -> list[Deal]:
    """Parse a sample email saved on disk — used for tests / dev work."""
    html = Path(path).read_text(encoding="utf-8", errors="replace")
    fe = FetchedEmail(
        msg_id=f"file:{path}",
        sender=source.replace("gmail_", ""),
        subject=Path(path).stem,
        date="",
        html=html,
        text="",
    )
    return parse_email(fe)


def scrape(dry_run: bool = False, limit: int = 50,
           since_days: int = 1) -> list[Deal]:
    """
    Main entry point. Reads Zenlist + LoopNet alert emails from Gmail and
    returns Deal records.

    dry_run=True: parses tests/sample_zenlist_email.html so you can exercise
    the pipeline without touching Gmail.
    """
    if dry_run:
        sample = Path(__file__).resolve().parent.parent / "tests" / "sample_zenlist_email.html"
        if sample.exists():
            logger.info("Gmail: dry-run mode — parsing %s", sample)
            return parse_html_file(str(sample))
        logger.info("Gmail: dry-run mode — no sample file found, returning empty")
        return []

    try:
        client = _build_client()
    except (RuntimeError, ImportError, FileNotFoundError, ValueError) as e:
        logger.warning("Gmail: %s — skipping channel", e)
        return []

    deals: list[Deal] = []
    try:
        if isinstance(client, GmailIMAPClient):
            client.connect()
        for sender_kw in ("zenlist", "loopnet"):
            try:
                emails = client.search_from(sender_kw, since_days=since_days)
            except Exception as e:
                logger.warning("Gmail: search for '%s' failed: %s", sender_kw, e)
                continue
            for fe in emails:
                try:
                    deals.extend(parse_email(fe))
                except Exception as e:
                    logger.warning("Gmail: parse failed for '%s': %s", fe.subject[:60], e)
                if len(deals) >= limit:
                    break
            if len(deals) >= limit:
                break
    finally:
        if isinstance(client, GmailIMAPClient):
            client.disconnect()

    logger.info("Gmail: scrape complete — %d deals total", len(deals))
    return deals[:limit]


__all__ = [
    "scrape",
    "FetchedEmail",
    "GmailIMAPClient",
    "GmailAPIClient",
    "parse_email",
    "parse_zenlist_email",
    "parse_loopnet_email",
    "parse_generic_email",
    "parse_html_file",
]
