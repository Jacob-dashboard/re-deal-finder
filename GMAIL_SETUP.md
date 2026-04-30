# Gmail Alert Scraper — Setup

The Gmail scraper reads **Zenlist** and **LoopNet** alert emails from
`Aiagent974@gmail.com` and pipes the listings into the deal pipeline.

It supports two transports:

| Transport | Auth                  | Setup difficulty | When to use                              |
|-----------|-----------------------|------------------|------------------------------------------|
| IMAP      | Gmail App Password    | 2 minutes        | **Default.** Quick start, fewest moving parts. |
| Gmail API | OAuth2 client secret  | 10 minutes       | Production / long-lived, more rate headroom. |

You only need to set up **one** of them. Pick IMAP unless you have a reason not to.

---

## Option 1 — IMAP (recommended for now)

Gmail does not allow a regular password to be used by IMAP — you need a
**16-character App Password**. This requires 2-Step Verification to be enabled
on the account.

### Steps

1. Go to <https://myaccount.google.com/security>
2. Enable **2-Step Verification** if it isn't already on
3. Once 2-Step is on, scroll to the bottom of the Security page and click
   **App passwords** (or visit <https://myaccount.google.com/apppasswords>)
4. Pick app: **Mail**, device: **Other** → name it `re-deal-finder`, click **Generate**
5. Copy the 16-character password (no spaces)
6. Open `config.py` in this repo and set:
   ```python
   GMAIL_EMAIL        = "Aiagent974@gmail.com"
   GMAIL_APP_PASSWORD = "xxxxxxxxxxxxxxxx"   # the 16 chars from step 5
   GMAIL_USE_API      = False
   ```
7. Test:
   ```bash
   python run_search.py --channel email --limit 10
   ```

You should see log lines like
`Gmail IMAP: 'zenlist' → N emails (last 1 days)`.

### Don't commit the password

Add it via env var instead if you'd rather not put it in `config.py`:

```bash
export GMAIL_APP_PASSWORD="xxxxxxxxxxxxxxxx"
```

The scraper falls back to `GMAIL_APP_PASSWORD` / `GMAIL_EMAIL` from the
environment when the values in `config.py` are blank.

---

## Option 2 — Gmail API (OAuth2)

Use this when you want longer-lived auth, finer-grained control, or you can't
turn on 2-Step Verification.

### Steps

1. Go to the Google Cloud Console: <https://console.cloud.google.com/>
2. Create a new project (or pick an existing one)
3. **APIs & Services** → **Library** → search **Gmail API** → Enable
4. **APIs & Services** → **OAuth consent screen**
   - User type: **External**
   - Add yourself (`Aiagent974@gmail.com`) as a Test user
   - Scopes: add `.../auth/gmail.readonly`
5. **APIs & Services** → **Credentials** → **Create Credentials** →
   **OAuth client ID** → **Desktop app**
6. Download the JSON, save it as `credentials.json` at the repo root
7. Install the Google client libraries:
   ```bash
   pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib
   ```
8. In `config.py`, set:
   ```python
   GMAIL_USE_API = True
   GMAIL_CREDENTIALS_PATH = "credentials.json"
   GMAIL_TOKEN_PATH       = "token.json"
   ```
9. First run will pop a browser for consent — approve, and `token.json` will
   be written next to `credentials.json` for all future runs:
   ```bash
   python run_search.py --channel email --limit 10
   ```

Both `credentials.json` and `token.json` are already covered by `.gitignore`
patterns — never commit them.

---

## Usage

The `email` channel runs the Gmail scraper standalone:

```bash
python run_search.py --channel email
```

It also runs as part of the default `all` pipeline:

```bash
python run_search.py            # equivalent to --channel all
```

Dry-run uses the bundled `tests/sample_zenlist_email.html` fixture — no
network, no Gmail access needed:

```bash
python run_search.py --channel email --dry-run
```

## What gets parsed

| Sender    | Fields extracted                                            |
|-----------|-------------------------------------------------------------|
| Zenlist   | address, ZIP, price, units, sqft, cap rate, MLS#, DOM, URL  |
| LoopNet   | address, ZIP, price, units, sqft, cap rate, asset class, URL |
| (any other) | address + price (best-effort generic parser)              |

Listings then flow through the standard `pipeline.filter` → `pipeline.scorer`
→ `pipeline.alert` chain — same as the LoopNet/Crexi web scrapers.

## Troubleshooting

- **`AUTHENTICATIONFAILED` from IMAP**: the app password is wrong. Generate a
  new one and try again. Make sure 2-Step Verification is on for the account.
- **`Gmail: 'zenlist' → 0 emails`**: no Zenlist emails landed in the last
  `GMAIL_LOOKBACK_DAYS` (default 1). Bump it in `config.py`, or check the
  inbox manually.
- **Listings show up but with no price/units**: the alert template likely
  uses a layout the parser doesn't recognize yet. Save a copy to
  `tests/sample_zenlist_email.html` and the parsers can be refined against it.
