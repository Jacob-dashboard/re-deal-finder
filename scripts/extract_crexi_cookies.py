#!/usr/bin/env python3
"""
Extract Crexi cookies from local Chrome and save to config/crexi_cookies.json.

Same approach used for LoopNet:
  - read Chrome's encrypted Cookies SQLite DB
  - decrypt with the AES key derived from the macOS Keychain entry
  - filter rows whose host_key matches *crexi*
  - emit Playwright-compatible JSON

Run: python3 scripts/extract_crexi_cookies.py
"""

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


def _keychain_password() -> str:
    r = subprocess.run(
        ["security", "find-generic-password", "-s", "Chrome Safe Storage", "-w"],
        capture_output=True, text=True, check=True,
    )
    return r.stdout.strip()


def _derive_key(password: str) -> bytes:
    return hashlib.pbkdf2_hmac("sha1", password.encode(), b"saltysalt", 1003, dklen=16)


def _decrypt_v10(blob: bytes, key: bytes, host: str) -> str:
    """Decrypt a Chrome v10 cookie value. Chrome 127+ on macOS prefixes the
    plaintext with sha256(host_key) for binding — strip those 32 bytes when
    they match."""
    iv = b" " * 16
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    dec = cipher.decryptor()
    plain = dec.update(blob[3:]) + dec.finalize()
    pad_len = plain[-1]
    body = plain[:-pad_len]
    expected_prefix = hashlib.sha256(host.encode()).digest()
    if len(body) >= 32 and body[:32] == expected_prefix:
        body = body[32:]
    return body.decode("utf-8", errors="ignore")


def main() -> int:
    cookies_db = os.path.expanduser(
        "~/Library/Application Support/Google/Chrome/Default/Cookies"
    )
    if not os.path.exists(cookies_db):
        print(f"Chrome cookies DB not found at {cookies_db}", file=sys.stderr)
        return 1

    key = _derive_key(_keychain_password())
    tmp = "/tmp/_crexi_cookies_extract.sqlite"
    shutil.copy2(cookies_db, tmp)

    out = []
    try:
        conn = sqlite3.connect(tmp)
        c = conn.cursor()
        c.execute(
            "SELECT host_key, name, encrypted_value, path, is_secure "
            "FROM cookies WHERE host_key LIKE '%crexi%'"
        )
        for host, name, enc, path, secure in c.fetchall():
            try:
                value = _decrypt_v10(enc, key, host) if enc[:3] == b"v10" else enc.decode("utf-8", "ignore")
            except Exception as e:
                print(f"  decrypt fail {name}: {e}", file=sys.stderr)
                continue
            out.append({
                "name": name,
                "value": value,
                "domain": host,
                "path": path,
                "secure": bool(secure),
                "httpOnly": False,
                "sameSite": "Lax",
            })
        conn.close()
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass

    os.makedirs("config", exist_ok=True)
    with open("config/crexi_cookies.json", "w") as f:
        json.dump(out, f, indent=2)

    print(f"Saved {len(out)} Crexi cookies to config/crexi_cookies.json")
    if not out:
        print(
            "No cookies found. Make sure Chrome (Default profile) has visited "
            "crexi.com while logged in, then re-run.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
