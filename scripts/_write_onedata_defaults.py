"""Regenerate pieces/common/onedata_defaults.py with a fresh OneData access token."""

from __future__ import annotations

import base64
import json
import urllib.request
from pathlib import Path

HOST = "https://data.spice-platform.eu"
AUTH = base64.b64encode(b"filip.chrvala:Hah3ohe1").decode()
HEADERS = {"Content-Type": "application/json", "Authorization": f"Basic {AUTH}"}
OUT = Path(__file__).resolve().parents[1] / "pieces" / "common" / "onedata_defaults.py"


def _post(path: str, body: dict) -> dict:
    data = json.dumps(body).encode()
    req = urllib.request.Request(f"{HOST}{path}", data=data, method="POST", headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _fetch_token() -> str:
    try:
        out = _post(
            "/api/v3/onezone/user/tokens/named",
            {"name": "domino-uc32-cost-optimizer", "type": {"accessToken": {}}},
        )
        return str(out["token"])
    except Exception:
        req = urllib.request.Request(f"{HOST}/api/v3/onezone/provider/public/get_current_time")
        with urllib.request.urlopen(req, timeout=30) as resp:
            now = int(json.load(resp)["timeMillis"] // 1000)
        out = _post(
            "/api/v3/onezone/user/tokens/temporary",
            {
                "name": "domino-uc32-cost-optimizer-temp",
                "type": {"accessToken": {}},
                "expiresAt": now + 86400 * 30,
            },
        )
        return str(out["token"])


def main() -> None:
    token = _fetch_token()
    OUT.write_text(
        f'''"""Hardcoded OneData credentials for Domino dev (UC3.2 Cost Optimizer).

Domino workflow secrets can stay empty — pieces read these defaults when OneData
paths are used. Remove or externalize before a public release.
"""

DEFAULT_ONEZONE_HOST = "data.spice-platform.eu"
DEFAULT_INPUT_DIR = "onedata:///FilipsSpace/cost_optimizer/inputs"
DEFAULT_OUTPUT_DIR = "onedata:///FilipsSpace/cost_optimizer/outputs"
DEFAULT_ONEDATA_TOKEN = {token!r}
''',
        encoding="utf-8",
    )
    print("Wrote", OUT)


if __name__ == "__main__":
    main()
