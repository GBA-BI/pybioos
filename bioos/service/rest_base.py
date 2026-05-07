from __future__ import annotations

import json
import subprocess
from typing import Any, Dict, Optional
from urllib import parse


class RestClient:
    def __init__(self, base_url: str, token: Optional[str] = None, cookie: Optional[str] = None, timeout: int = 30):
        if not base_url:
            raise ValueError("Missing base URL.")
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.cookie = cookie
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        if params:
            query = parse.urlencode({key: value for key, value in params.items() if value is not None}, doseq=True)
            if query:
                url = f"{url}?{query}"

        cmd = [
            "curl",
            "--silent",
            "--show-error",
            "--location",
            "--write-out",
            "\n__BIOOS_STATUS__:%{http_code}\n__BIOOS_CONTENT_TYPE__:%{content_type}",
            "--max-time",
            str(self.timeout),
            "-X",
            method.upper(),
            "-H",
            "Accept: application/json, text/plain, */*",
        ]
        if self.token:
            cmd.extend(["-H", f"Authorization: Bearer {self.token}"])
        if self.cookie:
            cmd.extend(["-H", f"Cookie: {self.cookie}"])
        if headers:
            for key, value in headers.items():
                cmd.extend(["-H", f"{key}: {value}"])
        if json_body is not None:
            cmd.extend(["-H", "Content-Type: application/json", "--data", json.dumps(json_body, ensure_ascii=False)])
        cmd.append(url)

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Request failed for {method.upper()} {url}: {result.stderr.strip() or result.stdout.strip()}")

        raw_output = result.stdout
        status_marker = "\n__BIOOS_STATUS__:"
        content_type_marker = "\n__BIOOS_CONTENT_TYPE__:"
        status_pos = raw_output.rfind(status_marker)
        content_type_pos = raw_output.rfind(content_type_marker)
        if status_pos == -1 or content_type_pos == -1 or content_type_pos < status_pos:
            raise RuntimeError(f"Unexpected curl output for {method.upper()} {url}.")

        body = raw_output[:status_pos]
        status_code = raw_output[status_pos + len(status_marker):content_type_pos].strip()
        content_type = raw_output[content_type_pos + len(content_type_marker):].strip()
        try:
            status = int(status_code)
        except ValueError as exc:
            raise RuntimeError(f"Invalid HTTP status for {method.upper()} {url}: {status_code}") from exc

        if status >= 400:
            raise RuntimeError(f"HTTP {status} for {method.upper()} {url}: {body.strip()}")

        if "application/json" in content_type:
            return json.loads(body) if body else {}
        return body
