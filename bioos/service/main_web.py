from __future__ import annotations

import base64
import http.cookiejar
import json
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

from bioos.service.rest_base import RestClient

LOGIN_AES_KEY = b"0102030405060708"
DEFAULT_LOGIN_REFERER = "/product/mc"


def encrypt_main_web_password(password: str) -> str:
    plaintext = password.encode("utf-8")
    pad_size = 16 - (len(plaintext) % 16)
    padded = plaintext + bytes([pad_size]) * pad_size

    try:
        from Crypto.Cipher import AES  # type: ignore

        cipher = AES.new(LOGIN_AES_KEY, AES.MODE_ECB)
        ciphertext = cipher.encrypt(padded)
        return base64.b64encode(ciphertext).decode("utf-8")
    except Exception:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

        cipher = Cipher(algorithms.AES(LOGIN_AES_KEY), modes.ECB(), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded) + encryptor.finalize()
        return base64.b64encode(ciphertext).decode("utf-8")


class MainWebLoginSession:
    def __init__(self, base_url: str, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.cookie_jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cookie_jar))

    def initialize(self) -> dict:
        request = urllib.request.Request(
            f"{self.base_url}/login?redirect_uri=%2F",
            headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
        )
        with self.opener.open(request, timeout=self.timeout):
            pass
        return self.export_auth()

    def login(
        self,
        *,
        account_name: str,
        password: str,
        user_name: Optional[str] = None,
        platform: str = "tenant",
        language: str = "zh",
    ) -> dict:
        auth = self.initialize()
        csrf_token = auth.get("csrf_token")
        payload: Dict[str, Any] = {
            "Platform": platform,
            "UserType": "user" if user_name else "account",
            "AccountName": account_name,
            "Pass": encrypt_main_web_password(password),
        }
        if user_name:
            payload["UserName"] = user_name

        request = urllib.request.Request(
            f"{self.base_url}/api/auth/login",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            method="POST",
            headers={
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Referer": DEFAULT_LOGIN_REFERER,
                "x-language": language,
                **({"x-csrf-token": csrf_token} if csrf_token else {}),
            },
        )
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:  # type: ignore[attr-defined]
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Main web login failed: HTTP {exc.code}: {body}") from exc

        result = json.loads(body) if body else {}
        exported = self.export_auth()
        exported["login_result"] = result
        exported["login_token"] = result.get("LoginToken")
        return exported

    def export_auth(self) -> dict:
        cookies = []
        csrf_token = None
        for cookie in self.cookie_jar:
            cookies.append(f"{cookie.name}={cookie.value}")
            if cookie.name == "csrfToken":
                csrf_token = cookie.value
        return {
            "web_url": self.base_url,
            "csrf_token": csrf_token,
            "cookie": "; ".join(cookies) or None,
        }


class MainWebClient(RestClient):
    def __init__(
        self,
        base_url: str,
        *,
        login_token: Optional[str] = None,
        csrf_token: Optional[str] = None,
        cookie: Optional[str] = None,
        timeout: int = 30,
    ) -> None:
        headers: Dict[str, str] = {"X-TOP-REGION": "cn-north-1"}
        if login_token:
            headers["X-LoginToken"] = login_token
        if csrf_token:
            headers["x-csrf-token"] = csrf_token
        super().__init__(base_url=base_url, cookie=cookie, timeout=timeout)
        self.default_headers = headers

    @classmethod
    def login_with_password(
        cls,
        *,
        base_url: str,
        account_name: str,
        password: str,
        user_name: Optional[str] = None,
        timeout: int = 30,
    ) -> dict:
        session = MainWebLoginSession(base_url=base_url, timeout=timeout)
        return session.login(account_name=account_name, password=password, user_name=user_name)

    def graphql(self, operation_name: str, query: str, variables: Optional[Dict[str, Any]] = None) -> Any:
        payload = {
            "operationName": operation_name,
            "query": query,
            "variables": variables or {},
        }
        return self.request(
            "POST",
            f"/api/product/bio-pipeline/graphql?{operation_name}",
            json_body=payload,
            headers=self.default_headers,
        )

    def check_repository_account_exist(self) -> Any:
        query = """
query CheckRepositoryAccountExist($body: CheckRepositoryAccountExistRequestInput) {
  CheckRepositoryAccountExist(body: $body) {
    Exist
    Email
    Organizations {
      ID
      Name
      Owner
      Phone
    }
  }
}
""".strip()
        result = self.graphql(
            "CheckRepositoryAccountExist",
            query,
            {"body": {}},
        )
        if isinstance(result, dict):
            data = result.get("data")
            if isinstance(data, dict):
                return data.get("CheckRepositoryAccountExist", data)
        return result

    def get_repository_passport(self, *, expires_in: Optional[int] = None) -> Any:
        query = """
query GetRepositoryPassport($body: NetworkGetRepositoryPassportRequestInput) {
  GetRepositoryPassport(body: $body) {
    Passport
  }
}
""".strip()
        body: Dict[str, Any] = {}
        if expires_in is not None:
            body["ExpiresIn"] = expires_in
        result = self.graphql(
            "GetRepositoryPassport",
            query,
            {"body": body},
        )
        if isinstance(result, dict):
            data = result.get("data")
            if isinstance(data, dict):
                return data.get("GetRepositoryPassport", data)
        return result
