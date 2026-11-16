"""Поиск кандидатов мест через REST и MCP Geoapify, без сторонних пакетов."""

import json
import os
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def load_api_key(env_file):
    """Переменная окружения имеет приоритет над GEOAPIFY_API_KEY в .env."""
    key = os.getenv("GEOAPIFY_API_KEY", "").strip()
    if key:
        return key
    path = Path(env_file)
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            name, separator, value = line.partition("=")
            if separator and name.strip() == "GEOAPIFY_API_KEY":
                key = value.strip().strip("\"'")
                break
    if not key:
        raise ValueError("Задайте GEOAPIFY_API_KEY в окружении или локальном .env")
    return key


class GeoapifyClient:
    """Тайм-аут 20 с, до двух попыток для 429/5xx; ключ скрыт в ошибках."""

    def __init__(self, api_key, timeout=20):
        if not api_key.strip():
            raise ValueError("Нужен API-ключ Geoapify")
        self._api_key = api_key.strip()
        self.timeout = timeout

    def _request(self, endpoint, params=None, body=None):
        url = "https://api.geoapify.com/" + endpoint
        headers = {"User-Agent": "reels-router-research/0.1"}
        data = None
        if body is None:
            url += "?" + urlencode({**params, "apiKey": self._api_key})
        else:
            headers.update({"x-api-key": self._api_key, "Content-Type": "application/json"})
            data = json.dumps(body).encode("utf-8")
        started = time.perf_counter()
        for attempt in range(2):
            try:
                with urlopen(Request(url, data=data, headers=headers), timeout=self.timeout) as response:
                    payload = json.load(response)
                return {
                    "endpoint": endpoint,
                    "elapsed_seconds": round(time.perf_counter() - started, 3),
                    "attempts": attempt + 1,
                    "payload": payload,
                }
            except HTTPError as error:
                if attempt == 0 and (error.code == 429 or 500 <= error.code < 600):
                    time.sleep(2)
                    continue
                raise RuntimeError(f"Geoapify HTTP {error.code}, endpoint {endpoint}") from None
            except (URLError, TimeoutError):
                raise RuntimeError(f"Geoapify недоступен, endpoint {endpoint}") from None
            except (ValueError, UnicodeError):
                raise RuntimeError("Geoapify вернул некорректный JSON") from None

    def geocode(self, query, country="jp", lang="en", limit=5, response_format="json"):
        """Название и контекст → несколько кандидатов. Страну можно снять через None."""
        if response_format not in {"json", "geojson"}:
            raise ValueError("response_format должен быть json или geojson")
        params = {"text": query, "lang": lang, "limit": limit, "format": response_format}
        if country:
            params["filter"] = "countrycode:" + country
        return self._request("v1/geocode/search", params=params)

    def mcp_geocode(self, query, country="jp", lang="en", limit=5):
        """Тот же поиск через MCP tools/call; ошибки инструмента проверяются отдельно."""
        arguments = {"query": query, "lang": lang, "limit": limit}
        if country:
            arguments["country_codes"] = [country]
        record = self._request("v1/mcp", body={
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "geocode_address", "arguments": arguments},
        })
        payload = record["payload"]
        if "error" in payload or payload.get("result", {}).get("isError"):
            raise RuntimeError("Geoapify MCP вернул ошибку инструмента")
        return record

    def nearby(self, lat, lon, category="catering.cafe", radius=200, limit=4):
        """Рекомендации рядом с выбранной точкой, расстояния в метрах."""
        return self._request("v2/places", params={
            "categories": category, "filter": f"circle:{lon},{lat},{radius}",
            "bias": f"proximity:{lon},{lat}", "limit": limit, "lang": "en",
        })


def candidates(record):
    """Единый список свойств из JSON, GeoJSON либо MCP structuredContent."""
    payload = record["payload"]
    if "result" in payload:
        payload = payload["result"]["structuredContent"]
    if "features" in payload:
        return [feature["properties"] for feature in payload["features"]]
    return payload["results"]


def candidate_rows(record):
    """Короткие строки для таблицы; полная карточка остаётся в payload."""
    return [
        {
            "rank": index, "name": item.get("name") or item.get("address_line1"),
            "address": item.get("formatted"), "lat": item.get("lat"),
            "lon": item.get("lon"), "confidence": item.get("rank", {}).get("confidence"),
            "distance_m": item.get("distance"), "place_id": item.get("place_id"),
        }
        for index, item in enumerate(candidates(record), start=1)
    ]
