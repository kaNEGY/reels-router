import httpx
import pytest

from reels_router.geo_worker import Geoapify


@pytest.mark.anyio
async def test_geocoder_returns_candidates_without_default_country():
    def handler(request):
        assert request.url.params["text"] == "Tokyo Station"
        assert "filter" not in request.url.params
        return httpx.Response(200, json={"results": [
            {"formatted": "Tokyo Station, Japan", "lat": 35.68, "lon": 139.76,
             "place_id": "one", "rank": {"confidence": 0.9}},
            {"formatted": "Invalid", "lat": 120, "lon": 1},
        ]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        candidates = await Geoapify(client, "test-key").search("Tokyo Station")
    assert len(candidates) == 1
    assert candidates[0]["label"] == "Tokyo Station, Japan"
    assert candidates[0]["confidence"] == 0.9
