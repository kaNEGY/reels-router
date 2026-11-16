import httpx
import pytest

from reels_router.providers import OpenAICompatible, ProviderError


@pytest.mark.anyio
async def test_provider_normalizes_asr_text_and_image(tmp_path):
    def handler(request):
        assert request.headers["authorization"] == "Bearer test-key"
        if request.url.path.endswith("/audio/transcriptions"):
            assert b"audio-data" in request.content
            return httpx.Response(200, json={"text": "Kyoto"})
        assert b"test-model" in request.content
        return httpx.Response(200, json={"choices": [{"message": {"content": "Kyoto"}}], "usage": {"total_tokens": 8}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = OpenAICompatible(client, "https://test.example/v1", "test-key", "test-model", "example")
        audio, image = tmp_path / "audio.flac", tmp_path / "frame.jpg"
        audio.write_bytes(b"audio-data")
        image.write_bytes(b"image-data")
        assert (await provider.transcribe(audio)).text == "Kyoto"
        assert (await provider.generate("test")).usage == {"total_tokens": 8}
        assert (await provider.describe("test", image)).provider == "example"


@pytest.mark.anyio
async def test_provider_does_not_expose_error_body():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(401, text="secret"))) as client:
        provider = OpenAICompatible(client, "https://test.example/v1", "test-key", "model", "example")
        with pytest.raises(ProviderError, match="HTTP 401") as error:
            await provider.generate("test")
        assert "secret" not in str(error.value) and "test-key" not in str(error.value)
