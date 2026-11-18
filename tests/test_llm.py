import json

from reels_router.llm_worker import extract_places


def test_extract_places_keeps_only_checkable_evidence():
    sources = [{"id": "a", "kind": "transcript", "text": "We are visiting Kyoto station"}]
    response = {"places": [
        {"name": "Kyoto", "original_name": "Kyoto", "evidence": [{"source_id": "a", "quote": "Kyoto station"}]},
        {"name": "Tokyo", "original_name": "Tokyo", "evidence": [{"source_id": "a", "quote": "Tokyo tower"}]},
        {"name": "Osaka", "original_name": "Osaka", "evidence": [{"source_id": "missing", "quote": "Kyoto station"}]},
    ]}
    places = extract_places(json.dumps(response), sources)
    assert len(places) == 1 and places[0]["name"] == "Kyoto"
    assert places[0]["evidence"][0]["kind"] == "transcript"


def test_normalized_name_retains_original_quote():
    sources = [{"id": "frame", "kind": "vlm_description", "text": "The sign says 京都駅", "time_s": 4.2}]
    response = {"places": [{"name": "Kyoto Station", "original_name": "京都駅",
                             "evidence": [{"source_id": "frame", "quote": "京都駅"}]}]}
    place = extract_places(json.dumps(response, ensure_ascii=False), sources)[0]
    assert place["original_name"] == "京都駅" and place["evidence"][0]["time_s"] == 4.2
