from __future__ import annotations

import json

import requests


def enhance_bilingual_analysis(
    *,
    enabled: bool,
    api_key: str | None,
    model: str,
    section_key: str,
    analysis_ko: str,
    analysis_en: str,
    steps_ko: list[str],
    steps_en: list[str],
) -> tuple[str, str] | None:
    if not enabled or not api_key:
        return None

    prompt = {
        "section": section_key,
        "analysis_ko": analysis_ko,
        "analysis_en": analysis_en,
        "steps_ko": steps_ko,
        "steps_en": steps_en,
        "constraints": [
            "Return JSON only",
            "Keep each language 1-2 sentences",
            "Do not add new facts beyond provided text",
        ],
    }

    response = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "temperature": 0,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You refine bilingual market commentary. Output strict JSON: "
                        '{"analysis_ko":"...","analysis_en":"..."}. '
                        "Do not invent facts."
                    ),
                },
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
            "max_tokens": 220,
        },
        timeout=25,
    )
    response.raise_for_status()

    payload = response.json()
    content = (
        payload.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "")
        .strip()
    )
    if not content:
        return None

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        return None

    ko = str(parsed.get("analysis_ko") or "").strip()
    en = str(parsed.get("analysis_en") or "").strip()
    if not ko or not en:
        return None

    return ko, en
