import os
import textwrap

import requests


def _build_prompt(keywords: list[str]) -> str:
    keyword_text = ", ".join(keywords[:15])

    return textwrap.dedent(
        f"""
        You are a media-monitoring analyst. Based ONLY on the following keywords
        extracted from today's news headlines, write a single thematic paragraph.

        Rules:
        1. Maximum 80 words.
        2. Mention at least three distinct named storylines or topics by name.
        3. Write in present tense, journalistic style.
        4. No bullet points. No headers. One paragraph only.

        Keywords: {keyword_text}
        """
    ).strip()


def _trim_to_80_words(text: str) -> str:
    words = text.split()
    if len(words) <= 80:
        return text
    return " ".join(words[:80]) + "..."


def _fallback_summary(keywords: list[str]) -> str:
    top = keywords[:10]
    if not top:
        return "No keyword data is available yet. Start the ingester and wait for Spark to process headlines."

    return (
        f"Top news themes right now: {', '.join(top)}. "
        "Full LLM summary unavailable, so this fallback summary is based only on keyword frequency."
    )


def get_llm_summary(keywords: list[str]) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if api_key:
        prompt = _build_prompt(keywords)
        try:
            response = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-3-5-haiku-latest",
                    "max_tokens": 200,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=15,
            )
            response.raise_for_status()
            text = response.json()["content"][0]["text"].strip()
            return _trim_to_80_words(text)
        except Exception as exc:
            print(f"[llm_summary] Anthropic failed: {exc}")

    return _try_openai_or_fallback(keywords)


def _try_openai_or_fallback(keywords: list[str]) -> str:
    api_key = os.environ.get("OPENAI_API_KEY", "")

    if not api_key:
        return _fallback_summary(keywords)

    prompt = _build_prompt(keywords)

    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "gpt-4o-mini",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15,
        )
        response.raise_for_status()
        text = response.json()["choices"][0]["message"]["content"].strip()
        return _trim_to_80_words(text)
    except Exception as exc:
        print(f"[llm_summary] OpenAI failed: {exc}")
        return _fallback_summary(keywords)
