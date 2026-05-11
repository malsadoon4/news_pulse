"""
server.py — Flask web server

Serves dashboard.html and /data endpoint.
Run: python server.py
Open: http://localhost:5000
"""

import json
import os

import requests
from flask import Flask, jsonify, send_file

app = Flask(__name__)


def fallback_summary(keywords: list[str]) -> str:
    if not keywords:
        return "Waiting for live keyword data. Start ingester.py and streaming_job.py first."
    return "Top themes: " + ", ".join(keywords[:10]) + ". Set an API key for a full AI summary."


def get_llm_summary(keywords: list[str]) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return fallback_summary(keywords)

    try:
        kw_str = ", ".join(keywords[:15])
        prompt = f"""Based ONLY on these news keywords, write ONE paragraph, max 80 words.
Mention at least 3 named storylines. Use journalistic present-tense style. No bullets.
Keywords: {kw_str}"""

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
        words = text.split()
        return " ".join(words[:80]) + ("..." if len(words) > 80 else "")
    except Exception as exc:
        print(f"[llm] failed: {exc}")
        return fallback_summary(keywords)


@app.route("/")
def index():
    return send_file("dashboard.html")


@app.route("/data")
def data():
    try:
        with open("results.json", encoding="utf-8") as f:
            results = json.load(f)
        keywords = [row["word"] for row in results.get("top_words", [])]
        results["llm_summary"] = get_llm_summary(keywords)
        return jsonify(results)
    except FileNotFoundError:
        return jsonify({
            "by_source": [],
            "by_window": [],
            "top_words": [],
            "updated_at": "waiting...",
            "llm_summary": "Waiting for data. Run ingester.py and streaming_job.py.",
        })


if __name__ == "__main__":
    print("[server] Starting at http://localhost:5000")
    app.run(debug=False, port=5000)
