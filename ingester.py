"""
ingester.py — T1: RSS Feed Ingester
Pulls 4+ RSS feeds every 60 seconds and writes JSON Lines batches to data/incoming/.
Each record contains: source, title, url, ts
"""

import json
import os
import time
from datetime import datetime, timezone

import feedparser

INCOMING = "data/incoming"
os.makedirs(INCOMING, exist_ok=True)

FEEDS = {
    "BBC": "http://feeds.bbci.co.uk/news/rss.xml",
    "Reuters": "https://feeds.reuters.com/reuters/topNews",
    "CNN": "http://rss.cnn.com/rss/edition.rss",
    "Al_Jazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "NPR": "https://feeds.npr.org/1001/rss.xml",
    "Guardian": "https://www.theguardian.com/world/rss",
}


def parse_ts(entry) -> str:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def pull_once(tick: int) -> int:
    rows = []

    for source, url in FEEDS.items():
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:20]:
                title = getattr(entry, "title", "").strip()
                link = getattr(entry, "link", "").strip()
                if not title:
                    continue
                rows.append({"source": source, "title": title, "url": link, "ts": parse_ts(entry)})
        except Exception as exc:
            print(f"[ingester] WARNING: feed '{source}' failed — {exc}")

    if rows:
        path = os.path.join(INCOMING, f"batch_{tick:05d}.json")
        with open(path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"[ingester] tick={tick} wrote {len(rows)} records -> {path}")
    else:
        print(f"[ingester] tick={tick} no records")

    return len(rows)


if __name__ == "__main__":
    tick = 0
    print(f"[ingester] Starting — writing to {INCOMING}")
    while True:
        pull_once(tick)
        tick += 1
        time.sleep(60)
