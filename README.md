# News Pulse — HTML Dashboard Version

This version uses:

- `ingester.py` for RSS ingestion
- `streaming_job.py` for Spark Structured Streaming
- `server.py` for Flask API
- `dashboard.html` for the browser dashboard

## Setup

```bash
pip install pyspark==3.5.0 feedparser pandas requests flask
```

Check Java:

```bash
java -version
```

The challenge requires Java 11 or Java 17.

## Run

Use three terminals.

### Terminal 1

```bash
python ingester.py
```

### Terminal 2

```bash
python streaming_job.py
```

### Terminal 3

```bash
python server.py
```

Then open:

```text
http://localhost:5000
```

## What Happens

1. `ingester.py` writes RSS batches into `data/incoming/`.
2. `streaming_job.py` uses `spark.readStream` and `writeStream`.
3. Spark creates `by_source`, `by_window`, and `top_words`.
4. `streaming_job.py` dumps those results into `results.json`.
5. `server.py` reads `results.json` and sends it to the HTML dashboard through `/data`.
6. `dashboard.html` refreshes every 5 seconds.

## Reflection

The first component to break under 1000× input growth is the `top_words` streaming query because it uses a complete aggregation over all words. Spark must maintain a large state and rewrite the full result. I would fix it using windowed aggregation with `withWatermark()` to bound state, increase shuffle partitions, and consider approximate counting for very large streams.
