import time

import pandas as pd
import streamlit as st

from llm_summary import get_llm_summary
from streaming_job import build_spark, start_streaming_queries

st.set_page_config(
    page_title="News Pulse",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
        .main { background: #0d1117; color: #e6edf3; }
        h1 { color: #58a6ff; font-family: Georgia, serif; }
        .llm-box {
            background: #161b22;
            border-left: 4px solid #58a6ff;
            border-radius: 4px;
            padding: 1rem 1.2rem;
            font-size: 1.05rem;
            line-height: 1.7;
            color: #cdd9e5;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_spark_and_start_streams():
    spark = build_spark("NewsPulse")
    start_streaming_queries(spark)
    return spark


spark = get_spark_and_start_streams()


def query_table(table: str) -> pd.DataFrame:
    try:
        return spark.sql(f"SELECT * FROM {table}").toPandas()
    except Exception:
        return pd.DataFrame()


st.title("News Pulse — Live Dashboard")
st.caption("Real-time news aggregation powered by Spark Structured Streaming + LLM")

src_df = query_table("by_source")
win_df = query_table("by_window")
kw_df = query_table("top_words")

total_articles = int(src_df["count"].sum()) if not src_df.empty else 0
num_sources = len(src_df) if not src_df.empty else 0
total_windows = len(win_df) if not win_df.empty else 0
num_keywords = len(kw_df) if not kw_df.empty else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Headlines", total_articles)
col2.metric("Active Sources", num_sources)
col3.metric("Time Windows", total_windows)
col4.metric("Unique Keywords", num_keywords)

st.divider()

left, right = st.columns(2)

with left:
    st.subheader("Headlines by Source")
    if not src_df.empty:
        src_sorted = src_df.sort_values("count", ascending=False)
        st.bar_chart(src_sorted.set_index("source")["count"])
    else:
        st.info("Waiting for data. Run: python ingester.py")

with right:
    st.subheader("Headline Volume by Hour")
    if not win_df.empty:
        win_sorted = win_df.sort_values("window_start")
        win_sorted["label"] = win_sorted["window_start"].astype(str).str[11:16]
        st.line_chart(win_sorted.set_index("label")["count"])
    else:
        st.info("No windowed data yet.")

st.divider()

kw_col, llm_col = st.columns([1, 2])

with kw_col:
    st.subheader("Top Keywords")
    if not kw_df.empty:
        top10 = kw_df.sort_values("count", ascending=False).head(10)
        st.dataframe(
            top10[["word", "count"]].rename(
                columns={"word": "Keyword", "count": "Mentions"}
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No keywords yet.")

with llm_col:
    st.subheader("LLM Thematic Summary")
    if not kw_df.empty:
        top15_keywords = (
            kw_df.sort_values("count", ascending=False)
            .head(15)["word"]
            .tolist()
        )
        with st.spinner("Generating summary..."):
            summary = get_llm_summary(top15_keywords)
    else:
        summary = "Waiting for keyword data to arrive."

    st.markdown(f'<div class="llm-box">{summary}</div>', unsafe_allow_html=True)

st.divider()

refresh_every = 5
st.caption(f"Auto-refreshes every {refresh_every}s | Last updated: {time.strftime('%H:%M:%S')}")
time.sleep(refresh_every)
st.rerun()
