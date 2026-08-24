"""Streamlit web UI for the LangGraph extract -> transform -> load pipeline."""

import pandas as pd
import streamlit as st

from pipeline import build_graph

st.set_page_config(page_title="Data Cleaning Pipeline", page_icon="🧹", layout="wide")

st.title("🧹 Data Cleaning Pipeline")
st.caption("A 3-step LangGraph workflow: Extract → Transform → Load")

with st.sidebar:
    st.header("1. Provide data")
    uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
    use_sample = st.checkbox("Use bundled sample data (data/input.csv)", value=uploaded_file is None)
    run_clicked = st.button("Run pipeline", type="primary", use_container_width=True)

if run_clicked:
    if uploaded_file is not None:
        source_text = uploaded_file.getvalue().decode("utf-8")
    elif use_sample:
        with open("data/input.csv", encoding="utf-8") as f:
            source_text = f.read()
    else:
        st.warning("Upload a CSV file or check 'use bundled sample data'.")
        st.stop()

    app = build_graph()
    initial_state = {
        "source_path": "",
        "source_text": source_text,
        "output_path": "",
        "raw_records": [],
        "clean_records": [],
        "rejected": [],
        "output_text": "",
        "log": [],
    }
    final_state = app.invoke(initial_state)

    st.subheader("Pipeline Log")
    for line in final_state["log"]:
        st.write(f"- {line}")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader(f"Clean Records ({len(final_state['clean_records'])})")
        if final_state["clean_records"]:
            st.dataframe(pd.DataFrame(final_state["clean_records"]), use_container_width=True)
            st.download_button(
                "Download cleaned CSV",
                data=final_state["output_text"],
                file_name="output.csv",
                mime="text/csv",
                use_container_width=True,
            )
        else:
            st.info("No records passed cleaning.")

    with col2:
        st.subheader(f"Rejected Records ({len(final_state['rejected'])})")
        if final_state["rejected"]:
            st.dataframe(pd.DataFrame(final_state["rejected"]), use_container_width=True)
        else:
            st.info("No records were rejected.")
else:
    st.info("Upload a CSV (or use the sample data) in the sidebar, then click **Run pipeline**.")
