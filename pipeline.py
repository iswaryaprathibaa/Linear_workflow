"""3-step ETL pipeline (extract -> transform -> load) built with LangGraph."""

import csv
import os
from io import StringIO
from typing import Any, Dict, List, TypedDict

from langgraph.graph import StateGraph, START, END

INPUT_PATH = os.path.join("data", "input.csv")
OUTPUT_PATH = os.path.join("data", "output.csv")


class PipelineState(TypedDict):
    source_path: str
    source_text: str
    output_path: str
    raw_records: List[Dict[str, Any]]
    clean_records: List[Dict[str, Any]]
    rejected: List[Dict[str, Any]]
    output_text: str
    log: List[str]


def extract(state: PipelineState) -> PipelineState:
    if state.get("source_text"):
        raw_records = list(csv.DictReader(state["source_text"].splitlines()))
        source_desc = "uploaded file"
    else:
        with open(state["source_path"], newline="", encoding="utf-8") as f:
            raw_records = list(csv.DictReader(f))
        source_desc = state["source_path"]

    state["raw_records"] = raw_records
    state["log"].append(f"Extracted {len(raw_records)} raw records from {source_desc}")
    return state


def transform(state: PipelineState) -> PipelineState:
    clean_records: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    seen_ids = set()

    for row in state["raw_records"]:
        record_id = (row.get("id") or "").strip()
        name = (row.get("name") or "").strip()
        email = (row.get("email") or "").strip().lower()
        city = (row.get("city") or "").strip().title()
        age_raw = (row.get("age") or "").strip()

        if not name or not email or "@" not in email:
            rejected.append({**row, "reason": "missing name or invalid email"})
            continue

        try:
            age = int(age_raw)
        except ValueError:
            rejected.append({**row, "reason": "age is not a valid integer"})
            continue

        if age <= 0 or age > 120:
            rejected.append({**row, "reason": "age out of valid range"})
            continue

        if record_id in seen_ids:
            rejected.append({**row, "reason": "duplicate id"})
            continue
        seen_ids.add(record_id)

        clean_records.append({
            "id": record_id,
            "name": name,
            "email": email,
            "age": age,
            "city": city,
        })

    state["clean_records"] = clean_records
    state["rejected"] = rejected
    state["log"].append(
        f"Transformed data: {len(clean_records)} clean records, {len(rejected)} rejected"
    )
    return state


def load(state: PipelineState) -> PipelineState:
    fieldnames = ["id", "name", "email", "age", "city"]

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(state["clean_records"])
    state["output_text"] = buffer.getvalue()

    if state.get("output_path"):
        with open(state["output_path"], "w", newline="", encoding="utf-8") as f:
            f.write(state["output_text"])

    destination = state.get("output_path") or "memory"
    state["log"].append(f"Loaded {len(state['clean_records'])} records into {destination}")
    return state


def build_graph():
    graph = StateGraph(PipelineState)
    graph.add_node("extract", extract)
    graph.add_node("transform", transform)
    graph.add_node("load", load)

    graph.add_edge(START, "extract")
    graph.add_edge("extract", "transform")
    graph.add_edge("transform", "load")
    graph.add_edge("load", END)

    return graph.compile()


def main():
    app = build_graph()

    initial_state: PipelineState = {
        "source_path": INPUT_PATH,
        "source_text": "",
        "output_path": OUTPUT_PATH,
        "raw_records": [],
        "clean_records": [],
        "rejected": [],
        "output_text": "",
        "log": [],
    }

    final_state = app.invoke(initial_state)

    print("\n--- Pipeline Log ---")
    for line in final_state["log"]:
        print(f"- {line}")

    if final_state["rejected"]:
        print("\n--- Rejected Records ---")
        for row in final_state["rejected"]:
            print(f"  id={row.get('id')} reason={row['reason']}")


if __name__ == "__main__":
    main()
