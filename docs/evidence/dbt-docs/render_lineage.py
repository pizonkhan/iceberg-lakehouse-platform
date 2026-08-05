"""
Render the lakehouse's real dbt lineage graph as a static PNG.

Reads transform/lakehouse/target/manifest.json (produced by `make docs`, which
runs `dbt docs generate`) and draws every real lakehouse model's dependency
edges (manifest depends_on.nodes / child_map), laid out by medallion layer.
This is a direct rendering of the project's actual dependency graph, not a
hand-drawn approximation: node set, edges, and even the fan-in/fan-out shape
come straight from the manifest dbt itself produced against the live build.

Run from the repo root with:
    make docs
    uv run --with matplotlib --with networkx python \
        docs/evidence/dbt-docs/render_lineage.py
"""

import json
from pathlib import Path

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.patches import FancyArrowPatch

# This file lives at docs/evidence/dbt-docs/render_lineage.py.
REPO_ROOT = Path(__file__).resolve().parents[3]
MANIFEST_PATH = REPO_ROOT / "transform" / "lakehouse" / "target" / "manifest.json"
OUT_PATH = Path(__file__).resolve().parent / "lineage-graph.png"

# Palette (categorical, fixed order, from the project's dataviz reference palette).
COLOR_SOURCE = "#898781"  # muted grey: bronze sources, not a dbt model
COLOR_STAGING = "#2a78d6"  # blue
COLOR_SILVER = "#eb6834"  # orange
COLOR_DIM = "#1baf7a"  # aqua
COLOR_FACT = "#4a3aa7"  # violet
COLOR_BRIDGE = "#e34948"  # red, called out separately since it's structurally distinct
INK = "#0b0b0b"
MUTED = "#898781"
SURFACE = "#fcfcfb"
EDGE_COLOR = "#c3c2b7"


def layer_of(node_id: str, node: dict) -> str:
    if node_id.startswith("source."):
        return "source"
    path = node["path"]
    if path.startswith("staging/"):
        return "staging"
    if path.startswith("intermediate/"):
        return "silver"
    if path == "marts/facts/bridge_title_genre.sql":
        return "bridge"
    if path.startswith("marts/facts/"):
        return "fact"
    if path.startswith("marts/dimensions/"):
        return "dimension"
    return "other"


def main() -> None:
    manifest = json.loads(MANIFEST_PATH.read_text())

    models = {
        k: v
        for k, v in manifest["nodes"].items()
        if v["resource_type"] == "model"
        and v["package_name"] == "lakehouse"
        # wap_demo_dim is a purpose-built, isolated table for the write-audit-publish
        # ops demo (ops/wap.py): zero refs in, zero refs out (see manifest depends_on),
        # not part of the medallion dependency graph this diagram is documenting.
        # Excluding it matches the "37 real models" count already used in
        # docs/evidence/dagster/00-summary-notes.txt.
        and k != "model.lakehouse.wap_demo_dim"
    }
    sources = {k: v for k, v in manifest["sources"].items()}

    graph = nx.DiGraph()
    layer_map: dict[str, str] = {}
    label_map: dict[str, str] = {}

    for node_id, node in models.items():
        layer = layer_of(node_id, node)
        graph.add_node(node_id)
        layer_map[node_id] = layer
        label_map[node_id] = node["name"]

    for node_id, node in sources.items():
        graph.add_node(node_id)
        layer_map[node_id] = "source"
        label_map[node_id] = node["name"]

    for node_id, node in models.items():
        for dep in node["depends_on"]["nodes"]:
            if dep in graph:
                graph.add_edge(dep, node_id)

    layer_order = ["source", "staging", "silver", "dimension", "fact", "bridge"]
    layer_x = {name: i for i, name in enumerate(layer_order)}
    layer_color = {
        "source": COLOR_SOURCE,
        "staging": COLOR_STAGING,
        "silver": COLOR_SILVER,
        "dimension": COLOR_DIM,
        "fact": COLOR_FACT,
        "bridge": COLOR_BRIDGE,
    }

    # Bucket nodes by layer, sort each bucket alphabetically by label for a
    # deterministic, reproducible layout (rerunning this script produces the
    # same picture given the same manifest).
    buckets: dict[str, list[str]] = {name: [] for name in layer_order}
    for node_id in graph.nodes:
        buckets[layer_map[node_id]].append(node_id)
    for name in buckets:
        buckets[name].sort(key=lambda n: label_map[n])

    pos: dict[str, tuple[float, float]] = {}
    for name in layer_order:
        nodes = buckets[name]
        count = len(nodes)
        for i, node_id in enumerate(nodes):
            # Center each layer's column vertically; taller columns (silver,
            # source) span more vertical space than short ones (bridge).
            y = (count - 1) / 2 - i
            pos[node_id] = (layer_x[name], y)

    fig_height = max(10, max(len(v) for v in buckets.values()) * 0.62)
    fig, ax = plt.subplots(figsize=(24, fig_height), dpi=200)
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)

    # Edges first, so nodes draw on top. Colored by the upstream node's layer
    # (color follows the entity the edge originates from), so a downstream
    # model's incoming edges visually separate by which layer fed them.
    for src, dst in graph.edges:
        x1, y1 = pos[src]
        x2, y2 = pos[dst]
        arrow = FancyArrowPatch(
            (x1 + 0.09, y1),
            (x2 - 0.09, y2),
            connectionstyle="arc3,rad=0.08",
            arrowstyle="-|>",
            mutation_scale=9,
            color=layer_color[layer_map[src]],
            linewidth=0.9,
            alpha=0.4,
            zorder=1,
        )
        ax.add_patch(arrow)

    # Nodes: rounded boxes with the model/source name.
    box_w, box_h = 0.86, 0.42
    for node_id in graph.nodes:
        x, y = pos[node_id]
        layer = layer_map[node_id]
        color = layer_color[layer]
        is_source = layer == "source"
        box = mpatches.FancyBboxPatch(
            (x - box_w / 2, y - box_h / 2),
            box_w,
            box_h,
            boxstyle="round,pad=0.02,rounding_size=0.06",
            linewidth=1.1,
            edgecolor=color,
            facecolor="white" if not is_source else "#f1f0ec",
            zorder=2,
        )
        ax.add_patch(box)
        ax.text(
            x,
            y,
            label_map[node_id],
            ha="center",
            va="center",
            fontsize=6.3,
            color=INK,
            zorder=3,
            family="sans-serif",
        )

    # Column headers.
    header_labels = {
        "source": "bronze sources (9)",
        "staging": "staging (9)",
        "silver": "silver / intermediate (13)",
        "dimension": "dimensions (9)",
        "fact": "facts (5)",
        "bridge": "bridge (1)",
    }
    top_y = max(pos[n][1] for n in graph.nodes) + 0.9
    for name in layer_order:
        ax.text(
            layer_x[name],
            top_y,
            header_labels[name],
            ha="center",
            va="bottom",
            fontsize=11,
            fontweight="bold",
            color=layer_color[name] if name != "source" else MUTED,
        )

    ax.set_xlim(-0.7, len(layer_order) - 0.3)
    bottom_y = min(pos[n][1] for n in graph.nodes) - 0.8
    ax.set_ylim(bottom_y, top_y + 0.6)
    ax.axis("off")
    ax.set_title(
        "iceberg-lakehouse-platform: dbt lineage (transform/lakehouse), "
        f"{len(models)} real models, {graph.number_of_edges()} dependency edges\n"
        "generated from transform/lakehouse/target/manifest.json (dbt docs generate, trino target)",
        fontsize=12,
        color=INK,
        pad=18,
    )

    fig.tight_layout()
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_PATH, facecolor=SURFACE, bbox_inches="tight")
    print(f"wrote {OUT_PATH} ({OUT_PATH.stat().st_size} bytes)")
    print(f"nodes: {graph.number_of_nodes()}, edges: {graph.number_of_edges()}")


if __name__ == "__main__":
    main()
