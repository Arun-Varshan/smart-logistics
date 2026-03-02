from typing import Dict, List, Tuple

import networkx as nx

EMISSION_FACTOR_G_PER_KM = 120.0


def build_graph(zones: Dict[str, Dict[str, float]]) -> nx.Graph:
    G = nx.Graph()
    for name, meta in zones.items():
        G.add_node(name, x=meta.get("x", 0), y=meta.get("y", 0))
    # simple fully connected graph with Euclidean weights
    names = list(zones.keys())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = names[i], names[j]
            ax, ay = zones[a].get("x", 0), zones[a].get("y", 0)
            bx, by = zones[b].get("x", 0), zones[b].get("y", 0)
            dist = ((ax - bx) ** 2 + (ay - by) ** 2) ** 0.5
            G.add_edge(a, b, weight=dist)
    return G


def shortest_path_and_co2(zones: Dict[str, Dict[str, float]], origin: str, targets: List[str]) -> Tuple[List[str], float]:
    if origin not in zones or not targets:
        return [], 0.0
    G = build_graph(zones)
    route: List[str] = [origin]
    total_dist = 0.0
    current = origin
    for t in targets:
        if t == current:
            continue
        try:
            path = nx.shortest_path(G, source=current, target=t, weight="weight")
            route.extend(path[1:])
            # sum segment distances
            for k in range(len(path) - 1):
                total_dist += G[path[k]][path[k + 1]]["weight"]
            current = t
        except Exception:
            continue
    co2_g = total_dist * EMISSION_FACTOR_G_PER_KM
    return route, co2_g
