from __future__ import annotations

import json
import os
import heapq
import math
import re
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse


# ----------------------------
# In-memory "database" (Dict)
# ----------------------------


def build_default_state() -> Dict[str, Any]:
    """
    Default approximate network:
    - BTS Green Line: Sukhumvit + Silom (single fare system approximation)
    - MRT: Blue Line + Purple Line
    - Transfer edges (walkways) are modeled as zero-distance edges with `line=None`.
    """

    stations: Dict[str, Dict[str, Any]] = {}
    lines: Dict[str, Dict[str, Any]] = {
        "BTS_SUK": {"name": "BTS Sukhumvit Line (Light Green)", "system": "BTS_GREEN"},
        "BTS_SIL": {"name": "BTS Silom Line (Dark Green)", "system": "BTS_GREEN"},
        "MRT_BLUE": {"name": "MRT Blue Line", "system": "MRT_BLUE"},
        "MRT_PURPLE": {"name": "MRT Purple Line", "system": "MRT_PURPLE"},
    }
    edges: Dict[str, Dict[str, Any]] = {}

    edge_seq = 0

    def add_station(station_id: str, name: str, **extra: Any) -> None:
        # Merge if already exists (e.g., shared station like Siam/Tao Poon).
        cur = stations.get(station_id, {})
        merged = {**cur, **extra, "name": name}
        stations[station_id] = merged

    def add_edge(edge_id: str, a: str, b: str, line_id: Optional[str], km: float) -> None:
        edges[edge_id] = {"from": a, "to": b, "line": line_id, "km": float(km)}

    def next_edge_id(prefix: str) -> str:
        nonlocal edge_seq
        edge_seq += 1
        return f"{prefix}{edge_seq:05d}"

    # --- BTS Green (Sukhumvit + Silom) ---
    # BTS Sukhumvit station order from Khu Khot -> Kheha (excluding planned N6 to match operational station count).
    suk_station_order_codes = [
        "N24",
        "N23",
        "N22",
        "N21",
        "N20",
        "N19",
        "N18",
        "N17",
        "N16",
        "N15",
        "N14",
        "N13",
        "N12",
        "N11",
        "N10",
        "N9",
        "N8",
        "N7",
        "N5",
        "N4",
        "N3",
        "N2",
        "N1",
        "CEN",
        "E1",
        "E2",
        "E3",
        "E4",
        "E5",
        "E6",
        "E7",
        "E8",
        "E9",
        "E10",
        "E11",
        "E12",
        "E13",
        "E14",
        "E15",
        "E16",
        "E17",
        "E18",
        "E19",
        "E20",
        "E21",
        "E22",
        "E23",
    ]
    suk_names = {
        "N24": "Khu Khot",
        "N23": "Yaek Kor Por Aor",
        "N22": "Royal Thai Air Force Museum",
        "N21": "Bhumibol Adulyadej Hospital",
        "N20": "Saphan Mai",
        "N19": "Sai Yud",
        "N18": "Phahon Yothin 59",
        "N17": "Wat Phra Sri Mahathat",
        "N16": "11th Infantry Regiment",
        "N15": "Bang Bua",
        "N14": "Royal Forest Department",
        "N13": "Kasetsart University",
        "N12": "Sena Nikhom",
        "N11": "Ratchayothin",
        "N10": "Phahon Yothin 24",
        "N9": "Ha Yaek Lat Phrao",
        "N8": "Mo Chit",
        "N7": "Saphan Khwai",
        "N5": "Ari",
        "N4": "Sanam Pao",
        "N3": "Victory Monument",
        "N2": "Phaya Thai",
        "N1": "Ratchathewi",
        "CEN": "Siam",
        "E1": "Chit Lom",
        "E2": "Phloen Chit",
        "E3": "Nana",
        "E4": "Asok",
        "E5": "Phrom Phong",
        "E6": "Thong Lo",
        "E7": "Ekkamai",
        "E8": "Phra Khanong",
        "E9": "On Nut",
        "E10": "Bang Chak",
        "E11": "Punnawithi",
        "E12": "Udom Suk",
        "E13": "Bang Na",
        "E14": "Bearing",
        "E15": "Samrong",
        "E16": "Pu Chao",
        "E17": "Chang Erawan",
        "E18": "Royal Thai Naval Academy",
        "E19": "Pak Nam",
        "E20": "Srinagarindra",
        "E21": "Phraek Sa",
        "E22": "Sai Luat",
        "E23": "Kheha",
    }
    bts_suk_length_km = 54.25
    bts_suk_avg_km = bts_suk_length_km / (len(suk_station_order_codes) - 1)

    def bts_sid(code: str) -> str:
        if code == "CEN":
            return "BTS_CEN"
        return f"BTS_{code}"

    for code in suk_station_order_codes:
        add_station(bts_sid(code), suk_names[code])

    prev = None
    for code in suk_station_order_codes:
        sid = bts_sid(code)
        if prev is not None:
            add_edge(next_edge_id("BSU_"), prev, sid, "BTS_SUK", bts_suk_avg_km)
        prev = sid

    # Silom order from National Stadium -> Bang Wa.
    sil_station_order_codes = ["W1", "CEN", "S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "S11", "S12"]
    sil_names = {
        "W1": "National Stadium",
        "CEN": "Siam",
        "S1": "Ratchadamri",
        "S2": "Sala Daeng",
        "S3": "Chong Nonsi",
        "S4": "Saint Louis",
        "S5": "Surasak",
        "S6": "Saphan Taksin",
        "S7": "Krung Thon Buri",
        "S8": "Wongwian Yai",
        "S9": "Pho Nimit",
        "S10": "Talat Phlu",
        "S11": "Wutthakat",
        "S12": "Bang Wa",
    }
    bts_sil_length_km = 13.09
    bts_sil_avg_km = bts_sil_length_km / (len(sil_station_order_codes) - 1)

    def sil_sid(code: str) -> str:
        if code == "CEN":
            return "BTS_CEN"
        return f"BTS_{code}"

    for code in sil_station_order_codes:
        add_station(sil_sid(code), sil_names[code])

    prev = None
    for code in sil_station_order_codes:
        sid = sil_sid(code)
        if prev is not None:
            add_edge(next_edge_id("BSI_"), prev, sid, "BTS_SIL", bts_sil_avg_km)
        prev = sid

    # --- MRT Blue (Tha Phra -> Lak Song, operational) ---
    blue_order_codes_names = [
        ("BL01", "Tha Phra"),
        ("BL02", "Charan 13"),
        ("BL03", "Fai Chai"),
        ("BL04", "Bang Khun Non"),
        ("BL05", "Bang Yi Khan"),
        ("BL06", "Sirindhorn"),
        ("BL07", "Bang Phlat"),
        ("BL08", "Bang O"),
        ("BL09", "Bang Pho"),
        ("BL10", "Tao Poon"),
        ("BL11", "Bang Sue"),
        ("BL12", "Kamphaeng Phet"),
        ("BL13", "Chatuchak Park"),
        ("BL14", "Phahon Yothin"),
        ("BL15", "Lat Phrao"),
        ("BL16", "Ratchadaphisek"),
        ("BL17", "Sutthisan"),
        ("BL18", "Huai Khwang"),
        ("BL19", "Thailand Cultural Centre"),
        ("BL20", "Phra Ram 9"),
        ("BL21", "Phetchaburi"),
        ("BL22", "Sukhumvit"),
        ("BL23", "Queen Sirikit National Convention Centre"),
        ("BL24", "Khlong Toei"),
        ("BL25", "Lumphini"),
        ("BL26", "Si Lom"),
        ("BL27", "Sam Yan"),
        ("BL28", "Hua Lamphong"),
        ("BL29", "Wat Mangkon"),
        ("BL30", "Sam Yot"),
        ("BL31", "Sanam Chai"),
        ("BL32", "Itsaraphap"),
        ("BL33", "Bang Phai"),
        ("BL34", "Bang Wa"),
        ("BL35", "Phetkasem 48"),
        ("BL36", "Phasi Charoen"),
        ("BL37", "Bang Khae"),
        ("BL38", "Lak Song"),
    ]
    mrt_blue_length_km = 48.0
    mrt_blue_avg_km = mrt_blue_length_km / (len(blue_order_codes_names) - 1)

    def mrtl_sid_blue(code: str, name: str) -> str:
        if code == "BL10":  # Tao Poon shared with Purple Line
            return "MRT_TAOPON"
        return f"MRTB_{code}"

    prev = None
    for code, name in blue_order_codes_names:
        sid = mrtl_sid_blue(code, name)
        add_station(sid, name)
        if prev is not None:
            add_edge(next_edge_id("MBL_"), prev, sid, "MRT_BLUE", mrt_blue_avg_km)
        prev = sid

    # --- MRT Purple (Khlong Bang Phai -> Tao Poon, operational) ---
    purple_order_codes_names = [
        ("PP01", "Khlong Bang Phai"),
        ("PP02", "Talad Bang Yai"),
        ("PP03", "Sam Yaek Bang Yai"),
        ("PP04", "Bang Phlu"),
        ("PP05", "Bang Rak Yai"),
        ("PP06", "Bang Rak Noi Tha It"),
        ("PP07", "Sai Ma"),
        ("PP08", "Phra Nang Klao Bridge"),
        ("PP09", "Yaek Nonthaburi 1"),
        ("PP10", "Bang Krasor"),
        ("PP11", "Nonthaburi Civic Center"),
        ("PP12", "Ministry of Public Health"),
        ("PP13", "Yaek Tiwanon"),
        ("PP14", "Wong Sawang"),
        ("PP15", "Bang Son"),
        ("PP16", "Tao Poon"),
    ]
    mrt_purple_length_km = 23.63
    mrt_purple_avg_km = mrt_purple_length_km / (len(purple_order_codes_names) - 1)

    def mrtl_sid_purple(code: str, name: str) -> str:
        if code == "PP16":  # Tao Poon shared with Blue Line
            return "MRT_TAOPON"
        return f"MRTP_{code}"

    prev = None
    for code, name in purple_order_codes_names:
        sid = mrtl_sid_purple(code, name)
        add_station(sid, name)
        if prev is not None:
            add_edge(next_edge_id("MPR_"), prev, sid, "MRT_PURPLE", mrt_purple_avg_km)
        prev = sid

    # --- Transfers (walkways / paid-area connection approximation) ---
    # BTS -> MRT transfers (km=0, line=None so they won't affect "line_changes")
    transfers: List[Tuple[str, str, str]] = [
        (bts_sid("E4"), "MRTB_BL22", "XFER_ASOK_TO_MRT_SUKHUMVIT"),
        (bts_sid("S2"), "MRTB_BL26", "XFER_SALA_DAENG_TO_MRT_SI_LOM"),
        (bts_sid("S12"), "MRTB_BL34", "XFER_BANG_WA_TO_MRT_BANG_WA"),
        (bts_sid("N8"), "MRTB_BL13", "XFER_MOCHIT_TO_CHATUCHAK_PARK"),
        (bts_sid("N9"), "MRTB_BL14", "XFER_LATPHRAO_TO_PHAYON_YOTHIN"),
    ]

    for a_sid, b_sid, eid in transfers:
        # Use km=0: transfer walking is not counted as ride distance in this approximation.
        add_edge(eid, a_sid, b_sid, None, 0.0)

    return {"stations": stations, "lines": lines, "edges": edges, "_rev": 0}


STATE: Dict[str, Any] = build_default_state()


def bump_rev() -> None:
    STATE["_rev"] = int(STATE.get("_rev", 0)) + 1


# ----------------------------
# Graph + caching (adjacency)
# ----------------------------

_GRAPH_CACHE: Dict[str, Any] = {"rev": None, "adj": None}


def build_adjacency() -> Dict[str, List[Tuple[str, str]]]:
    """
    adjacency[u] = list of (v, edge_id)
    """
    adjacency: Dict[str, List[Tuple[str, str]]] = {}
    edges = STATE["edges"]
    stations = STATE["stations"]

    for sid in stations.keys():
        adjacency.setdefault(sid, [])

    for edge_id, e in edges.items():
        a = e.get("from")
        b = e.get("to")
        if a not in stations or b not in stations:
            # Skip invalid edges (CRUD validators should prevent this).
            continue
        adjacency.setdefault(a, []).append((b, edge_id))
        adjacency.setdefault(b, []).append((a, edge_id))

    return adjacency


def get_adjacency_cached() -> Dict[str, List[Tuple[str, str]]]:
    rev = STATE.get("_rev", 0)
    if _GRAPH_CACHE.get("rev") != rev or _GRAPH_CACHE.get("adj") is None:
        _GRAPH_CACHE["adj"] = build_adjacency()
        _GRAPH_CACHE["rev"] = rev
    return _GRAPH_CACHE["adj"]


# ----------------------------
# Search algorithms
# ----------------------------


def reconstruct_path(
    start: str,
    goal: str,
    parent: Dict[str, Optional[str]],
    parent_edge: Dict[str, Optional[str]],
) -> Tuple[List[str], List[str]]:
    """
    parent[v] = prev node
    parent_edge[v] = edge used from parent[v] -> v
    """
    path: List[str] = []
    edge_path: List[str] = []

    cur: Optional[str] = goal
    while cur is not None:
        path.append(cur)
        if cur == start:
            break
        edge_id = parent_edge.get(cur)
        prev = parent.get(cur)
        if prev is None:
            break
        edge_path.append(edge_id if edge_id is not None else "")
        cur = prev

    path.reverse()
    edge_path = list(reversed([eid for eid in edge_path if eid != ""]))
    return path, edge_path


def bfs_shortest_path(start: str, goal: str) -> Dict[str, Any]:
    """
    BFS on an unweighted graph.
    Guarantees shortest path in number of edges.
    """
    adjacency = get_adjacency_cached()
    if start == goal:
        return {"path": [start], "edges": [], "visited": 0, "steps": 0}

    visited = set([start])
    parent: Dict[str, Optional[str]] = {start: None}
    parent_edge: Dict[str, Optional[str]] = {start: None}

    q = deque([start])
    steps = 0
    visited_count = 0

    while q:
        u = q.popleft()
        visited_count += 1
        for v, edge_id in adjacency.get(u, []):
            if v in visited:
                continue
            visited.add(v)
            parent[v] = u
            parent_edge[v] = edge_id
            if v == goal:
                path, edge_path = reconstruct_path(start, goal, parent, parent_edge)
                steps = len(edge_path)
                return {
                    "path": path,
                    "edges": edge_path,
                    "visited": visited_count,
                    "steps": steps,
                }
            q.append(v)

    return {"path": [], "edges": [], "visited": visited_count, "steps": -1}


def dfs_any_path(start: str, goal: str, limit_nodes: int = 200000) -> Dict[str, Any]:
    """
    DFS finds *a* path (not guaranteed shortest).
    We use a stack and stop when goal is reached.
    """
    adjacency = get_adjacency_cached()
    if start == goal:
        return {"path": [start], "edges": [], "visited": 0, "steps": 0}

    visited = set()
    parent: Dict[str, Optional[str]] = {start: None}
    parent_edge: Dict[str, Optional[str]] = {start: None}

    stack: List[str] = [start]
    visited_count = 0

    while stack:
        u = stack.pop()
        if u in visited:
            continue
        visited.add(u)
        visited_count += 1
        if visited_count > limit_nodes:
            break

        if u == goal:
            path, edge_path = reconstruct_path(start, goal, parent, parent_edge)
            return {"path": path, "edges": edge_path, "visited": visited_count, "steps": len(edge_path)}

        for v, edge_id in adjacency.get(u, []):
            if v in visited:
                continue
            if v not in parent:
                parent[v] = u
                parent_edge[v] = edge_id
            if v == goal:
                parent.setdefault(v, u)
                parent_edge.setdefault(v, edge_id)
                path, edge_path = reconstruct_path(start, goal, parent, parent_edge)
                return {"path": path, "edges": edge_path, "visited": visited_count, "steps": len(edge_path)}
            stack.append(v)

    return {"path": [], "edges": [], "visited": visited_count, "steps": -1}


def dijkstra_shortest_km(start: str, goal: str) -> Dict[str, Any]:
    """
    Dijkstra on a weighted graph (edge weight = `km`).
    Finds shortest path by total distance.
    """
    adjacency = get_adjacency_cached()
    if start == goal:
        return {"path": [start], "edges": [], "visited": 0, "steps": 0}

    dist: Dict[str, float] = {start: 0.0}
    parent: Dict[str, Optional[str]] = {start: None}
    parent_edge: Dict[str, Optional[str]] = {start: None}

    heap: List[Tuple[float, str]] = [(0.0, start)]
    visited_count = 0

    while heap:
        d, u = heapq.heappop(heap)
        if d != dist.get(u, float("inf")):
            continue
        visited_count += 1
        if u == goal:
            path, edge_path = reconstruct_path(start, goal, parent, parent_edge)
            return {"path": path, "edges": edge_path, "visited": visited_count, "steps": len(edge_path)}

        for v, edge_id in adjacency.get(u, []):
            edge = STATE["edges"].get(edge_id, {})
            w = float(edge.get("km", 1.0))
            nd = d + w
            if nd < dist.get(v, float("inf")):
                dist[v] = nd
                parent[v] = u
                parent_edge[v] = edge_id
                heapq.heappush(heap, (nd, v))

    return {"path": [], "edges": [], "visited": visited_count, "steps": -1}


def compute_route(start: str, end: str, algo: str) -> Dict[str, Any]:
    t0 = time.time()
    if start not in STATE["stations"] or end not in STATE["stations"]:
        return {"ok": False, "error": "START_OR_END_NOT_FOUND", "ms": int((time.time() - t0) * 1000)}

    algo_l = (algo or "").lower()
    if algo_l == "dfs":
        res = dfs_any_path(start, end)
    elif algo_l in ("dijkstra", "dk", "djk"):
        res = dijkstra_shortest_km(start, end)
    else:
        # Default: BFS shortest number of edges.
        res = bfs_shortest_path(start, end)

    path = res["path"]
    edge_path = res["edges"]

    # Derive "lines passed through" from edge labels.
    lines_in_order: List[str] = []
    line_names_in_order: List[str] = []
    seen_lines = set()
    unique_line_names: List[str] = []

    for eid in edge_path:
        e = STATE["edges"].get(eid)
        if not e:
            continue
        line_id = e.get("line")
        if not line_id:
            continue
        lines_in_order.append(line_id)
        ln = STATE["lines"].get(line_id, {}).get("name")
        if ln is not None:
            line_names_in_order.append(ln)
        if line_id not in seen_lines:
            seen_lines.add(line_id)
            unique_line_names.append(STATE["lines"].get(line_id, {}).get("name", line_id))

    path_names = [STATE["stations"][sid]["name"] for sid in path] if path else []

    # Count line changes along the route (adjacent edges with different line).
    line_changes = 0
    if lines_in_order:
        for i in range(1, len(lines_in_order)):
            if lines_in_order[i] != lines_in_order[i - 1]:
                line_changes += 1

    # Total distance along the route (km).
    total_km = 0.0
    for eid in edge_path:
        e = STATE["edges"].get(eid, {})
        total_km += float(e.get("km", 0.0))

    # Fare estimation (approx): distance-based mapping by fare system.
    fare_models = {
        # BTS green line unified system approximation (Sukhumvit+Silom).
        "BTS_GREEN": {"min_baht": 17, "max_baht": 65, "min_km": 1.0, "max_km": 54.25},
        # MRT Blue line (Tha Phra -> Lak Song).
        "MRT_BLUE": {"min_baht": 17, "max_baht": 45, "min_km": 1.0, "max_km": 48.0},
        # MRT Purple line (Khlong Bang Phai -> Tao Poon).
        "MRT_PURPLE": {"min_baht": 14, "max_baht": 42, "min_km": 1.0, "max_km": 23.63},
    }

    def estimate_fare(system: str, km: float) -> Tuple[int, int]:
        model = fare_models.get(system)
        if not model or km <= 0:
            return 0, 0
        min_km = float(model["min_km"])
        max_km = float(model["max_km"])
        if max_km <= min_km:
            return int(model["max_baht"]), int(math.ceil(float(model["max_baht"])))
        ratio = (km - min_km) / (max_km - min_km)
        ratio = max(0.0, min(1.0, ratio))
        fare = float(model["min_baht"]) + (float(model["max_baht"]) - float(model["min_baht"])) * ratio
        fare_est = int(round(fare))
        fare_full = int(math.ceil(fare))
        return fare_est, fare_full

    dist_by_system: Dict[str, float] = {}
    for eid in edge_path:
        e = STATE["edges"].get(eid)
        if not e:
            continue
        line_id = e.get("line")
        if not line_id:
            continue  # transfers modeled with line=None
        line_meta = STATE["lines"].get(line_id, {})
        system = line_meta.get("system")
        if not system:
            continue
        dist_by_system[system] = dist_by_system.get(system, 0.0) + float(e.get("km", 0.0))

    def infer_system_from_station_id(sid: str) -> Optional[str]:
        if sid.startswith("BTS_"):
            return "BTS_GREEN"
        if sid.startswith("MRTB_"):
            return "MRT_BLUE"
        if sid.startswith("MRTP_"):
            return "MRT_PURPLE"
        if sid == "MRT_TAOPON":
            # If we used one of the MRT line segments, follow that. Otherwise, default to Blue.
            return "MRT_PURPLE" if "MRT_PURPLE" in lines_in_order else "MRT_BLUE"
        return None

    systems_present = set()
    for sid in path:
        sys = infer_system_from_station_id(sid)
        if sys:
            systems_present.add(sys)

    systems_to_charge = set(dist_by_system.keys()) | systems_present

    fare_breakdown: Dict[str, Any] = {}
    fare_est_total = 0
    fare_full_total = 0
    for system in systems_to_charge:
        km = dist_by_system.get(system, 0.0)
        # If the trip touches a system (start/end/interchange) but has 0 km inside it (only transfer edge),
        # charge at least the system minimum fare.
        if km <= 0.0:
            km = float(fare_models[system]["min_km"])
        fare_est, fare_full = estimate_fare(system, km)
        fare_breakdown[system] = {"km": km, "fare_est_baht": fare_est, "fare_full_baht": fare_full}
        fare_est_total += fare_est
        fare_full_total += fare_full

    # Improve line_changes for cases where we transfer into another system but do not ride any segment there
    # (e.g., ending at a MRT station connected from BTS by a zero-distance transfer edge).
    start_sys = infer_system_from_station_id(start)
    end_sys = infer_system_from_station_id(end)
    last_line_id = lines_in_order[-1] if lines_in_order else None
    if (
        start_sys
        and end_sys
        and start_sys != end_sys
        and dist_by_system.get(end_sys, 0.0) <= 0.0
        and last_line_id
    ):
        end_line_id = None
        if end_sys == "MRT_BLUE":
            end_line_id = "MRT_BLUE"
        elif end_sys == "MRT_PURPLE":
            end_line_id = "MRT_PURPLE"
        elif end_sys == "BTS_GREEN":
            end_line_id = last_line_id if last_line_id in ("BTS_SUK", "BTS_SIL") else "BTS_SUK"
        if end_line_id and end_line_id != last_line_id:
            line_changes += 1
            if end_line_id not in seen_lines:
                seen_lines.add(end_line_id)
                ln = STATE["lines"].get(end_line_id, {}).get("name", end_line_id)
                unique_line_names.append(ln)
                lines_in_order.append(end_line_id)
                line_names_in_order.append(ln)

    ms = int((time.time() - t0) * 1000)
    ok = len(path) > 0
    return {
        "ok": ok,
        "algo": algo.lower(),
        "start": start,
        "end": end,
        "path": path,
        "path_names": path_names,
        "edges": edge_path,
        "lines_in_order": lines_in_order,
        "lines_in_order_names": line_names_in_order,
        "line_changes": line_changes,
        "unique_line_names": unique_line_names,
        "visited": res["visited"],
        "steps": res["steps"],
        "total_km": total_km,
        "fare_est_baht": fare_est_total,
        "fare_full_baht": fare_full_total,
        "fare_breakdown": fare_breakdown,
        "ms": ms,
    }


# ----------------------------
# Utilities / Validation
# ----------------------------


ID_RE = re.compile(r"^[A-Za-z0-9_\-]{1,64}$")


def is_valid_id(x: Any) -> bool:
    if not isinstance(x, str):
        return False
    return bool(ID_RE.match(x))


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: Dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def read_json_body(handler: BaseHTTPRequestHandler, max_bytes: int = 2_000_000) -> Dict[str, Any]:
    length = handler.headers.get("Content-Length")
    if not length:
        return {}
    try:
        n = int(length)
    except ValueError:
        n = 0
    if n <= 0:
        return {}
    if n > max_bytes:
        raise ValueError("PAYLOAD_TOO_LARGE")
    raw = handler.rfile.read(n)
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def safe_get(d: Dict[str, Any], key: str, default: Any = None) -> Any:
    val = d.get(key, default)
    return val


# ----------------------------
# Web UI (minimal HTML)
# ----------------------------


def render_home() -> str:
    stations_html = ""
    for sid, s in sorted(STATE["stations"].items(), key=lambda x: x[0]):
        stations_html += f"<tr><td>{sid}</td><td>{s.get('name','')}</td></tr>"
    station_options_html = ""
    for sid, s in sorted(STATE["stations"].items(), key=lambda x: x[1].get("name", "")):
        label = s.get("name", sid)
        station_options_html += f"<option value=\"{sid}\">{label} ({sid})</option>"

    lines_html = ""
    for lid, l in sorted(STATE["lines"].items(), key=lambda x: x[0]):
        lines_html += f"<tr><td>{lid}</td><td>{l.get('name','')}</td></tr>"

    edges_html = ""
    for eid, e in sorted(STATE["edges"].items(), key=lambda x: x[0]):
        edges_html += (
            f"<tr><td>{eid}</td><td>{e.get('from')}</td><td>{e.get('to')}</td><td>{e.get('line')}</td><td>{e.get('km','')}</td></tr>"
        )

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>BTS/MRT Route Finder</title>
  <link
    rel="stylesheet"
    href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
    integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
    crossorigin=""
  />
  <style>
    :root {{
      --bg: #f6f8fb;
      --card: #ffffff;
      --line: #e5e7eb;
      --text: #111827;
      --muted: #6b7280;
      --primary: #2563eb;
      --primary-hover: #1d4ed8;
    }}
    body {{ font-family: Arial, sans-serif; margin: 18px; background: var(--bg); color: var(--text); }}
    h2 {{ margin: 6px 0 12px 0; }}
    h3 {{ margin: 4px 0 10px 0; }}
    .row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }}
    .card {{ border: 1px solid var(--line); border-radius: 12px; padding: 14px; background: var(--card); box-shadow: 0 2px 10px rgba(0,0,0,0.04); }}
    .card-wide {{ grid-column: 1 / -1; }}
    input, select {{
      padding: 8px 10px;
      margin: 4px 0 8px 0;
      width: 100%;
      box-sizing: border-box;
      border: 1px solid #d1d5db;
      border-radius: 8px;
      background: #fff;
    }}
    input:focus, select:focus {{ outline: 2px solid #bfdbfe; border-color: #93c5fd; }}
    button {{
      padding: 9px 10px;
      margin-top: 8px;
      cursor: pointer;
      width: 100%;
      border: 0;
      border-radius: 8px;
      background: var(--primary);
      color: white;
      font-weight: 600;
    }}
    button:hover {{ background: var(--primary-hover); }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 8px; }}
    th, td {{ border: 1px solid var(--line); padding: 7px; text-align: left; font-size: 12px; }}
    th {{ background: #f8fafc; }}
    pre {{ background: #0b1020; color: #d1e6ff; border: 1px solid #1f2937; border-radius: 8px; padding: 10px; overflow: auto; max-height: 300px; }}
    .muted {{ color: var(--muted); font-size: 12px; }}
    #routeMap {{ width: 100%; height: 520px; border: 1px solid var(--line); border-radius: 10px; }}
    .form-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
  </style>
</head>
<body>
  <h2>BTS/MRT Route Finder (Python, BFS/DFS/Dijkstra)</h2>
  <div class="muted">
    Data is in-memory. Use CRUD APIs below (or edit dict via export/import).
    For shortest by distance: use <b>Dijkstra</b>. Default uses <b>BFS</b> (min number of edges).
  </div>

  <div class="row">
    <div class="card">
      <h3>Find Route</h3>
      <form id="routeForm">
        <div class="form-grid">
          <div>
            <label>Start Station</label>
            <select name="start" required>
              <option value="">-- Select start station --</option>
              {station_options_html}
            </select>
          </div>
          <div>
            <label>End Station</label>
            <select name="end" required>
              <option value="">-- Select end station --</option>
              {station_options_html}
            </select>
          </div>
        </div>
        <label>Algorithm</label>
        <select name="algo">
          <option value="bfs" selected>BFS (shortest edges)</option>
          <option value="dfs">DFS (any path)</option>
          <option value="dijkstra">Dijkstra (shortest km)</option>
        </select>
        <button type="submit">Compute</button>
      </form>
    </div>

    <div class="card">
      <h3>Stations</h3>
      <table>
        <thead><tr><th>ID</th><th>Name</th></tr></thead>
        <tbody>{stations_html}</tbody>
      </table>
      <hr/>
      <h4>Create/Update Station</h4>
      <form id="stationForm">
        <label>Station ID</label>
        <input name="id" placeholder="e.g. S6" />
        <label>Station Name</label>
        <input name="name" placeholder="e.g. New Station" />
        <button type="submit">Save</button>
      </form>
      <h4>Delete Station</h4>
      <form id="delStationForm">
        <label>Station ID</label>
        <input name="id" placeholder="e.g. S6" />
        <button type="submit">Delete</button>
      </form>
    </div>

    <div class="card">
      <h3>Lines</h3>
      <table>
        <thead><tr><th>ID</th><th>Name</th></tr></thead>
        <tbody>{lines_html}</tbody>
      </table>
      <hr/>
      <h4>Create/Update Line</h4>
      <form id="lineForm">
        <label>Line ID</label>
        <input name="id" placeholder="e.g. L3" />
        <label>Line Name</label>
        <input name="name" placeholder="e.g. Line Red" />
        <button type="submit">Save</button>
      </form>
      <h4>Delete Line</h4>
      <form id="delLineForm">
        <label>Line ID</label>
        <input name="id" placeholder="e.g. L3" />
        <button type="submit">Delete</button>
      </form>
    </div>

    <div class="card">
      <h3>Edges (Connections)</h3>
      <table>
        <thead><tr><th>ID</th><th>From</th><th>To</th><th>Line</th><th>km</th></tr></thead>
        <tbody>{edges_html}</tbody>
      </table>
      <hr/>
      <h4>Create/Update Edge</h4>
      <form id="edgeForm">
        <label>Edge ID</label>
        <input name="id" placeholder="e.g. E5" />
        <label>From Station ID</label>
        <input name="from" placeholder="e.g. BTS_E4 or MRTB_BL22" />
        <label>To Station ID</label>
        <input name="to" placeholder="e.g. BTS_N24 or MRTB_BL34" />
        <label>Line ID</label>
        <input name="line" placeholder="e.g. L1" />
        <label>km (distance)</label>
        <input name="km" placeholder="e.g. 1.25" />
        <button type="submit">Save</button>
      </form>
      <h4>Delete Edge</h4>
      <form id="delEdgeForm">
        <label>Edge ID</label>
        <input name="id" placeholder="e.g. E5" />
        <button type="submit">Delete</button>
      </form>
    </div>

    <div class="card card-wide">
      <h3>Route On Real Map</h3>
      <div class="muted">plot ตามแผนที่จริงด้วย OpenStreetMap + OSRM (road geometry) และเปิดใน Google Maps ได้</div>
      <div style="margin:8px 0;">
        <button id="openGoogleMapsBtn" type="button">Open Current Route In Google Maps</button>
      </div>
      <div id="routeMap"></div>
    </div>
  </div>

  <script
    src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
    integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
    crossorigin=""
  ></script>
  <script>
    function formToObject(form) {{
      const fd = new FormData(form);
      const obj = {{}};
      for (const [k,v] of fd.entries()) obj[k] = v;
      return obj;
    }}

    async function postJson(url, body) {{
      const res = await fetch(url, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(body),
      }});
      const data = await res.json();
      return data;
    }}

    async function delJson(url) {{
      const res = await fetch(url, {{ method: 'DELETE' }});
      const data = await res.json();
      return data;
    }}

    const routeForm = document.getElementById('routeForm');
    const openGoogleMapsBtn = document.getElementById('openGoogleMapsBtn');
    let lastRouteStationNames = [];

    let routeMap = null;
    let routeLayerGroup = null;
    const geocodeCache = {{}};

    function initRouteMap() {{
      if (routeMap) return;
      routeMap = L.map('routeMap').setView([13.7563, 100.5018], 11);
      L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
        maxZoom: 19,
        attribution: '&copy; OpenStreetMap contributors',
      }}).addTo(routeMap);
      routeLayerGroup = L.layerGroup().addTo(routeMap);
    }}

    async function geocodeStation(name) {{
      if (!name) return null;
      if (geocodeCache[name]) return geocodeCache[name];
      const q = `${{name}} station Bangkok`;
      const url = `https://nominatim.openstreetmap.org/search?format=json&limit=1&q=${{encodeURIComponent(q)}}`;
      const res = await fetch(url, {{
        headers: {{
          'Accept': 'application/json',
        }},
      }});
      if (!res.ok) return null;
      const arr = await res.json();
      if (!Array.isArray(arr) || arr.length === 0) return null;
      const p = [parseFloat(arr[0].lat), parseFloat(arr[0].lon)];
      geocodeCache[name] = p;
      return p;
    }}

    async function fetchOsrmSegment(a, b) {{
      const url = `https://router.project-osrm.org/route/v1/driving/${{a[1]}},${{a[0]}};${{b[1]}},${{b[0]}}?overview=full&geometries=geojson`;
      const res = await fetch(url);
      if (!res.ok) return null;
      const data = await res.json();
      if (!data || data.code !== 'Ok' || !Array.isArray(data.routes) || data.routes.length === 0) return null;
      const coords = data.routes[0].geometry?.coordinates || [];
      if (!Array.isArray(coords) || coords.length === 0) return null;
      return coords.map(([lon, lat]) => [lat, lon]);
    }}

    async function drawRouteMap(pathNames) {{
      initRouteMap();
      routeLayerGroup.clearLayers();
      if (!Array.isArray(pathNames) || pathNames.length === 0) return;

      const coords = [];
      for (const name of pathNames) {{
        // Small delay to be polite with geocoding service and avoid bursts.
        await new Promise(r => setTimeout(r, 120));
        const p = await geocodeStation(name);
        if (p) coords.push({{ name, latlng: p }});
      }}
      if (coords.length === 0) return;

      for (let i = 0; i < coords.length; i++) {{
        const c = coords[i];
        const marker = L.circleMarker(c.latlng, {{
          radius: 5,
          color: i === 0 ? '#2b9348' : (i === coords.length - 1 ? '#d90429' : '#1d4ed8'),
          fillOpacity: 0.9,
        }});
        marker.bindPopup(`${{i + 1}}. ${{c.name}}`);
        marker.addTo(routeLayerGroup);
      }}

      let allBounds = [];
      let hasOsrm = false;
      for (let i = 0; i < coords.length - 1; i++) {{
        const seg = await fetchOsrmSegment(coords[i].latlng, coords[i + 1].latlng);
        if (seg && seg.length > 0) {{
          hasOsrm = true;
          const poly = L.polyline(seg, {{ color: '#d90429', weight: 4, opacity: 0.85 }});
          poly.addTo(routeLayerGroup);
          allBounds = allBounds.concat(seg);
        }} else {{
          // Fallback to straight line if OSRM has no route for this pair.
          const straight = [coords[i].latlng, coords[i + 1].latlng];
          const poly = L.polyline(straight, {{
            color: '#ef476f',
            weight: 3,
            opacity: 0.65,
            dashArray: '6,6',
          }});
          poly.addTo(routeLayerGroup);
          allBounds = allBounds.concat(straight);
        }}
      }}
      if (!hasOsrm) {{
        alert('OSRM route not found for some segments. Dashed lines are straight fallback.');
      }}
      if (allBounds.length > 1) {{
        routeMap.fitBounds(L.latLngBounds(allBounds), {{ padding: [20, 20] }});
      }}
    }}

    function openRouteInGoogleMaps(pathNames) {{
      if (!Array.isArray(pathNames) || pathNames.length < 2) {{
        alert('Please compute a route first.');
        return;
      }}
      const origin = `${{pathNames[0]}}, Bangkok`;
      const destination = `${{pathNames[pathNames.length - 1]}}, Bangkok`;
      const via = pathNames
        .slice(1, -1)
        .slice(0, 8)
        .map(x => `${{x}}, Bangkok`)
        .join('|');
      let url = `https://www.google.com/maps/dir/?api=1&travelmode=transit&origin=${{encodeURIComponent(origin)}}&destination=${{encodeURIComponent(destination)}}`;
      if (via) url += `&waypoints=${{encodeURIComponent(via)}}`;
      window.open(url, '_blank');
    }}

    openGoogleMapsBtn.addEventListener('click', () => {{
      openRouteInGoogleMaps(lastRouteStationNames);
    }});

    routeForm.addEventListener('submit', async (e) => {{
      e.preventDefault();
      const fd = new FormData(routeForm);
      const start = encodeURIComponent(fd.get('start') || '');
      const end = encodeURIComponent(fd.get('end') || '');
      const algo = fd.get('algo') || 'bfs';
      const url = `/api/route?start=${{start}}&end=${{end}}&algo=${{encodeURIComponent(algo)}}`;
      const res = await fetch(url);
      const data = await res.json();
      lastRouteStationNames = data.path_names || [];
      drawRouteMap(lastRouteStationNames);
    }});

    const stationForm = document.getElementById('stationForm');
    stationForm.addEventListener('submit', async (e) => {{
      e.preventDefault();
      const body = formToObject(stationForm);
      const data = await postJson('/api/stations', body);
      alert(JSON.stringify(data));
      window.location.reload();
    }});

    const delStationForm = document.getElementById('delStationForm');
    delStationForm.addEventListener('submit', async (e) => {{
      e.preventDefault();
      const fd = new FormData(delStationForm);
      const id = fd.get('id') || '';
      const data = await delJson(`/api/stations/${{encodeURIComponent(id)}}`);
      alert(JSON.stringify(data));
      window.location.reload();
    }});

    const lineForm = document.getElementById('lineForm');
    lineForm.addEventListener('submit', async (e) => {{
      e.preventDefault();
      const body = formToObject(lineForm);
      const data = await postJson('/api/lines', body);
      alert(JSON.stringify(data));
      window.location.reload();
    }});

    const delLineForm = document.getElementById('delLineForm');
    delLineForm.addEventListener('submit', async (e) => {{
      e.preventDefault();
      const fd = new FormData(delLineForm);
      const id = fd.get('id') || '';
      const data = await delJson(`/api/lines/${{encodeURIComponent(id)}}`);
      alert(JSON.stringify(data));
      window.location.reload();
    }});

    const edgeForm = document.getElementById('edgeForm');
    edgeForm.addEventListener('submit', async (e) => {{
      e.preventDefault();
      const body = formToObject(edgeForm);
      const data = await postJson('/api/edges', body);
      alert(JSON.stringify(data));
      window.location.reload();
    }});

    const delEdgeForm = document.getElementById('delEdgeForm');
    delEdgeForm.addEventListener('submit', async (e) => {{
      e.preventDefault();
      const fd = new FormData(delEdgeForm);
      const id = fd.get('id') || '';
      const data = await delJson(`/api/edges/${{encodeURIComponent(id)}}`);
      alert(JSON.stringify(data));
      window.location.reload();
    }});

    initRouteMap();
  </script>
</body>
</html>"""

    return html


def build_station_options_html() -> str:
    options = ""
    for sid, s in sorted(STATE["stations"].items(), key=lambda x: x[1].get("name", "")):
        label = s.get("name", sid)
        options += f"<option value=\"{sid}\">{label} ({sid})</option>"
    return options


def render_layout(title: str, active: str, content_html: str, script_js: str = "") -> str:
    nav_item = lambda key, href, label: (
        f'<a href="{href}" class="nav-item {"active" if active == key else ""}">{label}</a>'
    )
    nav_html = (
        nav_item("route", "/", "Route Planner")
        + nav_item("stations", "/stations", "Stations")
        + nav_item("lines", "/lines", "Lines")
        + nav_item("edges", "/edges", "Edges")
    )
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
  <link rel="stylesheet" href="https://cdn.datatables.net/1.13.8/css/jquery.dataTables.min.css"/>
  <style>
    :root {{
      --bg: #f6f8fb;
      --card: #ffffff;
      --line: #e5e7eb;
      --text: #111827;
      --muted: #6b7280;
      --primary: #2563eb;
      --primary-hover: #1d4ed8;
      --dark: #0f172a;
    }}
    body {{ font-family: Arial, sans-serif; margin: 0; background: var(--bg); color: var(--text); }}
    .topbar {{
      position: sticky; top: 0; z-index: 10;
      background: var(--dark); color: #fff; border-bottom: 1px solid #111827;
      padding: 10px 16px; display: flex; gap: 10px; align-items: center;
    }}
    .brand {{ font-weight: 700; margin-right: 14px; }}
    .nav-item {{
      color: #cbd5e1; text-decoration: none; padding: 8px 10px; border-radius: 8px; font-size: 14px;
    }}
    .nav-item:hover {{ background: #1e293b; color: #fff; }}
    .nav-item.active {{ background: #1d4ed8; color: #fff; }}
    .container {{ padding: 16px; max-width: 1400px; margin: 0 auto; }}
    .row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 16px; }}
    .card {{ border: 1px solid var(--line); border-radius: 12px; padding: 14px; background: var(--card); box-shadow: 0 2px 10px rgba(0,0,0,0.04); }}
    .card-wide {{ grid-column: 1 / -1; }}
    h2 {{ margin: 0 0 12px 0; }}
    h3 {{ margin: 4px 0 10px 0; }}
    input, select {{
      padding: 8px 10px; margin: 4px 0 8px 0; width: 100%;
      box-sizing: border-box; border: 1px solid #d1d5db; border-radius: 8px; background: #fff;
    }}
    input:focus, select:focus {{ outline: 2px solid #bfdbfe; border-color: #93c5fd; }}
    button {{
      padding: 9px 10px; margin-top: 8px; cursor: pointer; width: 100%;
      border: 0; border-radius: 8px; background: var(--primary); color: white; font-weight: 600;
    }}
    button:hover {{ background: var(--primary-hover); }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 8px; }}
    th, td {{ border: 1px solid var(--line); padding: 7px; text-align: left; font-size: 12px; }}
    th {{ background: #f8fafc; }}
    pre {{ background: #0b1020; color: #d1e6ff; border: 1px solid #1f2937; border-radius: 8px; padding: 10px; overflow: auto; max-height: 320px; }}
    .muted {{ color: var(--muted); font-size: 12px; }}
    .form-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
    #routeMap {{ width: 100%; height: 520px; border: 1px solid var(--line); border-radius: 10px; }}
    .btn-small {{ width: auto; padding: 6px 10px; margin: 0 4px 0 0; font-size: 12px; }}
    .btn-danger {{ background: #dc2626; }}
    .btn-danger:hover {{ background: #b91c1c; }}
    .action-cell {{ white-space: nowrap; }}
    .map-wrap {{ position: relative; }}
    .map-loading {{
      position: absolute; inset: 0; display: none; align-items: center; justify-content: center;
      background: rgba(255,255,255,0.75); z-index: 500; border-radius: 10px;
    }}
    .spinner {{
      width: 42px; height: 42px; border: 4px solid #dbeafe; border-top: 4px solid #2563eb;
      border-radius: 50%; animation: spin 0.9s linear infinite;
    }}
    @keyframes spin {{ from {{ transform: rotate(0deg); }} to {{ transform: rotate(360deg); }} }}
  </style>
</head>
<body>
  <div class="topbar">
    <div class="brand">BTS/MRT Planner</div>
    {nav_html}
  </div>
  <div class="container">
    {content_html}
  </div>
  <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
  <script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
  <script>
    async function postJson(url, body) {{
      const res = await fetch(url, {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(body),
      }});
      return await res.json();
    }}
    async function delJson(url) {{
      const res = await fetch(url, {{ method: 'DELETE' }});
      return await res.json();
    }}
    function formToObject(form) {{
      const fd = new FormData(form);
      const obj = {{}};
      for (const [k,v] of fd.entries()) obj[k] = v;
      return obj;
    }}
    function initDataTable(selector) {{
      if (!window.jQuery || !window.jQuery.fn || !window.jQuery.fn.DataTable) return;
      if (window.jQuery.fn.DataTable.isDataTable(selector)) return;
      window.jQuery(selector).DataTable({{
        pageLength: 10,
        lengthMenu: [10, 25, 50, 100],
        order: [],
      }});
    }}
    {script_js}
  </script>
</body>
</html>"""


def render_route_page() -> str:
    station_options_html = build_station_options_html()
    content_html = f"""
    <style>
      .route-layout {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; align-items: start; }}
      .route-viz {{ margin-top: 10px; border: 1px solid var(--line); border-radius: 10px; padding: 12px; background: #fff; max-height: 420px; overflow-y: auto; }}
      .route-item {{ display: flex; align-items: center; gap: 10px; min-height: 28px; }}
      .route-dot {{ width: 12px; height: 12px; border-radius: 50%; border: 2px solid #fff; box-shadow: 0 0 0 1px #94a3b8; flex-shrink: 0; }}
      .route-label {{ font-size: 13px; }}
      .route-connector {{ width: 4px; height: 20px; margin-left: 4px; border-radius: 4px; opacity: 0.9; }}
      .route-chip {{ display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px; color: #fff; margin-left: 8px; }}
      .graph-wrap {{ border: 1px solid var(--line); border-radius: 10px; background: #f3f4f6; padding: 10px; }}
      #routeGraph {{ width: 100%; height: 540px; display: block; background: #e5e7eb; border-radius: 8px; }}
      .route-points-card {{ min-height: 100%; }}
      @media (max-width: 980px) {{ .route-layout {{ grid-template-columns: 1fr; }} }}
    </style>
    <h2>Route Planner</h2>
    <div class="muted"></div>
    <div class="route-layout">
      <div class="card">
        <h3>Find Route (Distance-first)</h3>
        <form id="routeForm">
          <div class="form-grid">
            <div>
              <label>Start Station</label>
              <select name="start" required>
                <option value="">-- Select start station --</option>
                {station_options_html}
              </select>
            </div>
            <div>
              <label>End Station</label>
              <select name="end" required>
                <option value="">-- Select end station --</option>
                {station_options_html}
              </select>
            </div>
          </div>
          <button type="submit">Compute</button>
        </form>
        <div id="routeSummary" style="margin-top:10px; display:none;" tabindex="-1">
          <h3 style="margin-top:12px;">Result</h3>
          <table>
            <tbody>
              <tr><th style="width:40%;">Lines Used</th><td id="sumLinesUsed">-</td></tr>
              <tr><th>Distance</th><td id="sumDistance">-</td></tr>
              <tr><th>Line Changes</th><td id="sumLineChanges">-</td></tr>
              <tr><th>Estimated Fare</th><td id="sumFareEst">-</td></tr>
            </tbody>
          </table>
          <div id="sumError" class="muted" style="margin-top:6px; color:#b91c1c;"></div>
        </div>
      </div>
      <div class="card route-points-card">
        <h3>Route Points</h3>
        <div class="muted"></div>
        <div id="routeVizStatus" class="muted" style="margin:6px 0 8px 0;"></div>
        <div id="routeViz" class="route-viz"></div>
      </div>
    </div>
    <div class="row" style="margin-top:16px;">
      <div class="card card-wide">
        <h3>Current Graph View</h3>
        <div class="muted"></div>
        <div id="graphStatus" class="muted" style="margin:6px 0 8px 0;"></div>
        <div class="graph-wrap"><svg id="routeGraph"></svg></div>
      </div>
    </div>
    """
    script_js = """
    const routeForm = document.getElementById('routeForm');
    const routeSummary = document.getElementById('routeSummary');
    const sumDistance = document.getElementById('sumDistance');
    const sumFareEst = document.getElementById('sumFareEst');
    const sumLineChanges = document.getElementById('sumLineChanges');
    const sumLinesUsed = document.getElementById('sumLinesUsed');
    const sumError = document.getElementById('sumError');
    const graphStatus = document.getElementById('graphStatus');
    const routeGraph = document.getElementById('routeGraph');
    const routeViz = document.getElementById('routeViz');
    const routeVizStatus = document.getElementById('routeVizStatus');
    let edgeKmById = {};

    function colorByLine(lineName) {{
      const s = String(lineName || '').toLowerCase();
      if (s.includes('sukhumvit') || s.includes('light green')) return '#2fb344';
      if (s.includes('silom')) return '#166534';
      if (s.includes('blue')) return '#2563eb';
      if (s.includes('purple')) return '#7c3aed';
      return '#64748b';
    }}

    function renderRoutePoints(pathNames, linesInOrderNames) {{
      routeViz.innerHTML = '';
      if (!Array.isArray(pathNames) || pathNames.length === 0) {{
        routeVizStatus.textContent = 'No stations to display.';
        return;
      }}
      const frag = document.createDocumentFragment();
      for (let i = 0; i < pathNames.length; i++) {{
        const lineName = i === 0 ? (linesInOrderNames?.[0] || 'Start') : (linesInOrderNames?.[i - 1] || 'Transfer');
        const color = colorByLine(lineName);

        const row = document.createElement('div');
        row.className = 'route-item';
        const dot = document.createElement('div');
        dot.className = 'route-dot';
        dot.style.background = color;
        const label = document.createElement('div');
        label.className = 'route-label';
        label.textContent = `${i + 1}. ${pathNames[i]}`;
        const chip = document.createElement('span');
        chip.className = 'route-chip';
        chip.style.background = color;
        chip.textContent = lineName;
        row.appendChild(dot);
        row.appendChild(label);
        if (i > 0) row.appendChild(chip);
        frag.appendChild(row);

        if (i < pathNames.length - 1) {{
          const connLineName = linesInOrderNames?.[i] || 'Transfer';
          const connColor = colorByLine(connLineName);
          const conn = document.createElement('div');
          conn.className = 'route-connector';
          conn.style.background = connColor;
          frag.appendChild(conn);
        }}
      }}
      routeViz.appendChild(frag);
      routeVizStatus.textContent = '';
    }}

    function nodeNameLabel(name) {
      const s = String(name || '');
      if (s.length <= 12) return s;
      return `${s.slice(0, 11)}…`;
    }

    function clearGraph() {
      while (routeGraph.firstChild) routeGraph.removeChild(routeGraph.firstChild);
    }

    function renderRouteGraph(pathIds, pathNames, edgeIds, linesInOrderNames) {
      clearGraph();
      if (!Array.isArray(pathIds) || pathIds.length < 2) {
        graphStatus.textContent = 'No graph to display.';
        return;
      }
      const width = Math.max(900, routeGraph.clientWidth || 900);
      const height = 540;
      routeGraph.setAttribute('viewBox', `0 0 ${width} ${height}`);

      const n = pathIds.length;
      const nodes = pathIds.map((sid, i) => ({
        id: sid,
        name: pathNames?.[i] || sid,
        x: width * (0.15 + (0.7 * (i / Math.max(1, n - 1)))) + ((i % 2 === 0) ? -35 : 35),
        y: height * (0.25 + (0.5 * ((i % 5) / 4))),
      }));
      const links = [];
      for (let i = 0; i < n - 1; i++) {
        const km = Number(edgeKmById[edgeIds?.[i]] || 0);
        links.push({
          source: nodes[i],
          target: nodes[i + 1],
          lineName: linesInOrderNames?.[i] || 'Transfer',
          kmLabel: km > 0 ? `${Math.round(km * 10) / 10} km` : '',
        });
      }

      const centerX = width / 2;
      const centerY = height / 2;
      for (let iter = 0; iter < 220; iter++) {
        for (let i = 0; i < nodes.length; i++) {
          for (let j = i + 1; j < nodes.length; j++) {
            const a = nodes[i], b = nodes[j];
            const dx = b.x - a.x, dy = b.y - a.y;
            const dist2 = Math.max(80, dx * dx + dy * dy);
            const force = 8000 / dist2;
            const fx = (dx / Math.sqrt(dist2)) * force;
            const fy = (dy / Math.sqrt(dist2)) * force;
            a.x -= fx; a.y -= fy; b.x += fx; b.y += fy;
          }
        }
        for (const e of links) {
          const dx = e.target.x - e.source.x;
          const dy = e.target.y - e.source.y;
          const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
          const desired = 130;
          const pull = (dist - desired) * 0.015;
          const fx = (dx / dist) * pull;
          const fy = (dy / dist) * pull;
          e.source.x += fx; e.source.y += fy; e.target.x -= fx; e.target.y -= fy;
        }
        for (const nd of nodes) {
          nd.x += (centerX - nd.x) * 0.0025;
          nd.y += (centerY - nd.y) * 0.0025;
          nd.x = Math.max(45, Math.min(width - 45, nd.x));
          nd.y = Math.max(45, Math.min(height - 45, nd.y));
        }
      }

      const svgNS = 'http://www.w3.org/2000/svg';
      for (const e of links) {
        const line = document.createElementNS(svgNS, 'line');
        line.setAttribute('x1', String(e.source.x));
        line.setAttribute('y1', String(e.source.y));
        line.setAttribute('x2', String(e.target.x));
        line.setAttribute('y2', String(e.target.y));
        line.setAttribute('stroke', colorByLine(e.lineName));
        line.setAttribute('stroke-width', '2.6');
        line.setAttribute('stroke-opacity', '0.9');
        routeGraph.appendChild(line);

        if (e.kmLabel) {
          const tx = (e.source.x + e.target.x) / 2;
          const ty = (e.source.y + e.target.y) / 2 - 6;
          const text = document.createElementNS(svgNS, 'text');
          text.setAttribute('x', String(tx));
          text.setAttribute('y', String(ty));
          text.setAttribute('text-anchor', 'middle');
          text.setAttribute('font-size', '12');
          text.setAttribute('fill', '#334155');
          text.textContent = e.kmLabel;
          routeGraph.appendChild(text);
        }
      }

      nodes.forEach((nd, i) => {
        const circle = document.createElementNS(svgNS, 'circle');
        circle.setAttribute('cx', String(nd.x));
        circle.setAttribute('cy', String(nd.y));
        circle.setAttribute('r', '18');
        circle.setAttribute('fill', '#7dd3fc');
        circle.setAttribute('stroke', '#0891b2');
        circle.setAttribute('stroke-width', '1.5');
        routeGraph.appendChild(circle);

        const text = document.createElementNS(svgNS, 'text');
        text.setAttribute('x', String(nd.x));
        text.setAttribute('y', String(nd.y + 4));
        text.setAttribute('text-anchor', 'middle');
        text.setAttribute('font-size', '9');
        text.setAttribute('font-weight', '700');
        text.setAttribute('fill', '#0f172a');
        text.textContent = nodeNameLabel(nd.name);
        routeGraph.appendChild(text);

        const title = document.createElementNS(svgNS, 'title');
        title.textContent = `${i + 1}. ${nd.name}`;
        circle.appendChild(title);
      });
      graphStatus.textContent = '';
      routeGraph.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    async function ensureEdgeKmMap() {
      if (Object.keys(edgeKmById).length > 0) return;
      try {
        const res = await fetch('/api/network');
        const data = await res.json();
        if (!data || !Array.isArray(data.edges)) return;
        edgeKmById = {};
        for (const e of data.edges) edgeKmById[e.id] = Number(e.km || 0);
      } catch (_) {}
    }

    routeForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const fd = new FormData(routeForm);
      const start = encodeURIComponent(fd.get('start') || '');
      const end = encodeURIComponent(fd.get('end') || '');
      const data = await (await fetch(`/api/route?start=${start}&end=${end}&algo=dijkstra`)).json();
      routeSummary.style.display = 'block';
      if (!data.ok) {
        sumDistance.textContent = '-';
        sumFareEst.textContent = '-';
        sumLineChanges.textContent = '-';
        sumLinesUsed.textContent = '-';
        sumError.textContent = `Cannot compute route: ${data.error || 'UNKNOWN_ERROR'}`;
        return;
      }
      sumError.textContent = '';
      const km = Number(data.total_km || 0);
      sumDistance.textContent = `${km.toFixed(2)} km`;
      sumFareEst.textContent = `${Number(data.fare_est_baht || 0)} baht`;
      sumLineChanges.textContent = String(Number(data.line_changes || 0));
      sumLinesUsed.textContent = Array.isArray(data.unique_line_names) && data.unique_line_names.length > 0
        ? data.unique_line_names.join(' -> ')
        : '-';
      await ensureEdgeKmMap();
      renderRoutePoints(data.path_names || [], data.lines_in_order_names || []);
      renderRouteGraph(data.path || [], data.path_names || [], data.edges || [], data.lines_in_order_names || []);
      routeSummary.scrollIntoView({ behavior: 'smooth', block: 'start' });
      routeSummary.focus({ preventScroll: true });
    });
    """
    return render_layout("Route Planner", "route", content_html, script_js)


def render_stations_page() -> str:
    stations_html = ""
    for sid, s in sorted(STATE["stations"].items(), key=lambda x: x[0]):
        nm = s.get("name", "")
        stations_html += (
            f"<tr>"
            f"<td>{sid}</td><td>{nm}</td>"
            f"<td class='action-cell'>"
            f"<button class='btn-small station-edit' data-id='{sid}' data-name='{nm}' type='button'>Edit</button>"
            f"<button class='btn-small btn-danger station-del' data-id='{sid}' type='button'>Delete</button>"
            f"</td>"
            f"</tr>"
        )
    content_html = f"""
    <h2>Stations CRUD</h2>
    <div class="row">
      <div class="card card-wide">
        <table id="stationsTable">
          <thead><tr><th>ID</th><th>Name</th><th>Actions</th></tr></thead>
          <tbody>{stations_html}</tbody>
        </table>
      </div>
      <div class="card">
        <h3>Create/Update Station</h3>
        <form id="stationForm">
          <label>Station ID</label><input name="id" placeholder="e.g. BTS_NEW" />
          <label>Station Name</label><input name="name" placeholder="e.g. New Station" />
          <button type="submit">Save</button>
        </form>
      </div>
      <div class="card">
        <h3>Delete Station</h3>
        <form id="delStationForm">
          <label>Station ID</label><input name="id" placeholder="e.g. BTS_NEW" />
          <button type="submit">Delete</button>
        </form>
      </div>
    </div>
    """
    script_js = """
    initDataTable('#stationsTable');
    document.getElementById('stationForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      alert(JSON.stringify(await postJson('/api/stations', formToObject(e.target))));
      window.location.reload();
    });
    document.getElementById('delStationForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const id = new FormData(e.target).get('id') || '';
      alert(JSON.stringify(await delJson(`/api/stations/${encodeURIComponent(id)}`)));
      window.location.reload();
    });
    document.querySelectorAll('.station-edit').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelector('#stationForm input[name=\"id\"]').value = btn.dataset.id || '';
        document.querySelector('#stationForm input[name=\"name\"]').value = btn.dataset.name || '';
        window.scrollTo({ top: document.getElementById('stationForm').offsetTop - 80, behavior: 'smooth' });
      });
    });
    document.querySelectorAll('.station-del').forEach(btn => {
      btn.addEventListener('click', async () => {
        const id = btn.dataset.id || '';
        if (!id) return;
        if (!confirm(`Delete station ${id}?`)) return;
        alert(JSON.stringify(await delJson(`/api/stations/${encodeURIComponent(id)}`)));
        window.location.reload();
      });
    });
    """
    return render_layout("Stations CRUD", "stations", content_html, script_js)


def render_lines_page() -> str:
    lines_html = ""
    for lid, l in sorted(STATE["lines"].items(), key=lambda x: x[0]):
        nm = l.get("name", "")
        lines_html += (
            f"<tr>"
            f"<td>{lid}</td><td>{nm}</td>"
            f"<td class='action-cell'>"
            f"<button class='btn-small line-edit' data-id='{lid}' data-name='{nm}' type='button'>Edit</button>"
            f"<button class='btn-small btn-danger line-del' data-id='{lid}' type='button'>Delete</button>"
            f"</td>"
            f"</tr>"
        )
    content_html = f"""
    <h2>Lines CRUD</h2>
    <div class="row">
      <div class="card card-wide">
        <table id="linesTable">
          <thead><tr><th>ID</th><th>Name</th><th>Actions</th></tr></thead>
          <tbody>{lines_html}</tbody>
        </table>
      </div>
      <div class="card">
        <h3>Create/Update Line</h3>
        <form id="lineForm">
          <label>Line ID</label><input name="id" placeholder="e.g. L_NEW" />
          <label>Line Name</label><input name="name" placeholder="e.g. New Line" />
          <button type="submit">Save</button>
        </form>
      </div>
      <div class="card">
        <h3>Delete Line</h3>
        <form id="delLineForm">
          <label>Line ID</label><input name="id" placeholder="e.g. L_NEW" />
          <button type="submit">Delete</button>
        </form>
      </div>
    </div>
    """
    script_js = """
    initDataTable('#linesTable');
    document.getElementById('lineForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      alert(JSON.stringify(await postJson('/api/lines', formToObject(e.target))));
      window.location.reload();
    });
    document.getElementById('delLineForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const id = new FormData(e.target).get('id') || '';
      alert(JSON.stringify(await delJson(`/api/lines/${encodeURIComponent(id)}`)));
      window.location.reload();
    });
    document.querySelectorAll('.line-edit').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelector('#lineForm input[name=\"id\"]').value = btn.dataset.id || '';
        document.querySelector('#lineForm input[name=\"name\"]').value = btn.dataset.name || '';
        window.scrollTo({ top: document.getElementById('lineForm').offsetTop - 80, behavior: 'smooth' });
      });
    });
    document.querySelectorAll('.line-del').forEach(btn => {
      btn.addEventListener('click', async () => {
        const id = btn.dataset.id || '';
        if (!id) return;
        if (!confirm(`Delete line ${id}?`)) return;
        alert(JSON.stringify(await delJson(`/api/lines/${encodeURIComponent(id)}`)));
        window.location.reload();
      });
    });
    """
    return render_layout("Lines CRUD", "lines", content_html, script_js)


def render_edges_page() -> str:
    edges_html = ""
    for eid, e in sorted(STATE["edges"].items(), key=lambda x: x[0]):
        a = e.get("from")
        b = e.get("to")
        line_id = e.get("line")
        km = e.get("km", "")
        line_disp = "" if line_id is None else line_id
        edges_html += (
            f"<tr>"
            f"<td>{eid}</td><td>{a}</td><td>{b}</td><td>{line_disp}</td><td>{km}</td>"
            f"<td class='action-cell'>"
            f"<button class='btn-small edge-edit' data-id='{eid}' data-from='{a}' data-to='{b}' data-line='{line_disp}' data-km='{km}' type='button'>Edit</button>"
            f"<button class='btn-small btn-danger edge-del' data-id='{eid}' type='button'>Delete</button>"
            f"</td>"
            f"</tr>"
        )
    content_html = f"""
    <h2>Edges CRUD</h2>
    <div class="row">
      <div class="card card-wide">
        <table id="edgesTable">
          <thead><tr><th>ID</th><th>From</th><th>To</th><th>Line</th><th>km</th><th>Actions</th></tr></thead>
          <tbody>{edges_html}</tbody>
        </table>
      </div>
      <div class="card">
        <h3>Create/Update Edge</h3>
        <form id="edgeForm">
          <label>Edge ID</label><input name="id" placeholder="e.g. E_NEW" />
          <label>From Station ID</label><input name="from" placeholder="e.g. BTS_E4" />
          <label>To Station ID</label><input name="to" placeholder="e.g. MRTB_BL22" />
          <label>Line ID (optional for transfer)</label><input name="line" placeholder="e.g. MRT_BLUE or leave empty" />
          <label>km (distance)</label><input name="km" placeholder="e.g. 1.25" />
          <button type="submit">Save</button>
        </form>
      </div>
      <div class="card">
        <h3>Delete Edge</h3>
        <form id="delEdgeForm">
          <label>Edge ID</label><input name="id" placeholder="e.g. E_NEW" />
          <button type="submit">Delete</button>
        </form>
      </div>
    </div>
    """
    script_js = """
    initDataTable('#edgesTable');
    document.getElementById('edgeForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const body = formToObject(e.target);
      if (body.line === '') body.line = null;
      alert(JSON.stringify(await postJson('/api/edges', body)));
      window.location.reload();
    });
    document.getElementById('delEdgeForm').addEventListener('submit', async (e) => {
      e.preventDefault();
      const id = new FormData(e.target).get('id') || '';
      alert(JSON.stringify(await delJson(`/api/edges/${encodeURIComponent(id)}`)));
      window.location.reload();
    });
    document.querySelectorAll('.edge-edit').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelector('#edgeForm input[name=\"id\"]').value = btn.dataset.id || '';
        document.querySelector('#edgeForm input[name=\"from\"]').value = btn.dataset.from || '';
        document.querySelector('#edgeForm input[name=\"to\"]').value = btn.dataset.to || '';
        document.querySelector('#edgeForm input[name=\"line\"]').value = btn.dataset.line || '';
        document.querySelector('#edgeForm input[name=\"km\"]').value = btn.dataset.km || '';
        window.scrollTo({ top: document.getElementById('edgeForm').offsetTop - 80, behavior: 'smooth' });
      });
    });
    document.querySelectorAll('.edge-del').forEach(btn => {
      btn.addEventListener('click', async () => {
        const id = btn.dataset.id || '';
        if (!id) return;
        if (!confirm(`Delete edge ${id}?`)) return;
        alert(JSON.stringify(await delJson(`/api/edges/${encodeURIComponent(id)}`)));
        window.location.reload();
      });
    });
    """
    return render_layout("Edges CRUD", "edges", content_html, script_js)


# ----------------------------
# HTTP handler
# ----------------------------


class Handler(BaseHTTPRequestHandler):
    server_version = "BTSMRT/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        # Keep logs clean; comment this out if you want full request logs.
        return

    def _read_body_if_json(self) -> Dict[str, Any]:
        try:
            return read_json_body(self)
        except json.JSONDecodeError:
            return {}

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        q = parse_qs(parsed.query)

        if path == "/" or path == "":
            html = render_route_page()
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/stations":
            html = render_stations_page()
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/lines":
            html = render_lines_page()
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/edges":
            html = render_edges_page()
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/api/stations":
            json_response(self, 200, {"ok": True, "stations": STATE["stations"]})
            return

        if path.startswith("/api/stations/"):
            sid = path.split("/", 3)[3]
            if sid not in STATE["stations"]:
                json_response(self, 404, {"ok": False, "error": "STATION_NOT_FOUND"})
                return
            json_response(self, 200, {"ok": True, "station": STATE["stations"][sid]})
            return

        if path == "/api/lines":
            json_response(self, 200, {"ok": True, "lines": STATE["lines"]})
            return

        if path.startswith("/api/lines/"):
            lid = path.split("/", 3)[3]
            if lid not in STATE["lines"]:
                json_response(self, 404, {"ok": False, "error": "LINE_NOT_FOUND"})
                return
            json_response(self, 200, {"ok": True, "line": STATE["lines"][lid]})
            return

        if path == "/api/edges":
            json_response(self, 200, {"ok": True, "edges": STATE["edges"]})
            return

        if path.startswith("/api/edges/"):
            eid = path.split("/", 3)[3]
            if eid not in STATE["edges"]:
                json_response(self, 404, {"ok": False, "error": "EDGE_NOT_FOUND"})
                return
            json_response(self, 200, {"ok": True, "edge": STATE["edges"][eid]})
            return

        if path == "/api/route":
            start = (q.get("start") or [""])[0]
            end = (q.get("end") or [""])[0]
            algo = (q.get("algo") or ["bfs"])[0]
            payload = compute_route(start, end, algo)
            status = 200 if payload.get("ok") else 400
            json_response(self, status, payload)
            return

        if path == "/api/export":
            # Export whole dict for backup.
            json_response(self, 200, {"ok": True, "state": {k: v for k, v in STATE.items() if not k.startswith("_")}})
            return

        if path == "/api/health":
            json_response(self, 200, {"ok": True, "rev": STATE.get("_rev", 0), "stations": len(STATE["stations"]), "lines": len(STATE["lines"]), "edges": len(STATE["edges"])})
            return

        if path == "/api/network":
            nodes = []
            for sid, station in STATE["stations"].items():
                nodes.append({"id": sid, "name": station.get("name", sid)})
            edges = []
            for eid, edge in STATE["edges"].items():
                edges.append(
                    {
                        "id": eid,
                        "from": edge.get("from"),
                        "to": edge.get("to"),
                        "line": edge.get("line"),
                        "km": float(edge.get("km", 0.0)),
                    }
                )
            json_response(self, 200, {"ok": True, "nodes": nodes, "edges": edges})
            return

        json_response(self, 404, {"ok": False, "error": "NOT_FOUND"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/stations":
            body = self._read_body_if_json()
            sid = safe_get(body, "id")
            name = safe_get(body, "name", "")
            if not is_valid_id(sid):
                json_response(self, 400, {"ok": False, "error": "INVALID_STATION_ID"})
                return
            if not isinstance(name, str) or not name.strip():
                json_response(self, 400, {"ok": False, "error": "INVALID_STATION_NAME"})
                return
            STATE["stations"][sid] = {"name": name.strip()}
            bump_rev()
            json_response(self, 200, {"ok": True, "station": STATE["stations"][sid]})
            return

        if path == "/api/lines":
            body = self._read_body_if_json()
            lid = safe_get(body, "id")
            name = safe_get(body, "name", "")
            if not is_valid_id(lid):
                json_response(self, 400, {"ok": False, "error": "INVALID_LINE_ID"})
                return
            if not isinstance(name, str) or not name.strip():
                json_response(self, 400, {"ok": False, "error": "INVALID_LINE_NAME"})
                return
            # Preserve extra metadata (e.g., `system`) if this line already exists.
            STATE["lines"][lid] = {**STATE["lines"].get(lid, {}), "name": name.strip()}
            bump_rev()
            json_response(self, 200, {"ok": True, "line": STATE["lines"][lid]})
            return

        if path == "/api/edges":
            body = self._read_body_if_json()
            eid = safe_get(body, "id")
            a = safe_get(body, "from")
            b = safe_get(body, "to")
            lid = safe_get(body, "line")
            km_raw = safe_get(body, "km", 1.0)

            if not is_valid_id(eid):
                json_response(self, 400, {"ok": False, "error": "INVALID_EDGE_ID"})
                return
            if a not in STATE["stations"] or b not in STATE["stations"]:
                json_response(self, 400, {"ok": False, "error": "STATION_NOT_FOUND_FOR_EDGE"})
                return
            if lid is not None and lid not in STATE["lines"]:
                json_response(self, 400, {"ok": False, "error": "LINE_NOT_FOUND_FOR_EDGE"})
                return

            if a == b:
                json_response(self, 400, {"ok": False, "error": "SELF_LOOP_NOT_ALLOWED"})
                return

            try:
                km = float(km_raw)
            except (TypeError, ValueError):
                json_response(self, 400, {"ok": False, "error": "INVALID_EDGE_KM"})
                return

            STATE["edges"][eid] = {"from": a, "to": b, "line": lid, "km": km}
            bump_rev()
            json_response(self, 200, {"ok": True, "edge": STATE["edges"][eid]})
            return

        if path == "/api/import":
            # Replace current state from exported dict.
            body = self._read_body_if_json()
            new_state = safe_get(body, "state", None)
            if not isinstance(new_state, dict):
                json_response(self, 400, {"ok": False, "error": "INVALID_IMPORT_PAYLOAD"})
                return

            stations = new_state.get("stations")
            lines = new_state.get("lines")
            edges = new_state.get("edges")

            if not isinstance(stations, dict) or not isinstance(lines, dict) or not isinstance(edges, dict):
                json_response(self, 400, {"ok": False, "error": "IMPORT_MISSING_KEYS"})
                return

            # Validate minimal shape.
            for sid, s in stations.items():
                if not is_valid_id(sid) or not isinstance(s, dict) or not isinstance(s.get("name"), str):
                    json_response(self, 400, {"ok": False, "error": "INVALID_STATION_SHAPE"})
                    return
            for lid, l in lines.items():
                if not is_valid_id(lid) or not isinstance(l, dict) or not isinstance(l.get("name"), str):
                    json_response(self, 400, {"ok": False, "error": "INVALID_LINE_SHAPE"})
                    return
            for eid, e in edges.items():
                if not is_valid_id(eid) or not isinstance(e, dict):
                    json_response(self, 400, {"ok": False, "error": "INVALID_EDGE_SHAPE"})
                    return
                line_id = e.get("line")
                if e.get("from") not in stations or e.get("to") not in stations:
                    json_response(self, 400, {"ok": False, "error": "EDGE_REFERENCE_NOT_FOUND"})
                    return
                # `line` can be None for transfer edges.
                if line_id is not None and line_id not in lines:
                    json_response(self, 400, {"ok": False, "error": "EDGE_REFERENCE_NOT_FOUND"})
                    return

            # Replace.
            STATE["stations"] = stations
            STATE["lines"] = lines
            STATE["edges"] = edges
            bump_rev()
            json_response(self, 200, {"ok": True, "imported": True})
            return

        json_response(self, 404, {"ok": False, "error": "NOT_FOUND"})

    def do_PUT(self) -> None:
        # Simple semantics: PUT to /api/stations/<id> etc.
        parsed = urlparse(self.path)
        path = parsed.path
        body = self._read_body_if_json()

        if path.startswith("/api/stations/"):
            sid = path.split("/", 3)[3]
            if sid not in STATE["stations"]:
                json_response(self, 404, {"ok": False, "error": "STATION_NOT_FOUND"})
                return
            name = safe_get(body, "name", None)
            if not isinstance(name, str) or not name.strip():
                json_response(self, 400, {"ok": False, "error": "INVALID_STATION_NAME"})
                return
            STATE["stations"][sid]["name"] = name.strip()
            bump_rev()
            json_response(self, 200, {"ok": True, "station": STATE["stations"][sid]})
            return

        if path.startswith("/api/lines/"):
            lid = path.split("/", 3)[3]
            if lid not in STATE["lines"]:
                json_response(self, 404, {"ok": False, "error": "LINE_NOT_FOUND"})
                return
            name = safe_get(body, "name", None)
            if not isinstance(name, str) or not name.strip():
                json_response(self, 400, {"ok": False, "error": "INVALID_LINE_NAME"})
                return
            STATE["lines"][lid]["name"] = name.strip()
            bump_rev()
            json_response(self, 200, {"ok": True, "line": STATE["lines"][lid]})
            return

        if path.startswith("/api/edges/"):
            eid = path.split("/", 3)[3]
            if eid not in STATE["edges"]:
                json_response(self, 404, {"ok": False, "error": "EDGE_NOT_FOUND"})
                return
            a = safe_get(body, "from", None)
            b = safe_get(body, "to", None)
            lid = safe_get(body, "line", None)
            km_raw = safe_get(body, "km", 1.0)
            try:
                km = float(km_raw)
            except (TypeError, ValueError):
                json_response(self, 400, {"ok": False, "error": "INVALID_EDGE_KM"})
                return
            if a not in STATE["stations"] or b not in STATE["stations"]:
                json_response(self, 400, {"ok": False, "error": "INVALID_EDGE_REFERENCES"})
                return
            if lid is not None and lid not in STATE["lines"]:
                json_response(self, 400, {"ok": False, "error": "INVALID_EDGE_REFERENCES"})
                return
            if a == b:
                json_response(self, 400, {"ok": False, "error": "SELF_LOOP_NOT_ALLOWED"})
                return
            STATE["edges"][eid] = {"from": a, "to": b, "line": lid, "km": km}
            bump_rev()
            json_response(self, 200, {"ok": True, "edge": STATE["edges"][eid]})
            return

        json_response(self, 404, {"ok": False, "error": "NOT_FOUND"})

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path.startswith("/api/stations/"):
            sid = path.split("/", 3)[3]
            if sid not in STATE["stations"]:
                json_response(self, 404, {"ok": False, "error": "STATION_NOT_FOUND"})
                return
            # Remove station and incident edges.
            del STATE["stations"][sid]
            edges_to_delete = [eid for eid, e in STATE["edges"].items() if e.get("from") == sid or e.get("to") == sid]
            for eid in edges_to_delete:
                STATE["edges"].pop(eid, None)
            bump_rev()
            json_response(self, 200, {"ok": True, "deleted": sid})
            return

        if path.startswith("/api/lines/"):
            lid = path.split("/", 3)[3]
            if lid not in STATE["lines"]:
                json_response(self, 404, {"ok": False, "error": "LINE_NOT_FOUND"})
                return
            # Remove line and edges that reference it.
            del STATE["lines"][lid]
            edges_to_delete = [eid for eid, e in STATE["edges"].items() if e.get("line") == lid]
            for eid in edges_to_delete:
                STATE["edges"].pop(eid, None)
            bump_rev()
            json_response(self, 200, {"ok": True, "deleted": lid})
            return

        if path.startswith("/api/edges/"):
            eid = path.split("/", 3)[3]
            if eid not in STATE["edges"]:
                json_response(self, 404, {"ok": False, "error": "EDGE_NOT_FOUND"})
                return
            del STATE["edges"][eid]
            bump_rev()
            json_response(self, 200, {"ok": True, "deleted": eid})
            return

        json_response(self, 404, {"ok": False, "error": "NOT_FOUND"})


# ----------------------------
# Entrypoint
# ----------------------------


def run_streamlit_app() -> None:
    import matplotlib.pyplot as plt
    import pandas as pd
    import streamlit as st

    st.set_page_config(page_title="BTS/MRT Planner", layout="wide")
    st.title("BTS/MRT Planner")

    if "state" not in st.session_state:
        st.session_state["state"] = build_default_state()
        st.session_state["graph_cache"] = {"rev": None, "adj": None}

    global STATE, _GRAPH_CACHE
    STATE = st.session_state["state"]
    _GRAPH_CACHE = st.session_state["graph_cache"]

    tabs = st.tabs(["Route Planner", "Stations CRUD", "Lines CRUD", "Edges CRUD"])

    with tabs[0]:
        left, right = st.columns([1, 1], gap="large")
        with left:
            st.subheader("Find Route")
            station_ids = sorted(STATE["stations"].keys())
            start = st.selectbox("Start Station", station_ids, format_func=lambda x: f"{x} - {STATE['stations'][x]['name']}")
            end = st.selectbox("End Station", station_ids, index=min(1, len(station_ids) - 1), format_func=lambda x: f"{x} - {STATE['stations'][x]['name']}")
            algo = st.selectbox("Algorithm", ["dijkstra", "bfs", "dfs"], index=0)
            do_compute = st.button("Compute Route", type="primary")

            if do_compute:
                result = compute_route(start, end, algo)
                st.session_state["last_route"] = result

            result = st.session_state.get("last_route")
            if result:
                if not result.get("ok"):
                    st.error(f"Cannot compute route: {result.get('error', 'UNKNOWN_ERROR')}")
                else:
                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("Distance", f"{float(result.get('total_km', 0)):.2f} km")
                    c2.metric("Est. Fare", f"{int(result.get('fare_est_baht', 0))} baht")
                    c3.metric("Line Changes", int(result.get("line_changes", 0)))
                    c4.metric("Stops", int(result.get("steps", 0)) + 1)
                    st.write("Lines Used:", " -> ".join(result.get("unique_line_names", [])) or "-")

        with right:
            st.subheader("Route Points")
            result = st.session_state.get("last_route")
            if result and result.get("ok"):
                names = result.get("path_names", [])
                line_names = result.get("lines_in_order_names", [])
                rows: List[Dict[str, Any]] = []
                for i, nm in enumerate(names):
                    seg_line = line_names[i - 1] if i > 0 and i - 1 < len(line_names) else "-"
                    rows.append({"No.": i + 1, "Station": nm, "Line": seg_line})
                st.dataframe(pd.DataFrame(rows), use_container_width=True, height=420, hide_index=True)
            else:
                st.info("Compute route to see route points.")

        st.subheader("Current Graph View")
        result = st.session_state.get("last_route")
        if result and result.get("ok"):
            path_ids = result.get("path", [])
            path_names = result.get("path_names", [])
            edge_ids = result.get("edges", [])
            line_names = result.get("lines_in_order_names", [])

            n = len(path_ids)
            if n >= 2:
                xs = [0.12 + (0.76 * (i / max(1, n - 1))) for i in range(n)]
                ys = [0.2 + 0.6 * ((i % 5) / 4) for i in range(n)]

                fig, ax = plt.subplots(figsize=(15, 6), facecolor="#e5e7eb")
                ax.set_facecolor("#e5e7eb")
                ax.set_title("Current Graph View")
                ax.axis("off")

                # Draw edges + km labels.
                for i in range(n - 1):
                    x1, y1 = xs[i], ys[i]
                    x2, y2 = xs[i + 1], ys[i + 1]
                    line_name = line_names[i] if i < len(line_names) else ""
                    color = "#64748b"
                    s = str(line_name).lower()
                    if "sukhumvit" in s or "light green" in s:
                        color = "#2fb344"
                    elif "silom" in s:
                        color = "#166534"
                    elif "blue" in s:
                        color = "#2563eb"
                    elif "purple" in s:
                        color = "#7c3aed"
                    ax.plot([x1, x2], [y1, y2], color=color, linewidth=2.4, alpha=0.9)

                    eid = edge_ids[i] if i < len(edge_ids) else None
                    km = float(STATE["edges"].get(eid, {}).get("km", 0.0)) if eid else 0.0
                    if km > 0:
                        mx, my = (x1 + x2) / 2.0, (y1 + y2) / 2.0
                        ax.text(mx, my + 0.03, f"{km:.1f} km", ha="center", va="center", fontsize=9, color="#334155")

                # Draw nodes + station labels.
                for i in range(n):
                    x, y = xs[i], ys[i]
                    ax.scatter([x], [y], s=680, c="#7dd3fc", edgecolors="#0891b2", linewidths=1.4, zorder=3)
                    label = str(path_names[i]) if i < len(path_names) else str(path_ids[i])
                    if len(label) > 13:
                        label = label[:12] + "…"
                    ax.text(x, y, label, ha="center", va="center", fontsize=8, fontweight="bold", color="#0f172a", zorder=4)

                st.pyplot(fig, use_container_width=True)
            else:
                st.info("Route too short for graph.")
        else:
            st.info("Compute route to see graph.")

    with tabs[1]:
        st.subheader("Stations")
        st.dataframe(
            pd.DataFrame([{"id": sid, "name": s.get("name", "")} for sid, s in sorted(STATE["stations"].items())]),
            use_container_width=True,
            height=360,
            hide_index=True,
        )
        c1, c2 = st.columns(2)
        with c1:
            with st.form("station_add_update"):
                sid = st.text_input("Station ID", value="")
                name = st.text_input("Station Name", value="")
                submitted = st.form_submit_button("Create / Update")
                if submitted:
                    if not is_valid_id(sid) or not name.strip():
                        st.error("Invalid station id or name.")
                    else:
                        STATE["stations"][sid] = {"name": name.strip()}
                        bump_rev()
                        st.success(f"Saved station {sid}")
        with c2:
            with st.form("station_delete"):
                sid_del = st.text_input("Delete Station ID", value="")
                submitted_del = st.form_submit_button("Delete")
                if submitted_del:
                    if sid_del not in STATE["stations"]:
                        st.error("Station not found.")
                    else:
                        del STATE["stations"][sid_del]
                        for eid in [eid for eid, e in STATE["edges"].items() if e.get("from") == sid_del or e.get("to") == sid_del]:
                            STATE["edges"].pop(eid, None)
                        bump_rev()
                        st.success(f"Deleted station {sid_del}")

    with tabs[2]:
        st.subheader("Lines")
        st.dataframe(
            pd.DataFrame([{"id": lid, "name": l.get("name", ""), "system": l.get("system", "")} for lid, l in sorted(STATE["lines"].items())]),
            use_container_width=True,
            height=360,
            hide_index=True,
        )
        c1, c2 = st.columns(2)
        with c1:
            with st.form("line_add_update"):
                lid = st.text_input("Line ID", value="")
                lname = st.text_input("Line Name", value="")
                lsys = st.text_input("System", value="")
                submitted = st.form_submit_button("Create / Update")
                if submitted:
                    if not is_valid_id(lid) or not lname.strip():
                        st.error("Invalid line id or name.")
                    else:
                        STATE["lines"][lid] = {"name": lname.strip(), "system": lsys.strip() or "CUSTOM"}
                        bump_rev()
                        st.success(f"Saved line {lid}")
        with c2:
            with st.form("line_delete"):
                lid_del = st.text_input("Delete Line ID", value="")
                submitted_del = st.form_submit_button("Delete")
                if submitted_del:
                    if lid_del not in STATE["lines"]:
                        st.error("Line not found.")
                    else:
                        del STATE["lines"][lid_del]
                        for eid in [eid for eid, e in STATE["edges"].items() if e.get("line") == lid_del]:
                            STATE["edges"].pop(eid, None)
                        bump_rev()
                        st.success(f"Deleted line {lid_del}")

    with tabs[3]:
        st.subheader("Edges")
        st.dataframe(
            pd.DataFrame(
                [
                    {"id": eid, "from": e.get("from"), "to": e.get("to"), "line": e.get("line"), "km": e.get("km", 0)}
                    for eid, e in sorted(STATE["edges"].items())
                ]
            ),
            use_container_width=True,
            height=360,
            hide_index=True,
        )
        c1, c2 = st.columns(2)
        with c1:
            with st.form("edge_add_update"):
                eid = st.text_input("Edge ID", value="")
                a = st.text_input("From Station ID", value="")
                b = st.text_input("To Station ID", value="")
                lid = st.text_input("Line ID (optional)", value="")
                km = st.number_input("Distance (km)", min_value=0.0, value=1.0, step=0.1)
                submitted = st.form_submit_button("Create / Update")
                if submitted:
                    if (not is_valid_id(eid)) or (a not in STATE["stations"]) or (b not in STATE["stations"]) or (a == b):
                        st.error("Invalid edge id/references.")
                    elif lid and lid not in STATE["lines"]:
                        st.error("Line ID not found.")
                    else:
                        STATE["edges"][eid] = {"from": a, "to": b, "line": (lid or None), "km": float(km)}
                        bump_rev()
                        st.success(f"Saved edge {eid}")
        with c2:
            with st.form("edge_delete"):
                eid_del = st.text_input("Delete Edge ID", value="")
                submitted_del = st.form_submit_button("Delete")
                if submitted_del:
                    if eid_del not in STATE["edges"]:
                        st.error("Edge not found.")
                    else:
                        del STATE["edges"][eid_del]
                        bump_rev()
                        st.success(f"Deleted edge {eid_del}")

    st.caption("In-memory dict mode. Data resets when Streamlit process restarts.")


if __name__ == "__main__":
    run_streamlit_app()
