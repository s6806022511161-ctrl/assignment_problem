# BTS/MRT Streamlit Route Planner

Single-file Python web app for BTS/MRT route planning with in-memory CRUD management.

## Features

- Route search with graph algorithms: `Dijkstra`, `BFS`, `DFS`
- Trip summary: distance, estimated fare, line changes, stations count
- Graph visualization (node/edge) with distance labels on each edge
- In-memory CRUD for:
  - Stations
  - Lines
  - Edges
- Built with Streamlit (no separate frontend framework)

## Tech Stack

- Python 3.10+ (recommended)
- Streamlit
- Pandas
- Matplotlib

## Project Structure

- `app.py` - main application (all logic + UI)
- `requirements.txt` - Python dependencies

## Installation

### 1) Clone repository

```bash
git clone https://github.com/s6806022511161-ctrl/assignment_problem.git
cd assignment_problem
```

### 2) Create virtual environment

Windows (PowerShell):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

macOS/Linux:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3) Install dependencies

```bash
pip install -r requirements.txt
```

## Run Application

```bash
streamlit run app.py
```

Default URL:

- `http://localhost:8501`

## Usage

1. Open **Route Planner** tab
2. Select **Start Station** and **End Station**
3. Choose algorithm (recommended: `dijkstra`)
4. Click **Compute Route**
5. Review:
   - metrics summary
   - route points table
   - current graph view (station nodes + edge distances)

## Notes

- Data is stored in-memory only (`dict`), so changes reset when app restarts.
- If you need persistence, add file/database storage in a next version.