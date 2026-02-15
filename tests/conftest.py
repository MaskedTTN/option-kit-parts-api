import os
import sys
import sqlite3
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

# Ensure src is importable
HERE = os.path.dirname(__file__)
SRC_DIR = os.path.abspath(os.path.join(HERE, '..', 'src'))

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import main
import services.db as services_db


def init_db(conn: sqlite3.Connection):
    cursor = conn.cursor()

    # Create minimal schema used by tests
    cursor.executescript(r"""
    CREATE TABLE vehicles (
        vid TEXT PRIMARY KEY,
        series TEXT,
        body TEXT,
        model TEXT,
        market TEXT,
        prod_month TEXT,
        engine TEXT,
        steering TEXT,
        created_at TEXT
    );

    CREATE TABLE main_group_definitions (
        mg_number TEXT PRIMARY KEY,
        mg_name TEXT,
        description TEXT
    );

    CREATE TABLE subgroup_definitions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mg_number TEXT,
        sg_number TEXT,
        sg_name TEXT
    );

    CREATE TABLE vehicle_main_groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vid TEXT,
        mg_number TEXT,
        url TEXT
    );

    CREATE TABLE vehicle_subgroups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vehicle_mg_id INTEGER,
        sg_definition_id INTEGER
    );

    CREATE TABLE diagrams (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vehicle_subgroup_id INTEGER,
        diagram_id TEXT,
        title TEXT,
        url TEXT
    );

    CREATE TABLE parts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        diagram_id INTEGER,
        position TEXT,
        description TEXT,
        part_number TEXT,
        quantity TEXT,
        supplement TEXT,
        from_date TEXT,
        up_to_date TEXT,
        price TEXT,
        notes TEXT,
        option_requirements TEXT,
        option_codes TEXT
    );
    """)

    # Seed basic data (populate all fields required by response model)
    cursor.execute(
        "INSERT INTO vehicles (vid, series, body, model, market, prod_month, engine, steering, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("TESTVID", "3", "Sedan", "330i", "US", "2026-02", "B48", "LHD", "2026-02-15")
    )
    cursor.execute("INSERT INTO main_group_definitions (mg_number, mg_name) VALUES (?, ?)", ("11", "Engine"))
    cursor.execute("INSERT INTO vehicle_main_groups (vid, mg_number, url) VALUES (?, ?, ?)", ("TESTVID", "11", "/mg/11"))
    vmg_id = cursor.lastrowid
    cursor.execute("INSERT INTO subgroup_definitions (mg_number, sg_number, sg_name) VALUES (?, ?, ?)", ("11", "10", "Engine Sub"))
    sg_def_id = cursor.lastrowid
    cursor.execute("INSERT INTO vehicle_subgroups (vehicle_mg_id, sg_definition_id) VALUES (?, ?)", (vmg_id, sg_def_id))
    vsg_id = cursor.lastrowid
    cursor.execute("INSERT INTO diagrams (vehicle_subgroup_id, diagram_id, title, url) VALUES (?, ?, ?, ?)", (vsg_id, "D1", "Main Diagram", "/d/1"))
    diagram_id = cursor.lastrowid
    # Part with option codes
    cursor.execute(
        "INSERT INTO parts (diagram_id, position, description, part_number, quantity, supplement, from_date, up_to_date, price, notes, option_requirements, option_codes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (diagram_id, "1", "Control Module", "1234", "1", "", "2020-01", "", "150.00", "", None, "S710A=Yes")
    )
    # Part without option codes
    cursor.execute(
        "INSERT INTO parts (diagram_id, position, description, part_number, quantity, supplement, from_date, up_to_date, price, notes, option_requirements, option_codes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (diagram_id, "2", "Universal Part", "5678", "1", "", "2020-01", "", "50.00", "", None, None)
    )

    conn.commit()


@contextmanager
def get_db_override(conn: sqlite3.Connection):
    try:
        yield conn
    finally:
        pass


@pytest.fixture
def client(tmp_path):
    # Create a single in-memory connection and seed it
    # Allow the in-memory sqlite connection to be used from FastAPI test server threads
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    # Monkeypatch the application's get_db to use the in-memory connection
    # The routers import `get_db` from `services.db` at import time, so override
    # both the services module and the router module references.
    services_db.get_db = lambda: get_db_override(conn)
    try:
        import routers.v1 as routers_v1
        routers_v1.get_db = lambda: get_db_override(conn)
    except Exception:
        # If routers are not imported yet, main import will pull them in later.
        pass
    # Also override main.get_db if present
    try:
        main.get_db = lambda: get_db_override(conn)
    except Exception:
        pass

    return TestClient(main.app)
