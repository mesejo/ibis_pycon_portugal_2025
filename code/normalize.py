"""
Build a normalized SQLite database from coffees.csv.
----------------------------------------------------
• Outputs coffees.db in the same folder.
• Requires only the Python std‑lib + pandas.
"""

import re
import sqlite3
from pathlib import Path

import pandas as pd

PARQUET = Path(__file__).parents[1] / "data" / "cleaned_coffees.parquet"
DB  = Path("coffees.db")

# ---------------------------------------------------------------------
# 1.  Load raw data
# ---------------------------------------------------------------------
df = pd.read_parquet(PARQUET)

# ---------------------------------------------------------------------
# 2.  Dimension tables
# ---------------------------------------------------------------------
# -- Roaster -----------------------------------------------------------
roasters = (
    df[["roaster_name", "roaster_country"]]
    .drop_duplicates()
    .sort_values("roaster_name")
    .reset_index(drop=True)
)
roasters.insert(0, "roaster_id", roasters.index + 1)
print(roasters)

# -- Origin ------------------------------------------------------------
origins = (
    df[["origin_country", "origin_region"]]
    .drop_duplicates()
    .sort_values(["origin_country", "origin_region"])
    .reset_index(drop=True)
)
origins.insert(0, "origin_id", origins.index + 1)
print(origins)

# -- Tasting notes -----------------------------------------------------
def explode_notes(s: str) -> list[str]:
    """Split 'citrus floral, bright' → ['citrus','floral','bright']."""
    tokens = re.split(r"[,/;]", s)            # first cut on punctuation
    words  = (w for t in tokens for w in t.split())  # then on spaces
    return [w.strip().lower() for w in words if w.strip()]


vocab = sorted({w for notes in df["tasting_notes"] for w in explode_notes(notes)})
notes = pd.DataFrame({"note": vocab})
notes.insert(0, "note_id", notes.index + 1)
print(notes)

# ---------------------------------------------------------------------
# 3.  Fact tables
# ---------------------------------------------------------------------
# add FK columns to original frame
df = (
    df.merge(roasters, on=["roaster_name", "roaster_country"])
      .merge(origins,  on=["origin_country", "origin_region"])
)

# -- Coffee ------------------------------------------------------------
coffee_cols = [
    "title", "roaster_id", "origin_id", "process", "altitude",
    "sca_points", "price_eur", "description",
]
coffees = df[coffee_cols].copy()
coffees.insert(0, "coffee_id", coffees.index + 1)

# -- Coffee‑Tasting‑Note bridge ---------------------------------------
bridge_rows = []
for idx, row in df.iterrows():
    c_id = idx + 1                               # matches coffee_id above
    for word in explode_notes(row["tasting_notes"]):
        n_id = notes.loc[notes.note == word, "note_id"].iat[0]
        bridge_rows.append({"coffee_id": c_id, "note_id": n_id})

coffee_notes = pd.DataFrame(bridge_rows)

# ---------------------------------------------------------------------
# 4.  Persist to SQLite with FK constraints
# ---------------------------------------------------------------------
DB.unlink(missing_ok=True)                       # start fresh each run
conn = sqlite3.connect(DB)
cur  = conn.cursor()

cur.executescript(
    """
    PRAGMA foreign_keys = ON;

    CREATE TABLE roaster(
        roaster_id     INTEGER PRIMARY KEY,
        roaster_name   TEXT    NOT NULL,
        roaster_country TEXT
    );

    CREATE TABLE origin(
        origin_id      INTEGER PRIMARY KEY,
        origin_country TEXT,
        origin_region  TEXT
    );

    CREATE TABLE tasting_note(
        note_id  INTEGER PRIMARY KEY,
        note     TEXT NOT NULL UNIQUE
    );

    CREATE TABLE coffee(
        coffee_id   INTEGER PRIMARY KEY,
        title       TEXT    NOT NULL,
        roaster_id  INTEGER NOT NULL REFERENCES roaster(roaster_id),
        origin_id   INTEGER NOT NULL REFERENCES origin(origin_id),
        process     TEXT,
        altitude    TEXT,
        sca_points  REAL,
        price_eur   REAL,
        description TEXT
    );

    CREATE TABLE coffee_tasting_note(
        coffee_id INTEGER NOT NULL REFERENCES coffee(coffee_id),
        note_id   INTEGER NOT NULL REFERENCES tasting_note(note_id),
        PRIMARY KEY (coffee_id, note_id)
    );
    """
)

roasters.to_sql("roaster",            conn, if_exists="append", index=False)
origins.to_sql("origin",              conn, if_exists="append", index=False)
notes.to_sql("tasting_note",          conn, if_exists="append", index=False)
coffees.to_sql("coffee",              conn, if_exists="append", index=False)
coffee_notes.to_sql("coffee_tasting_note", conn, if_exists="append", index=False)

conn.commit()
conn.close()

print("coffees.db created with normalized schema")
