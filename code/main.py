from operator import itemgetter
from pathlib import Path
from typing import Annotated

import ibis
from fastapi import FastAPI, Depends, Query
from ibis import _
from ibis.backends.sql import SQLBackend

from eda.recommendation import relevance, similarity


def from_env() -> SQLBackend:
    return ibis.sqlite.connect(Path(__file__).parents[1].joinpath("data", "coffees.db"))

def fetch_all(expr: ibis.Table, limit=None) -> list:
    return expr.to_pandas(limit=limit).to_dict(orient="records")

def fetch_one(expr: ibis.Table) -> dict:
    return fetch_all(expr, limit=1)[0]
app = FastAPI()

ConnectionDep = Annotated[SQLBackend, Depends(from_env)]


@app.get("/coffees")
def read_coffees(con: ConnectionDep, region: list[str] = Query(default=[])):
    coffee = con.table("coffee")
    roaster = con.table("roaster")
    origin = con.table("origin")

    coffees = (
        coffee.join(roaster, ["roaster_id"])
        .join(origin, ["origin_id"])
        .fill_null({"sca_points": 0.0})
    )

    if region:
        coffees = coffees.filter(_.origin_region.isin(region))

    return fetch_all(coffees)

@app.get("/coffees/{coffee_id}")
def read_coffee(coffee_id: int, con: ConnectionDep):
    coffee = con.table("coffee")
    roaster = con.table("roaster")
    coffee_tasting_note = con.table("coffee_tasting_note")
    tasting_note = con.table("tasting_note")

    expr = (
        coffee.join(roaster, ["roaster_id"])
        .join(coffee_tasting_note, ["coffee_id"])
        .join(tasting_note, ["note_id"])
        .filter(_.coffee_id == coffee_id)
        .group_by([_.coffee_id, _.roaster_name, _.title])
        .aggregate(flavour_profile=_.note.group_concat(sep=", "))
        .select(_.roaster_name, _.title, _.flavour_profile)
    )

    return fetch_one(expr)

@app.get("/coffees/{coffee_id}/recommended")
def read_coffee_recommendations(coffee_id: int, con: ConnectionDep):
    coffee = con.table("coffee")
    roaster = con.table("roaster")
    coffee_tasting_note = con.table("coffee_tasting_note")
    tasting_note = con.table("tasting_note")
    origin = con.table("origin")

    needle = (
        coffee.join(origin, ["origin_id"])
        .join(coffee_tasting_note, ["coffee_id"])
        .join(tasting_note, ["note_id"])
        .filter(_.coffee_id == coffee_id)
        .group_by(
            ibis._.coffee_id,
        )
        .aggregate(
            tasting_notes=ibis._.note.group_concat(sep=" "),
            origin_region=ibis._.origin_region.first(),
            origin_country=ibis._.origin_country.first(),
        )
        .to_pandas(limit=1)
    )

    region, country, tasting_notes = (
        needle.assign(
            tasting_notes=needle.tasting_notes.str.split()
        ).loc[0, ["origin_region", "origin_country", "tasting_notes"]]
    )

    expr = (
        coffee.join(roaster, ["roaster_id"])
        .join(origin, ["origin_id"])
        .join(coffee_tasting_note, ["coffee_id"])
        .join(tasting_note, ["note_id"])
        .group_by(
            [
                ibis._.coffee_id,
                ibis._.title,
            ]
        )
        .aggregate(
            tasting_score=ibis._.note.isin(tasting_notes).sum(),
            origin_region=ibis._.origin_region.first(),
            origin_country=ibis._.origin_country.first(),
            sca_points=ibis._.sca_points.max(),
            price_eur=ibis._.price_eur.min(),
        )
        .select(
            ibis._.coffee_id,
                ibis._.title,
            similarity(region, country),
            relevance=relevance(),
        )
        .order_by(ibis._.score.desc(), ibis._.relevance.desc())
    )

    return fetch_all(expr, limit=5)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
