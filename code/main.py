from pathlib import Path
from typing import Annotated

import ibis
from fastapi import FastAPI, Depends, Query
from ibis import _
from ibis.backends.sql import SQLBackend


def from_env() -> SQLBackend:
    return ibis.sqlite.connect(Path(__file__).parents[1].joinpath("data", "coffees.db"))


app = FastAPI()

ConnectionDep = Annotated[SQLBackend, Depends(from_env)]


@app.get("/coffees")
def read_coffees(con: ConnectionDep, region: list[str] = Query(default=[])):
    coffee = con.table("coffee")
    roaster = con.table("roaster")
    origin = con.table("origin")

    coffees = coffee.join(roaster, ["roaster_id"]).join(origin, ["origin_id"]).fill_null({"sca_points": 0.0})

    if region:
        coffees = coffees.filter(_.origin_region.isin(region))

    return (
        coffees
        .to_pandas()
        .to_dict(orient="records")
    )


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

    return expr.to_pandas(limit=1).to_dict(orient="records")[0]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
