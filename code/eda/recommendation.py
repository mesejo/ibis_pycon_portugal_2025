import ibis

from pathlib import Path

data_dir = Path(__file__).parents[2] / "data"
coffees = ibis.read_parquet(data_dir / "cleaned_coffees.parquet")
expr = coffees.select("origin_country", "origin_region", "tasting_notes").limit(1)

expr = expr.mutate(tasting_notes=ibis._.tasting_notes.split(" "))

con = ibis.sqlite.connect(data_dir / "coffees.db")

coffee = con.table("coffee")
roaster = con.table("roaster")
coffee_tasting_note = con.table("coffee_tasting_note")
tasting_note = con.table("tasting_note")
origin = con.table("origin")


def relevance(relevance_coef=0.5):
    return ibis._.sca_points.fill_null(0) + ibis._.price_eur * relevance_coef


expr = (
    coffees.select(
        "title",
        "sca_points",
        "price_eur",
        "origin_country",
        "origin_region",
        "tasting_notes",
    )
    .mutate(
        tasting_score=ibis._.tasting_notes.split(" ")
        .intersect(["citrus", "floral", "bright"])
        .length()
    )
    .select(
        ibis._.title,
        (
            ibis._.tasting_score
            + (ibis._.origin_region == ibis.literal("Yirgacheffe")).cast(int)
            + (ibis._.origin_country == ibis.literal("Ethiopia")).cast(int)
        ).name("score"),
        relevance=relevance(),
    )
    .order_by(ibis._.score.desc(), ibis._.relevance.desc())
)

res = expr.execute()
print(res)


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
        tasting_score=ibis._.note.isin(["citrus", "floral", "bright"]).sum(),
        origin_region=ibis._.origin_region.first(),
        origin_country=ibis._.origin_country.first(),
        sca_points=ibis._.sca_points.max(),
        price_eur=ibis._.price_eur.min(),
    )
    .select(
        ibis._.coffee_id,
        (
            ibis._.tasting_score
            + (ibis._.origin_region == ibis.literal("Yirgacheffe")).cast(int)
            + (ibis._.origin_country == ibis.literal("Ethiopia")).cast(int)
        ).name("score"),
        relevance=relevance(),
    )
    .order_by(ibis._.score.desc(), ibis._.relevance.desc())
)

res = expr.execute()
print(res)
