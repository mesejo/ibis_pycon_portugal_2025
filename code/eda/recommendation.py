import ibis

from pathlib import Path

data_dir = Path(__file__).parents[2] / "data"
coffees = ibis.read_parquet(data_dir / "cleaned_coffees.parquet")

def relevance(relevance_coef=0.5):
    return ibis._.sca_points.fill_null(0) + ibis._.price_eur * relevance_coef

def similarity(region, country):
    return (
            ibis._.tasting_score
            + (ibis._.origin_region == region).ifelse(1, 0)
            + (ibis._.origin_country == country).ifelse(1, 0)
    ).name("score")

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
        similarity("Yirgacheffe", "Ethiopia"),
        relevance=relevance(),
    )
    .order_by(ibis._.score.desc(), ibis._.relevance.desc())
)

if __name__ == "__main__":
    res = expr.execute()
    print(res)

