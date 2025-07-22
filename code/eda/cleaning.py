from pathlib import Path

import ibis


coffee_raw = ibis.read_csv(
    (data_dir := Path(__file__).parents[2].joinpath("data")) / "coffees.csv"
)


cleaned_coffee = coffee_raw.mutate(
    # Clean whitespace
    roaster_name=coffee_raw.roaster_name.strip(),
    roaster_country=coffee_raw.roaster_country.strip(),
    title=coffee_raw.title.strip(),
    # Normalize altitude
    altitude=(
        coffee_raw.altitude.strip().upper().re_replace(r"[^\d]", "").cast("int64")
    ),
    # Handle missing values
    sca_points_filled=coffee_raw.sca_points.fill_null(82.0),
    price_eur_filled=coffee_raw.price_eur.fill_null(18.0),
    # Clean description
    description=coffee_raw.description.strip(),
).filter(
    # Remove rows with missing essential data, notice the use of the deferred variable name
    ibis._.roaster_country.notnull(),
    ibis._.origin_country.notnull(),
    ibis._.price_eur.notnull(),
)

cleaned_coffee.to_parquet(data_dir / "cleaned_coffees.parquet")


