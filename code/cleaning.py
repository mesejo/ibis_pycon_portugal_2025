from pathlib import Path

import ibis
from ibis import _


coffee_raw = ibis.read_csv(Path(__file__).parents[1].joinpath("data", "coffees.csv"))


cleaned_coffee = (coffee_raw
.mutate(
    # Clean whitespace
    roaster_name=coffee_raw.roaster_name.strip(),
    roaster_country=coffee_raw.roaster_country.strip(),
    title=coffee_raw.title.strip(),

    # Normalize altitude
    altitude_clean=coffee_raw.altitude.strip().upper().re_replace(r'[^\d]', '').cast('int64'),

    # Handle missing values
    sca_points_filled=coffee_raw.sca_points.fill_null(82.0),
    price_eur_filled=coffee_raw.price_eur.fill_null(18.0),

    # Clean description
    description_clean=coffee_raw.description.strip()
)
.filter(
    # Remove rows with missing essential data, notice the use of the deferred variable name
    _.roaster_country.notnull(),
    _.origin_country.notnull(),
    _.price_eur.notnull()
))

# res = ibis.to_sql(cleaned_coffee, dialect="postgres")
