import polars as pl

from src import storage


def get_districts():

    states = storage.load_parquet("/br/states.parquet").select(
        pl.col("id_state"), pl.col("short_name").alias("state")
    )
    cities = storage.load_parquet("/br/cities.parquet").select(
        pl.col("id_city"), pl.col("name").alias("city"), pl.col("id_state")
    )
    districts = storage.load_parquet("/br/districs.parquet").select(
        pl.col("name").alias("district"), pl.col("id_city")
    )

    df = (
        districts.join(cities, ["id_city"])
        .join(states, ["id_state"])
        .drop("id_city", "id_state")
    )

    return df
