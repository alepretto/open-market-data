import os
from dataclasses import dataclass, field
from datetime import datetime

import polars as pl


@dataclass
class ZapImoveisAnalysis:
    data_path: str
    df_ads: pl.DataFrame = field(default_factory=pl.DataFrame)

    def make(self):

        df_analysis = self.get_data()

        df_analysis = df_analysis.with_columns(
            area=pl.col("area")
            .str.replace("Tamanho do imóvel", "")
            .str.replace("m²", "")
            .str.strip_chars(),
            bedrooms=pl.col("bedrooms").str.replace("Quantidade de quartos\n", ""),
            bathrooms=pl.col("bathrooms").str.replace("Quantidade de banheiros\n", ""),
            location=pl.col("location").str.replace("para alugar ", ""),
            fees=pl.col("fees").str.split("•"),
            price=pl.col("price").str.replace("R\\$", "").str.split("/"),
        )

        df_analysis = df_analysis.with_columns(
            condominio=pl.col("fees")
            .list.eval(
                pl.when(pl.element().str.contains("Cond")).then(
                    pl.element().str.replace("Cond. R\\$", "").str.strip_chars()
                )
            )
            .list.drop_nulls()
            .list.first(),
            iptu=pl.col("fees")
            .list.eval(
                pl.when(pl.element().str.contains("IPTU")).then(
                    pl.element().str.replace("IPTU R\\$", "").str.strip_chars()
                )
            )
            .list.drop_nulls()
            .list.first(),
            price_periodicity=pl.col("price").list[-1],
            price=pl.col("price").list[0].str.strip_chars().cast(pl.Float64),
        ).drop("fees")

        df_analysis.show(limit=20)
        print(df_analysis.to_dicts()[0])

    def get_data(self):

        today = datetime.today().strftime("%Y-%m-%d")
        data_path = f"{self.data_path}/zap_imoveis/{today}"

        paths_created = os.listdir(data_path)
        items = pl.DataFrame()

        for path in paths_created:
            data_path_crated = f"{data_path}/{path}"
            files_extracted = os.listdir(data_path_crated)
            list_extracted = [f"{data_path_crated}/{f}" for f in files_extracted]

            for parquet in list_extracted:
                state, city, district = (
                    parquet.split("/")[-1].replace(".parquet", "").split("_")
                )
                new_items = pl.read_parquet(parquet).with_columns(
                    state=pl.lit(state),
                    city=pl.lit(city),
                    district=pl.lit(district),
                    tipo_imovel=pl.lit(path),
                )
                items = pl.concat([items, new_items])
        return items
