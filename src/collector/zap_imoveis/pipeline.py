import asyncio
import os
from dataclasses import dataclass
from datetime import datetime

import polars as pl

from src.collector.zap_imoveis.scraper import ZapScraper
from src.core import geo_catalog

URLS_ZAP = {
    "STUDIO": "https://www.zapimoveis.com.br/aluguel/studio/?transacao=aluguel&tipos=studio_residencial",
    "APARTAMENTO": "https://www.zapimoveis.com.br/aluguel/apartamentos/?transacao=aluguel&tipos=apartamento_residencial",
    "KITNET": "https://www.zapimoveis.com.br/aluguel/quitinetes/?transacao=aluguel&tipos=kitnet_residencial",
    "CASA": "https://www.zapimoveis.com.br/aluguel/casas/?transacao=aluguel&tipos=casa_residencial",
    "SOBRADO": "https://www.zapimoveis.com.br/aluguel/sobrados/?transacao=aluguel&tipos=sobrado_residencial",
    "CASA_CONDOMINIO": "https://www.zapimoveis.com.br/aluguel/casas-de-condominio/?transacao=aluguel&tipos=condominio_residencial",
    "CASA_VILA": "https://www.zapimoveis.com.br/aluguel/casas-de-vila/?transacao=aluguel&tipos=casa-vila_residencial",
    "COBERTURA": "https://www.zapimoveis.com.br/aluguel/cobertura/?transacao=aluguel&tipos=cobertura_residencial",
    "FLAT": "https://www.zapimoveis.com.br/aluguel/flat/?transacao=aluguel&tipos=flat_residencial",
    "LOFT": "https://www.zapimoveis.com.br/aluguel/loft/?transacao=aluguel&tipos=loft_residencial",
    "TERRENO": "https://www.zapimoveis.com.br/aluguel/terrenos-lotes-condominios/?transacao=aluguel&tipos=lote-terreno_residencial",
    "FAZENDA": "https://www.zapimoveis.com.br/aluguel/fazendas-sitios-chacaras/?transacao=aluguel&tipos=granja_residencial",
}


@dataclass
class ZapPipe:
    data_path: str

    async def run(self):

        items = self.get_items()

        sem = asyncio.Semaphore(3)

        async def safe_run(item):
            async with sem:
                await self.run_scraper(**item)
                await asyncio.sleep(1)

        tasks = [safe_run(item) for item in items]
        await asyncio.gather(*tasks)

    async def run_scraper(self, tipo_imovel: str, state: str, city: str, district: str):
        today = datetime.today().strftime("%Y-%m-%d")

        data_path = f"{self.data_path}/zap_imoveis/{today}/{tipo_imovel.lower()}"

        scraper = ZapScraper(
            url_zap=URLS_ZAP[tipo_imovel.upper()],
            data_path=data_path,
            state=state,
            city=city,
            district=district,
        )
        print(f"Iniciando Scraper: {state} - {city} - {district} - {tipo_imovel}")
        await scraper.execute()

    def get_items(self):
        """
        Entrega a lista de items já filtrada pelos items já extraídos hoje
        """
        df_districts = geo_catalog.get_districts()

        df_districts = df_districts.filter(
            (pl.col("state") == "SP") & (pl.col("city") == "São Paulo")
        )

        df_districts = self.add_tipo_imovel(df_districts)
        df_exclude = self.get_df_scraped_today()

        return df_districts.join(
            df_exclude, on=["state", "city", "district", "tipo_imovel"], how="anti"
        ).to_dicts()

    @staticmethod
    def add_tipo_imovel(df_items: pl.DataFrame):

        df_tipo_imovel = pl.DataFrame({"tipo_imovel": list(URLS_ZAP.keys())})

        df_items = df_items.join(df_tipo_imovel, how="cross")

        return df_items

    def get_df_scraped_today(self):

        today = datetime.today().strftime("%Y-%m-%d")
        data_path = f"{self.data_path}/zap_imoveis/{today}"

        paths_created = os.listdir(data_path)

        items_to_exclude = []

        for path in paths_created:
            data_path_crated = f"{data_path}/{path}"
            files_extracted = os.listdir(data_path_crated)
            files_extracted = [f.split(".")[0].split("_") for f in files_extracted]

            files_extracted = [
                {
                    "state": f[0],
                    "city": f[1],
                    "district": f[2],
                    "tipo_imovel": path.upper(),
                }
                for f in files_extracted
            ]

            items_to_exclude.extend(files_extracted)

        schema = {
            "state": pl.Utf8,
            "city": pl.Utf8,
            "district": pl.Utf8,
            "tipo_imovel": pl.Utf8,
        }

        df_exclude = pl.DataFrame(items_to_exclude, schema)
        return df_exclude
