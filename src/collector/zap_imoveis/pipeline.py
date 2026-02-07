import asyncio
from dataclasses import dataclass
from datetime import datetime

from src.collector.zap_imoveis.scraper import ZapScraper

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

        items = [
            {
                "state": "SP",
                "city": "São Paulo",
                "district": "Moema",
                "tipo_imovel": "CASA",
            },
            {
                "state": "SP",
                "city": "São Paulo",
                "district": "Itaim Bibi",
                "tipo_imovel": "CASA",
            },
        ]

        tasks = [self.run_scraper(**item) for item in items]

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

        await scraper.execute()
