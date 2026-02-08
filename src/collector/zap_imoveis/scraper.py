import asyncio
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from playwright.async_api import Locator, Page

from src.collector.zap_imoveis.parser import parse_card
from src.core.browser import BrowserManager

INPUT_SELECT = 'input[class="olx-core-input-textarea-element olx-core-input-textarea-element--default decoration-none [&::-webkit-search-cancel-button]:hidden"]'
DIV_AUTOCOMPLETE = 'div[data-cy="autocomplete-dialog"]'
BTN_SEARCH = 'button[data-cy="rp-search-btn"]'
BTN_OPTION = 'button[data-cy="autocomplete-item"]'
LI_OPTION = 'button[data-cy="autocomplete-item"]'
OPTION = 'button[data-cy="autocomplete-item"], a[data-cy="autocomplete-item"]'


@dataclass
class ZapScraper:
    url_zap: str
    data_path: str
    state: str
    city: str
    district: str
    headless: bool = False
    struct_data: pl.DataFrame = field(default_factory=pl.DataFrame)

    async def execute(self):

        browser = BrowserManager(headless=self.headless)
        async with browser.page() as page:
            resp = await page.goto(self.url_zap, wait_until="load", timeout=60000)
            if resp is None or resp.status >= 400:
                return

            not_found = await self._select_filter(page)
            if not_found:
                return

            await self._get_ads_district(page)

            self._save_data()

    async def _select_filter(self, page: Page):
        """
        Seleciona o filtro do distrito (quase o bairro).
        Retorna a flag "not_found"
        """
        await page.wait_for_selector(INPUT_SELECT)
        input_el = page.locator(INPUT_SELECT)

        await input_el.click()
        await input_el.fill(f"{self.district}, {self.city} - {self.state}")

        await page.wait_for_selector(DIV_AUTOCOMPLETE)
        await asyncio.sleep(2)
        dialog = page.locator(DIV_AUTOCOMPLETE)
        not_found = dialog.locator(
            "span", has_text="Não encontramos nenhuma localização"
        )
        if await not_found.count() > 0:
            return True

        await page.wait_for_selector(OPTION)
        btn_option = page.locator(BTN_OPTION)
        if await btn_option.count() == 0:
            return True

        await btn_option.first.click()

        await asyncio.sleep(2)
        return False

    async def _get_ads_district(self, page: Page):
        """
        Percorre as pagínas com a lista de anuncions e extrai as informações de todos os cards
        """

        page_number = 1
        while True:
            await page.wait_for_selector(
                'li[data-cy="rp-property-cd"], img[alt="Ilustração resultado vazio"]',
                timeout=5000,
            )
            img = page.locator('img[alt="Ilustração resultado vazio"]')
            if await img.count() > 0:
                break

            cards = page.locator('li[data-cy="rp-property-cd"]')
            n_cards = await cards.count()

            if not n_cards:
                break

            await self._get_ads_page(cards, n_cards)

            page_number += 1
            next_link = page.get_by_role(
                "link", name=f"página {page_number}", exact=True
            )

            if await next_link.count() == 0:
                break  # não tem próxima página

            # clique + espera navegação real
            await next_link.click()
            await page.wait_for_load_state("domcontentloaded")

    async def _get_ads_page(self, cards: Locator, n_cards: int):
        """
        Extrai as informações de todos os cards em uma página
        """

        data = []

        for idx in range(n_cards):
            card = cards.nth(idx)

            card_data = await parse_card(card)
            data.append(card_data.copy())

        await self._append_data(data)

    async def _append_data(self, data: list):
        """
        Transaforma em DataFrame a appenda as informações
        """

        df_data = pl.DataFrame(data)

        df_data = df_data.with_columns(
            id_imovel=pl.col("href").str.split("/").list[-2].str.split("id-").list[-1]
        )

        self.struct_data = pl.concat([self.struct_data, df_data])

    def _save_data(self):
        Path(self.data_path).mkdir(parents=True, exist_ok=True)
        path = f"{self.data_path}/{self.state}_{self.city}_{self.district}.parquet"
        print("Salvo no Path: ", path)
        self.struct_data.write_parquet(path)
