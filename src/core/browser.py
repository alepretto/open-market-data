from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncGenerator

from playwright.async_api import Browser, async_playwright

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
)


@dataclass
class BrowserManager:
    headless: bool = True
    user_agent: str = DEFAULT_USER_AGENT
    slow_mo: float | None = None

    @asynccontextmanager
    async def page(self):

        args = (
            [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ]
            if self.headless
            else None
        )

        async with async_playwright() as pw:
            browser: Browser = await pw.chromium.launch(
                headless=self.headless, slow_mo=self.slow_mo, args=args
            )

            context = await browser.new_context(
                user_agent=self.user_agent, viewport={"width": 1280, "height": 800}
            )

            page = await context.new_page()

            try:
                yield page

            finally:
                await context.close()
                await browser.close()
