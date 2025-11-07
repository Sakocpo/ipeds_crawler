from contextlib import asynccontextmanager
from typing import AsyncIterator
from playwright.async_api import async_playwright, Browser, Page


@asynccontextmanager
async def browser_page() -> AsyncIterator[Page]:
    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-gpu",
                "--disable-extensions",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-renderer-backgrounding",
            ],
        )
        page: Page = await browser.new_page()

        await page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font", "stylesheet"}
            else route.continue_(),
        )
        page.set_default_timeout(15_000)

        try:
            yield page
        finally:
            await browser.close()
