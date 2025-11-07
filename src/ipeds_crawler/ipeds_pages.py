from typing import Union
from playwright.async_api import Page, Frame, TimeoutError as PlaywrightTimeoutError

Node = Union[Page, Frame]


async def wait_frame_ready(frame: Node, timeout: int = 3_000) -> str | None:
    anchors = [
        "table.sc.survey-t input.sc-tbn",
        "div.facsimile_assets",
    ]
    for sel in anchors:
        try:
            await frame.locator(sel).first.wait_for(timeout=timeout, state="attached")
            return sel
        except PlaywrightTimeoutError:
            pass
    return None


async def goto_reported_data(page: Page, unit_id: str | int, survey_num: int, year: int) -> Node:
    iframe_url = (
        f"https://nces.ed.gov/ipeds/reported-data/html/{unit_id}"
        f"?year={year}&surveyNumber={survey_num}&viewMode=iframe"
    )
    shell_url = f"https://nces.ed.gov/ipeds/reported-data/{unit_id}?year={year}&surveyNumber={survey_num}"
    try:
        await page.goto(iframe_url, wait_until="domcontentloaded", timeout=15_000)
        return page
    except Exception:
        await page.goto(shell_url, wait_until="domcontentloaded", timeout=30_000)
        iframe_el = await page.query_selector("iframe[src*='viewMode=iframe']")
        if not iframe_el:
            raise RuntimeError("Embedded iframe not found on shell page.")
        frame = await iframe_el.content_frame()
        if not frame:
            raise RuntimeError("iframe content frame not available.")
        return frame
