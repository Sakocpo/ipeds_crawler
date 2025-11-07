from typing import Any, List, Sequence
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from .normalize import normalize
from ipeds_crawler.retry import retry_async

@retry_async(retries=2, delay=2)
async def wait_for_all(node, table_selector: str = "table#tbl4yearGradurate1.grid") -> bool:
    try:
        await node.locator(table_selector).first.wait_for(timeout=10_000)
        return True
    except PlaywrightTimeoutError:
        return False
    except Exception:
        return False


async def safe_all_inner_texts(locator) -> List[str]:
    try:
        return await locator.all_inner_texts()
    except PlaywrightTimeoutError:
        return []
    except Exception:
        return []


@retry_async(retries=2, delay=2)
async def get_text_data(frame,text: str,year: int,table_selector: str | None = None,value_selector: str | None = None,table_header: str = "",exact: bool = False,) -> list | float | int | None:
    if table_selector is None or value_selector is None:
        if year >= 2023:
            table_selector = table_selector or "table.sc.survey-t"
            value_selector = value_selector or "td.sc-tb-r.t-co span"
        elif 2020 <= year <= 2022:
            table_selector = table_selector or "table.grid"
            value_selector = value_selector or "td.number"
        else:
            table_selector = table_selector or "table.grid"
            value_selector = value_selector or "span"

    if not table_header:
        if exact:
            selector = f'{table_selector} tr:has(td:text-is("{text}")) {value_selector}'
        else:
            selector = f'{table_selector} tr:has-text("{text}") {value_selector}'
    else:
        if exact:
            selector = (
                f'{table_selector}:has(td:text-is("{table_header}")) '
                f'tr:has(td:text-is("{text}")) {value_selector}'
            )
        else:
            selector = (
                f'{table_selector}:has-text("{table_header}") '
                f'tr:has-text("{text}") {value_selector}'
            )

    locator = frame.locator(selector)
    try:
        value = await locator.all_inner_texts()
    except Exception:
        value = []

    value = normalize(value)

    if isinstance(value, list):
        return value
    return (value / 100.0) if "%" in text else value

@retry_async(retries=2, delay=2)
async def get_box_data(frame,text: str,year: int,table_selector: str | None = None,value_selector: str | None = None,num_box: int = 2,) -> list:
    if table_selector is None or value_selector is None:
        if year >= 2023:
            table_selector = table_selector or "table.sc.survey-t"
            value_selector = value_selector or "td.sc-tb-r.t-co span"
        elif 2020 <= year <= 2022:
            table_selector = table_selector or "table.grid"
            value_selector = value_selector or "td.number"
        else:
            table_selector = table_selector or "table.grid"
            value_selector = value_selector or "span"

    try:
        box_list = await frame.locator(f'{table_selector} tr:has-text("{text}") {value_selector}').all()
    except Exception:
        box_list = []

    value_list: list[Any] = []
    for i in range(min(num_box, len(box_list))):
        temp_value = await box_list[i].get_attribute("value")
        value_list.append(temp_value)

    return normalize(value_list)
