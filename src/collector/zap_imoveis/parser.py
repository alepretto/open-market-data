from playwright.async_api import Locator


async def _get_text(card: Locator, selector: str):
    loc = card.locator(selector)
    if await loc.count() == 0:
        return None
    return (await loc.first.inner_text()).strip()


async def parse_card(card: Locator):

    a_html = card.locator("a").first
    href = await a_html.get_attribute("href")

    location = await _get_text(card, '[data-cy="rp-cardProperty-location-txt"]')
    street = await _get_text(card, '[data-cy="rp-cardProperty-street-txt"]')

    area = await _get_text(card, '[data-cy="rp-cardProperty-propertyArea-txt"]')
    beds = await _get_text(card, '[data-cy="rp-cardProperty-bedroomQuantity-txt"]')
    baths = await _get_text(card, '[data-cy="rp-cardProperty-bathroomQuantity-txt"]')
    parking = await _get_text(
        card, '[data-cy="rp-cardProperty-parkingSpacesQuantity-txt"]'
    )

    price_block = card.locator('[data-cy="rp-cardProperty-price-txt"]')
    price = None
    fees = None

    if await price_block.count():
        ps = price_block.first.locator("p")
        if await ps.count() >= 1:
            price = (await ps.nth(0).inner_text()).strip()
        if await ps.count() >= 2:
            fees = (await ps.nth(1).inner_text()).strip()

    return {
        "href": href,
        "location": location,
        "street": street,
        "area": area,
        "bedrooms": beds,
        "bathrooms": baths,
        "parking": parking,
        "price": price,
        "fees": fees,
    }
