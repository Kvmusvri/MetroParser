import asyncio
import re
import pandas as pd
import aiohttp
from time import perf_counter
from asyncio import Semaphore
import csv
from selectolax.lexbor import LexborHTMLParser


# время выполнения time: 152.02
async def parse_product_char(product_link: str, semaphore: Semaphore) -> dict:
    await semaphore.acquire()
    async with aiohttp.ClientSession(trust_env=True, connector=aiohttp.TCPConnector(limit=100)) as session:
        response = await session.get(url=product_link)
        parser = LexborHTMLParser(await response.text())

        product_promo_price = await parse_promo_price(parser)
        product_usual_price = await parse_usual_price(parser)

        if not product_promo_price and not product_usual_price:
                await session.close()
                semaphore.release()
                return 0

        product_name = await parse_name(parser)
        product_id = await parse_id(parser)
        product_brand = await parse_brand(parser)

        card_char = {
            'id': product_id, # артикул
            'наименование': product_name,
            'ссылка на товар': product_link,
            'регулярная цена ₽': product_usual_price,
            'промо цена ₽': product_promo_price,
            'бренд': product_brand,
        }

    await session.close()

    semaphore.release()

    return card_char


async def parse_name(parser:LexborHTMLParser)->str:
    product_name=''

    product_name_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > h1 > span')
    if product_name_field:
        product_name = product_name_field[0].text()

    return product_name.strip()

async def parse_promo_price(parser: LexborHTMLParser) -> str:
    promo_price = ''

    promo_price_check_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--right > div.product-page-prices-and-buttons.product-page-content__desktop-prices-and-buttons > div.product-page-prices-and-buttons__prices-block > p')

    if promo_price_check_field:
        promo_price_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--right > div.product-page-prices-and-buttons.product-page-content__desktop-prices-and-buttons > div.product-page-prices-and-buttons__prices-block > div.base-tooltip.product-unit-prices.product-page-prices-and-buttons__unit-prices.bottom-right.style--product-page-major-prices.can-close-on-click-outside.style--product-page-major > button > div > div.product-unit-prices__actual-wrapper > span > span.product-price__sum > span.product-price__sum-rubles')

        if promo_price_field:
            promo_price = promo_price_field[0].text()

            promo_price_penny_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--right > div.product-page-prices-and-buttons.product-page-content__desktop-prices-and-buttons > div.product-page-prices-and-buttons__prices-block > div.base-tooltip.product-unit-prices.product-page-prices-and-buttons__unit-prices.bottom-right.style--product-page-major-prices.can-close-on-click-outside.style--product-page-major > button > div > div.product-unit-prices__actual-wrapper > span > span.product-price__sum > span.product-price__sum-penny')
            if promo_price_penny_field:
                promo_price+=promo_price_penny_field[0].text().strip()

    promo_price = re.sub('\xa0', '', promo_price)

    return promo_price.strip()


async def parse_usual_price(parser: LexborHTMLParser) -> str:
    usual_price = ''

    promo_price_check_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--right > div.product-page-prices-and-buttons.product-page-content__desktop-prices-and-buttons > div.product-page-prices-and-buttons__prices-block > p')

    if promo_price_check_field:
        usual_price_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--right > div.product-page-prices-and-buttons.product-page-content__desktop-prices-and-buttons > div.product-page-prices-and-buttons__prices-block > div.base-tooltip.product-unit-prices.product-page-prices-and-buttons__unit-prices.bottom-right.style--product-page-major-prices.can-close-on-click-outside.style--product-page-major > button > div > div.product-unit-prices__old-wrapper > span > span.product-price__sum > span.product-price__sum-rubles')
        if usual_price_field:
            usual_price = usual_price_field[0].text().strip()
            penny_price_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--right > div.product-page-prices-and-buttons.product-page-content__desktop-prices-and-buttons > div.product-page-prices-and-buttons__prices-block > div.base-tooltip.product-unit-prices.product-page-prices-and-buttons__unit-prices.bottom-right.style--product-page-major-prices.can-close-on-click-outside.style--product-page-major > button > div > div.product-unit-prices__old-wrapper > span > span.product-price__sum > span.product-price__sum-penny')
            if penny_price_field:
                usual_price += penny_price_field[0].text().strip()
    else:
        usual_price_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--right > div.product-page-prices-and-buttons.product-page-content__desktop-prices-and-buttons > div.product-page-prices-and-buttons__prices-block > div.base-tooltip.product-unit-prices.product-page-prices-and-buttons__unit-prices.bottom-right.style--product-page-major-prices.can-close-on-click-outside.style--product-page-major > button > div > div.product-unit-prices__actual-wrapper > span > span.product-price__sum > span.product-price__sum-rubles')
        if usual_price_field:
            usual_price = usual_price_field[0].text().strip()
            penny_price_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--right > div.product-page-prices-and-buttons.product-page-content__desktop-prices-and-buttons > div.product-page-prices-and-buttons__prices-block > div.base-tooltip.product-unit-prices.product-page-prices-and-buttons__unit-prices.bottom-right.style--product-page-major-prices.can-close-on-click-outside.style--product-page-major > button > div > div.product-unit-prices__actual-wrapper > span > span.product-price__sum > span.product-price__sum-penny')
            if penny_price_field:
                usual_price += penny_price_field[0].text().strip()

    usual_price = re.sub(r'\xa0', '', usual_price)

    return usual_price.strip()


async def parse_id(parser: LexborHTMLParser) -> str:
    product_id = ''

    id_field = parser.css('#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__article-and-actions > div.product-page-content__rating-and-article > p')

    if id_field:
        product_id = id_field[0].text().split(':')[-1].strip()

    return product_id


async def parse_brand(parser: LexborHTMLParser) -> str:
    product_brand = ''

    brand_field = parser.css("#__layout > div > div > div.content-page > div > div > div.product-page-content > article > div.product-page-content__columns > div.product-page-content__column.product-page-content__column--left > div.product-page-content__photos-prices-attrs > div.product-page-content__labels-and-short-attrs > div.product-attributes.product-page-content__attributes-short.style--product-page-short-list > ul > li:nth-child(3) > a")

    if brand_field:
        product_brand = brand_field[0].text().strip()

    return product_brand


def clean_csv(list_of_csv)->list:
    list_of_csv = str(list_of_csv)
    list_of_csv = re.sub(r'[\[\]\{\}\"\'\"]','', list_of_csv)

    return list_of_csv.split(',')


def excel_export(data_list: dict, table_name: str) -> None:
    df_list = []
    for d in data_list:
        df_list.append(pd.DataFrame([d]))

    combined_df = pd.concat(df_list)

    combined_df.to_excel(f'{table_name}.xlsx', index=False)


async def main()->None:
    semaphore_items = Semaphore(30)

    with open('moscow_products_links', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        moscow_list_of_csv = list(csv_reader)

    with open('spb_products_links', 'r') as read_obj:
        csv_reader = csv.reader(read_obj)
        spb_list_of_csv = list(csv_reader)

    moscow_list_of_csv = clean_csv(moscow_list_of_csv)
    spb_list_of_csv = clean_csv(spb_list_of_csv)

    parse_items_from_moscow_links_tasks = []
    for link in moscow_list_of_csv:
        parse_items_from_moscow_links_tasks.append(asyncio.create_task(parse_product_char(link, semaphore_items)))

    moscow_product_list = await asyncio.gather(*parse_items_from_moscow_links_tasks)

    parse_items_from_spb_links_tasks = []
    for link in spb_list_of_csv:
        parse_items_from_spb_links_tasks.append(asyncio.create_task(parse_product_char(link, semaphore_items)))

    spb_product_list = await asyncio.gather(*parse_items_from_spb_links_tasks)

    excel_export(moscow_product_list, 'moscow_table')
    excel_export(spb_product_list, 'spb_table')


if __name__ == '__main__':
    start = perf_counter()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    print(f"time: {(perf_counter() - start):.02f}")