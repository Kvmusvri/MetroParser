import asyncio
import csv
import aiohttp
from time import perf_counter
from selectolax.lexbor import LexborHTMLParser
from asyncio import Semaphore


# собираем количество страниц
async def parse_num_page(catalog: str, header:dict,  semaphore: Semaphore)->int:
    # сначала получаем количество страниц
    await semaphore.acquire()
    async with aiohttp.ClientSession(headers=header) as session:
        response = await session.get(catalog)
        parser = LexborHTMLParser(await response.text())

        nums_pages_div = parser.css('#catalog-wrapper > main > div:nth-child(2) > nav > ul')
        pages_links = [node.attributes['href'] for node in nums_pages_div[0].css('li a') if 'href' in node.attributes]
        pages_links.pop()

        num_last_pages = pages_links[-1].split('page=')[-1]

        semaphore.release()

        return int(num_last_pages)

# собираем ссылки на страницы
async def parse_product_links(page_link: str, header:dict, semaphore: Semaphore)->set:
    await semaphore.acquire()
    main_link = 'https://online.metro-cc.ru'
    async with aiohttp.ClientSession(headers=header) as session:

        response = await session.get(page_link)

        parser = LexborHTMLParser(await response.text())

        products_div = parser.css('#products-inner')

        links_products = [main_link + node.attributes['href'] for node in products_div[0].css('a') if 'href' in node.attributes]

        semaphore.release()

        return set(links_products)

# генерируем каталог из собранных страниц для каждого из регионов
def create_new_catalog(pages: int):
    pages_links = []
    for num_page in range(pages):
        change_catalog = sweet_catalog.split('page=')
        change_catalog[-1] = 'page=' + str(num_page + 1)
        pages_links.append("".join(change_catalog))
    return pages_links

async def main():
    semaphore_items = Semaphore(15)
    spb_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'ru,en;q=0.9',
        'cache-control': 'max-age=0',
        'cookie': 'exp_auth=3Rd5fv0vTUylyTasdzAcqA.1; is18Confirmed=false; _slid_server=671a5fae395c338fcb095eea; pdp_abc_20=3; plp_bmpl_bage=1; _ga=GA1.1.1489987874.1729781681; _gcl_au=1.1.1486857023.1729781681; _ym_uid=1729781681425647054; _ym_d=1729781681; uxs_uid=e635bfc0-9217-11ef-8d17-6da199f2c427; _ym_isad=1; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; flocktory-uuid=f52cd8ff-239a-4e71-b461-b3bc6c5a6e94-0; _ym_visorc=b; _slfreq=633ff97b9a3f3b9e90027740%3A633ffa4c90db8d5cf00d7810%3A1729796101%3B64a81e68255733f276099da5%3A64abaf645c1afe216b0a0d38%3A1729796101; tmr_lvid=6e65273c875ea92e5cd85f65982eccd4; tmr_lvidTS=1729788951205; tmr_detect=1%7C1729788951790; isVisibleManualInputTooltip=false; _slsession=81EF4521-54D2-4BC1-9BF7-B54C0508FED1; _slid=671a5fae395c338fcb095eea; metroStoreId=15; pickupStore=15; coords=60.002202%2630.26868; mp_5e1c29b29aeb315968bbfeb763b8f699_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A192bf05cca9cfd-01366e6705c97e-367b7637-2e2d77-192bf05cca9cfd%22%2C%22%24device_id%22%3A%20%22192bf05cca9cfd-01366e6705c97e-367b7637-2e2d77-192bf05cca9cfd%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; mp_88875cfb7a649ab6e6e310368f37a563_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A192bf05cd0cd60-0be4ec1d805368-367b7637-2e2d77-192bf05cd0cd60%22%2C%22%24device_id%22%3A%20%22192bf05cd0cd60-0be4ec1d805368-367b7637-2e2d77-192bf05cd0cd60%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; at=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJwaG9uZSI6Ijc5NjQ5MDEyMDIyIiwiY2FyZGhvbGRlcklkIjoiNTMwMDMyMjE2MjAxIiwidXBkYXRlU291cmNlIjoiZmFtaWx5UFJPRCIsInR5cGUiOiJhY2Nlc3MiLCJyZWZyZXNoVG9rZW5JZCI6IjIyYzZmMGYxLWZmOWQtNGZlMy1iYThmLWZlMjljZDk5MjJiZSIsImlhdCI6MTcyOTc5MDcwOCwiZXhwIjoxNzI5NzkyNTA4LCJhdWQiOlsib3B2IiwibWFya2V0cGxhY2UiLCJtZXRyby1hdXRoLWFwcCJdLCJpc3MiOiJtZXRyby1hdXRoLWFwcCIsInN1YiI6Ijc5NjQ5MDEyMDIyXzUzMDAzMjIxNjIwMSJ9.sE_LVS6xjSa2ucR8s7pOmkVc3F4TSVpZIWV1GcSuzzFrtqB4UmZgOynRGfZy0hsz6vbP4wOehQDiiewzpQIgQk5eATL46oQzMXROyiLLqIRzwbvmDcbiGXu9W7jLaXPWwYMA916sXYFH7xCjkmvtEOnAjndzj6aAyo6bFs8RnwhiVLYjxBf9O0ljTXQtq18D6X6qYhxsXyQcMB8jQwUta9qpnrqLbkzAR5kzZA3yXY2wzBknXcfXwvXsbdVJPgtVI1OEu78A1S9IKqAIKLPg5xl8cgb41z4to3z97r_fxLgeBKYf9zOqJ3dfK4OSonIs4sB7zHfB9Aeo-pb7Htgp7Do-ZKHa1cCPLk-w17pAbqm3QRbb4Z2by0NaV6iO15DcBk1EdcJOS6XZXL4Bv6ERVxZE1LITWJDvIuWtLgXYFpWE925N0ZEiwruJyq9iWjBeUn2Ew_4zzyAI1FU4oWRy9uD29D_DufI381XlHamxgjKv3xeWldbyhCpJDNrsbNVTdwlsGo0AasHwa90MqAUiijiM8ZvSbzaY7RDAjNf5Ico2gXhDep-lZhgeG-mQiiONMgESOF80VMFF1v0Np2FZLO7-zQ745J0PTHGo5Nss_CcXU4wdystzIqonfh9M_gsfH1jarGtpZDmeDWKJkvEDLzrVISzj50x56YNU0wqnzEo; at_exp=1729792508000; rt=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjIyYzZmMGYxLWZmOWQtNGZlMy1iYThmLWZlMjljZDk5MjJiZSIsInBob25lIjoiNzk2NDkwMTIwMjIiLCJjYXJkaG9sZGVySWQiOiI1MzAwMzIyMTYyMDEiLCJ1cGRhdGVTb3VyY2UiOiJmYW1pbHlQUk9EIiwidHlwZSI6InJlZnJlc2giLCJpYXQiOjE3Mjk3OTA3MDgsImV4cCI6MTc0NTM0MjcwOCwiYXVkIjpbXSwiaXNzIjoibWV0cm8tYXV0aC1hcHAiLCJzdWIiOiI3OTY0OTAxMjAyMl81MzAwMzIyMTYyMDEifQ.a0pU4zRb_z8dVh6C7HTcucJ1gWLu2i97Hy7gI6h1X3M5P07qSGWXpCWJMOl6Uu_O_Qmc0NSq1SWYdLmDWUXAPMGrgd-DM3zX562SN4MWSRpg6ckHetplm0b0qZ2xHsPv-qvdRMh3rzOcDL2FIfd3EFiJYk1j2AEQOX8hhBclRSapSPIUk8cpNtBLCSMdgvPAWtYadlqvK-yHNTxmTdnxLP2BPGOzNcftabGeIOnmjP54Tx-ibia_9MrehXeEn-fzCvm2SYHOixb-OEKkUKxxBwcRPZU9YlRpj-5d2ylh_solz8LYQwBh8DYe--2DQ0-vt9K6hqphqesPkEfF3pPiZ8D62m1YMYveaDw_6b12fjq7dCSmfiBvSFQkBaLh_UEluWfWsz8svRBP06RSIaBi3l5sRfT-Qs2mXRyWzIk6wahX60Y8WwXE5bx_K0Lwl_kRsbZMQZaY77bYGJRypjnsOzT-vrgNnTmYM4CYxLDGEW6spAmiXhVIItsrPWXEzLWDxNOU9PIDDlmqAzISiII6tUM3IJavD_EWvDSmSXgHQaVJqUK7BDO4yqIsYgTKENrmkEz7mNXisshZnoeChru5HqIudEbKbIX1i88F8omDPv8zQm4P6KZCMarqGY41-fClAImcslWamTp969Bt399H58EXzfoU_agPNKTrGGcc-vM; rt_exp=1745342708000; mindboxDeviceUUID=ad11b17e-642e-4de0-a3d4-254b5ed958ab; directCrm-session=%7B%22deviceGuid%22%3A%22ad11b17e-642e-4de0-a3d4-254b5ed958ab%22%7D; _ga_VHKD93V3FV=GS1.1.1729788274.2.1.1729790885.0.0.0',
        'if-none-match': '"253905-a1wETv6OmcadtqKKYWI+K9Ilef4"',
        'priority': 'u=0, i',
        'referer': 'https://online.metro-cc.ru/cabinet/settings',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "YaBrowser";v="24.7", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 YaBrowser/24.7.0.0 Safari/537.36',
    }
    moscow_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'ru,en;q=0.9',
        'cache-control': 'max-age=0',
        # 'cookie': 'exp_auth=3Rd5fv0vTUylyTasdzAcqA.1; is18Confirmed=false; _slid_server=671a5fae395c338fcb095eea; pdp_abc_20=3; plp_bmpl_bage=1; _ga=GA1.1.1489987874.1729781681; _gcl_au=1.1.1486857023.1729781681; _ym_uid=1729781681425647054; _ym_d=1729781681; uxs_uid=e635bfc0-9217-11ef-8d17-6da199f2c427; _ym_isad=1; popmechanic_sbjs_migrations=popmechanic_1418474375998%3D1%7C%7C%7C1471519752600%3D1%7C%7C%7C1471519752605%3D1; flocktory-uuid=f52cd8ff-239a-4e71-b461-b3bc6c5a6e94-0; _ym_visorc=b; _slfreq=633ff97b9a3f3b9e90027740%3A633ffa4c90db8d5cf00d7810%3A1729796101%3B64a81e68255733f276099da5%3A64abaf645c1afe216b0a0d38%3A1729796101; tmr_lvid=6e65273c875ea92e5cd85f65982eccd4; tmr_lvidTS=1729788951205; tmr_detect=1%7C1729788951790; isVisibleManualInputTooltip=false; _slsession=81EF4521-54D2-4BC1-9BF7-B54C0508FED1; _slid=671a5fae395c338fcb095eea; mp_5e1c29b29aeb315968bbfeb763b8f699_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A192bf05cca9cfd-01366e6705c97e-367b7637-2e2d77-192bf05cca9cfd%22%2C%22%24device_id%22%3A%20%22192bf05cca9cfd-01366e6705c97e-367b7637-2e2d77-192bf05cca9cfd%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; mp_88875cfb7a649ab6e6e310368f37a563_mixpanel=%7B%22distinct_id%22%3A%20%22%24device%3A192bf05cd0cd60-0be4ec1d805368-367b7637-2e2d77-192bf05cd0cd60%22%2C%22%24device_id%22%3A%20%22192bf05cd0cd60-0be4ec1d805368-367b7637-2e2d77-192bf05cd0cd60%22%2C%22%24search_engine%22%3A%20%22google%22%2C%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%22%24initial_referrer%22%3A%20%22https%3A%2F%2Fwww.google.com%2F%22%2C%22%24initial_referring_domain%22%3A%20%22www.google.com%22%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%7D; mindboxDeviceUUID=ad11b17e-642e-4de0-a3d4-254b5ed958ab; directCrm-session=%7B%22deviceGuid%22%3A%22ad11b17e-642e-4de0-a3d4-254b5ed958ab%22%7D; at_exp=1729793408000; rt_exp=1745343608000; at=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJwaG9uZSI6Ijc5NjQ5MDEyMDIyIiwiY2FyZGhvbGRlcklkIjoiNTMwMDMyMjE2MjAxIiwidXBkYXRlU291cmNlIjoiZmFtaWx5UFJPRCIsInR5cGUiOiJhY2Nlc3MiLCJyZWZyZXNoVG9rZW5JZCI6Ijc5OTU1ZmEzLTAwNzctNDQxYi05NWY0LWJhNzJmN2Y4MDgxMyIsImlhdCI6MTcyOTc5MTYwOCwiZXhwIjoxNzI5NzkzNDA4LCJhdWQiOlsib3B2IiwibWFya2V0cGxhY2UiLCJtZXRyby1hdXRoLWFwcCJdLCJpc3MiOiJtZXRyby1hdXRoLWFwcCIsInN1YiI6Ijc5NjQ5MDEyMDIyXzUzMDAzMjIxNjIwMSJ9.I2La-UexEfCw7VRDPih0JAo0cEsHLdV0nJcu7sxdXUEE6Vf0Te3Am5QI4MrbKHtgF9BCccAaUOkcJpEsznhbIs4VTgb1lpw-H1iKWr0uu20BE4ms0w1pq-nwRyIjYetlS3mxFrQXED9MIXJKOltIuaWlpalrc7Nk0-O6lnJrycuWoHyCjDsCwruxptIt6ofQUhb6DrIl1bN-gsymcZTHXUW3NqpyMinH8qv1dkDmtRq8U7CzHMYp8vDkfHZil8Tr9eUAMiHZij78cLzyB652TDrV77nD3v5ATlRMk5s-KGfpMRJllznSz7mu0witrbM_2Ki0iiKV1VQaAmXO9ZdcyUutaZnReFoZ1xy6bG-ix6g11tfLh0t3_Jptngb0s0XjgymnVK_0qJfgglURBwkoqrXauMdcqvTWYLnjgJYR6bxe1VCyuomLMqpf-BYDzl1PmBl5ireVSxsSPVJpOebkeFDrCgo2KxloDOGoAsikaTCHAOTJ9s9qkk55ayrBE2QGKU9hfLHyKc8fb6I_iaA70m3C-_yEmMWoEEzjwUL5ssbdJ7VvC3LsTBWlZ-Z7jkZxI0QpJiI3tQCPcMsOF6Vt8jVAH_JbmVofQ_UZ2wlEmwZF69AZaOq8GLRpY0YHWOfzaMjZFmUXPCIvNnCIaOYYv47WyOVs8Da9McOM-b5DuvY; rt=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6Ijc5OTU1ZmEzLTAwNzctNDQxYi05NWY0LWJhNzJmN2Y4MDgxMyIsInBob25lIjoiNzk2NDkwMTIwMjIiLCJjYXJkaG9sZGVySWQiOiI1MzAwMzIyMTYyMDEiLCJ1cGRhdGVTb3VyY2UiOiJmYW1pbHlQUk9EIiwidHlwZSI6InJlZnJlc2giLCJpYXQiOjE3Mjk3OTE2MDgsImV4cCI6MTc0NTM0MzYwOCwiYXVkIjpbXSwiaXNzIjoibWV0cm8tYXV0aC1hcHAiLCJzdWIiOiI3OTY0OTAxMjAyMl81MzAwMzIyMTYyMDEifQ.QAX_IgdgQ1DM7Ibncr764ZaTasmrX7phAgDC3skztQ-_X1jQ07rmiCaHhywL3awgLZAjreHrbyNYJg4ltor3wBJa9qXgcY0tPr6F8i9qMfKYDH1W1DymffW1h-0SOC8QZqItd9mDEteJ1WnqJtHYkpOlanFq1Q541seJqDd8ouRx8fg5Ajs50a3kg6iR14HgQfaXFuk7yDSIjDi-ln4FaOlok2QrC-joZjzPt_iBgPuAY2qWKd1r5NXSQWeonlqgTyqHMbPt87Z8CDDRlbMU7X3XO2BPMlcyMZO_-hPQ7sMYZta8FCeVZgDxBYOCHaQTyS6Ad1WR_7Z_MeExFPL_kDtt6Z4DhvqNVrM3bbkYSn_94C53yJ1dGOi23GT6o5qJ-0fpA-Vzv7avNSjP2ogqrjEFtk4XxlJb7KIxuDRJTg-10NWIsTTiSV0tbTMd4CES0V1rKATYwvHoYiONe19y28rPfQtnvXXovnd0F9Ri2gdapfU8u8pMn8oqbWWezoyT8CiwVzo4WKUcbWfAfS7zKmLN2yRyrn57AeVMi2fcoysSoeygkoSGAUn3ZsHnA9OLectYKEQ-s65Dv0P8Vl606MZ8axMBLBSHE71nnoLmKohEnUeoQL7_FiwHk4mRwh0ZOheQGDPY85eJvlfUEIQuRCKWG6uepkgJo7hha2c2r8k; metroStoreId=16; pickupStore=16; coords=59.94083006417833%2630.45617599999998; _ga_VHKD93V3FV=GS1.1.1729788274.2.1.1729791671.0.0.0',
        'if-none-match': '"25391c-C/ySe8zyrwWochb+ywsDQ3DucsE"',
        'priority': 'u=0, i',
        'referer': 'https://online.metro-cc.ru/',
        'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "YaBrowser";v="24.7", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 YaBrowser/24.7.0.0 Safari/537.36',
    }
    list_headers = [moscow_headers,spb_headers]

    sweet_catalog = "https://online.metro-cc.ru/category/sladosti_/konfety-podarochnye-nabory?from=under_search&page=1"
    parse_tasks=[]
    for header in list_headers:
        parse_tasks.append(asyncio.create_task(parse_num_page(sweet_catalog, header, semaphore_items)))

    pages = await asyncio.gather(*parse_tasks)

    pages_moscow = []
    pages_spb = []
    if pages[0]==pages[1]:
        pages_links = create_new_catalog(int(pages[0]))

        pages_moscow = pages_links
        pages_spb = pages_links
    else:
       pages_moscow = create_new_catalog(pages[0])
       pages_spb = create_new_catalog(pages[1])


    parse_moscow_product_tasks = []
    for page_link in pages_moscow:
        parse_moscow_product_tasks.append(asyncio.create_task(parse_product_links(page_link,
                                                                                  list_headers[0],
                                                                                  semaphore_items)))
    parse_spb_product_tasks = []
    for page_link in pages_spb:
        parse_spb_product_tasks.append(asyncio.create_task(parse_product_links(page_link,
                                                                           list_headers[1],
                                                                           semaphore_items)))

    moscow_products_links = await asyncio.gather(*parse_moscow_product_tasks)
    sbp_products_links = await asyncio.gather(*parse_spb_product_tasks)

    with open("moscow_products_links", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([moscow_products_links])

    with open("spb_products_links", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([sbp_products_links])


if __name__=="__main__":
    start = perf_counter()
    asyncio.run(main())
    print(f"time: {(perf_counter() - start):.02f}")