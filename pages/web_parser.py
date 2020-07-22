import asyncio
import os
import re
import time

import selenium
from bs4 import BeautifulSoup
from selenium.webdriver import Edge

from volleyball.conf import DATA_PATH, PAGES_PATH


def selenium_parser(url, xpath, url_suffix=None):
    """
    Selenium pages by retrieving the links to each team roster
    in the menu dropdown
    """
    # driver = Edge(executable_path='C:\\Users\\Pende\\Documents\\edge_driver\\msedgedriver.exe')
    driver = Edge(executable_path=os.environ.get('EDGE_DRIVER'))
    driver.get(url)

    time.sleep(2)

    nav_ul = driver.find_element_by_xpath(xpath)
    links = nav_ul.find_elements_by_tag_name('a')

    list_of_urls = []

    for index, link in enumerate(links):
        country = f'NOCOUNTRY{index}'
        href = link.get_attribute('href')

        if '/en/volleyball' in href:
            is_match = re.search(r'\/teams\/(\w+)\-(.*)', href)
            if is_match:
                country = is_match.group(1).upper()

        if 'Teams.asp' in href \
                or 'Team=' in href \
                    or '/Teams/' in href:
            is_match = re.search(r'Team\=(.*)', href)
            if is_match:
                country = is_match.group(1)

        if '/competions/teams/' in href:
            is_match = re.search(r'teams\/(\w+)(?=\-|\s+)', href)
            if is_match:
                country = is_match.group(1).capitalize()

        if url_suffix:
            href = f'{href}/{url_suffix}'
        list_of_urls.append((href, country))

    list_of_urls = list(set(list_of_urls))

    async def writer(output_path, html):
        with open(output_path, 'w') as f:
            try:
                f.write(html)
            except:
                return False
            print(f'Writing file to {output_path}')
            asyncio.sleep(1)

    async def main(output_path, html):
        return await writer(output_path, html)

    for list_of_url in list_of_urls:
        driver.get(list_of_url[0])
        # We just retrieve the body for
        # simplification instead of taking
        # the full HTML tag
        body = driver.find_element_by_tag_name('body')

        output_path = os.path.join(PAGES_PATH, 'temp', f'{list_of_url[1]}.html')
        html = body.get_attribute('innerHTML')

        asyncio.run(main(output_path, html))


if __name__ == "__main__":
    url = 'http://worldgrandprix.2017.fivb.com/en/group1/competition/teams/bel%20belgium/team_roster'
    xpath = '/html/body/form/div[5]/article/div/div[1]/ul'
    selenium_parser(url, xpath, url_suffix='team_roster')
