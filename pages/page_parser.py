import asyncio
import functools
import os
import re
import threading
import time
from urllib.parse import urljoin

import pandas
import selenium
from bs4 import BeautifulSoup
from selenium.webdriver import Edge

from my_machine_learning.conf import DATA_PATH, PAGES_PATH

PLAYERS = []

ERRORS = []

HEADERS = ['name', 'date_of_birth',
           'height', 'weight', 'spike', 'block', 'link']

@functools.lru_cache
def create_paths(target_folder=None):
    if target_folder:
        path = os.path.join(PAGES_PATH, target_folder)
    else:
        path = PAGES_PATH
    pages = os.listdir(path)
    return [os.path.join(path, page) for page in pages]


def refixer(item:str):
    if '\n' in item:
        words = item.split('\n')
        for index, word in enumerate(words):
            words[index] = word.strip()
        item = ' '.join(words)
        return item
    return item.strip()


def table(soup, map_rows, attrs: dict={}, has_header=True, output_name=None, **kwargs):
    """
    A simple layer to parse table
    """
    if attrs:
        table = soup.find('table', attrs=attrs)
    else:
        table = soup.find('table')

    if table:
        rows = table.find_all('tr')
        if has_header:
            rows.pop(0)
        
        base_keys = ['name', 'surname', 'height', 'weight', 'spike', 'block']
        # difference = set(list(rows_map.keys())) - set(base_keys)
        # if difference:
        #     raise ValueError('You should provide base keys for mapping the rows to the table.')
            
        for row in rows:
            characteristics = row.find_all('td')

            # Try to find the link as early
            # as possible in order to retrieve
            # the name of the player in situations
            # where it is actually embedded there
            link_tag = row.find('a')
            link = link_tag['href']

            if 'full_name' in map_rows:
                row_position = map_rows['full_name']
                # if isinstance(position, int):
                #     # Get the position of the
                #     # row in order to get the
                #     # player's name
                #     pass
                full_name = refixer(link_tag.text)
            else:
                try:
                    name = characteristics[map_rows['name']].text
                except:
                    name = None

                try:
                    surname = characteristics[map_rows['surname']].text
                except:
                    surname = None

                full_name = refixer(f'{name} {surname}')

            try:
                date_of_birth = refixer(
                    characteristics[map_rows['date_of_birth']].text)
            except:
                date_of_birth = None
            else:
                if '.' in date_of_birth:
                    date_of_birth.replace('.', '/')

            try:
                height = characteristics[map_rows['height']].text
            except:
                height = None

            try:
                weight = characteristics[map_rows['weight']].text
            except:
                weight = None

            try:
                spike = characteristics[map_rows['spike']].text
            except:
                spike = None

            try:
                block = characteristics[map_rows['block']].text
            except:
                block = None
    
            player = [full_name, date_of_birth, height,
                        weight, spike, block, link]

            PLAYERS.append(player)

        print(f"""Parsed table for '{kwargs['page_name']}'""")
    else:
        print('No table to parse')


def html_page_parser(parser, map_rows:dict, table_has_header=True, 
        target_folder=None, output_name=None, attrs: dict={}):
    pages = create_paths(target_folder=target_folder)
    threads = []

    if not callable(parser):
        raise TypeError('Parser should be a callable definition.')

    for index, page in enumerate(pages):
        with open(page, 'r') as f:
            file_name = f.name
            soup = BeautifulSoup(f.read(), 'html.parser')
            threads.append(
                threading.Thread(
                    target=parser, 
                    name=f'PAGE-{index}: {file_name}',
                    args=[soup, map_rows], 
                    kwargs={
                        'output_name': output_name,
                        'attrs': attrs,
                        'has_header': table_has_header,
                        'page_name': page,
                    }
                )
            )

    for index, thread in enumerate(threads):
        thread.start()
        if thread.is_alive():
            print(f"{thread.name}")
        thread.join()

    PLAYERS.insert(0, HEADERS)
    df = pandas.DataFrame(data=PLAYERS, columns=HEADERS)
    if output_name:
        df.to_csv(output_name)
        print('Completed.')
    return df


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
        href = link.get_attribute('href')
        if '/en/volleyball' in href:
            is_match = re.search(r'\/teams\/(\w+)\-(.*)', href)
            if is_match:
                country = is_match.group(1).upper()
            else:
                country = f'NOCOUNTRY{index}'

        if 'Teams.asp' in href \
                or 'Team=' in href \
                    or '/Teams/' in href:
            is_match = re.search(r'Team\=(.*)', href)
            if is_match:
                country = is_match.group(1)
            else:
                country = f'NOCOUNTRY{index}'

        if url_suffix:
            href = f'{href}/{url_suffix}'
        list_of_urls.append((href, country))

    async def writer(output_path, html):
        with open(output_path, 'w') as f:
            try:
                f.write(html)
            except:
                return False
            print(f'Writing file to {output_path}')
            # time.sleep(1)
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


# url = 'http://www.fivb.org/EN/volleyball/competitions/WorldGrandPrix/2006/Teams/Team_Roster.asp?TEAM=AZE&TRN=WGP2006&sm=53'
# xpath = '/html/body/center/table[4]/tbody/tr[1]/td[2]/table/tbody/tr[9]/td/div[2]'

# selenium_parser(url, xpath)

# time.sleep(2)


if __name__ == "__main__":
    html_page_parser(
        table,
        {
            'name': 4,
            'surname': 5,
            'date_of_birth': 7,
            'height': 8,
            'weight': 9,
            'spike': 10,
            'block': 11
        },
        target_folder='u20_wc_2007',
        output_name='u20_world_championships_2007.csv',
        attrs={'class': 'players'},
    )
