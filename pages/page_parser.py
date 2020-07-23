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

from volleyball.conf import DATA_PATH, PAGES_PATH

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
            if link_tag:
                link = link_tag['href']
            else:
                link = None

            if 'full_name' in map_rows:
                row_position = map_rows['full_name']
                full_name = refixer(link_tag.text)
            else:
                try:
                    name = refixer(characteristics[map_rows['name']].text)
                except:
                    name = None

                try:
                    surname = refixer(characteristics[map_rows['surname']].text)
                except:
                    surname = None

                full_name = refixer(f'{name} {surname}')

            try:
                date_of_birth = refixer(characteristics[map_rows['date_of_birth']].text)
            except:
                date_of_birth = None
            else:
                if '.' in date_of_birth:
                    date_of_birth.replace('.', '/')

            try:
                height = refixer(characteristics[map_rows['height']].text)
            except:
                height = None

            try:
                weight = refixer(characteristics[map_rows['weight']].text)
            except:
                weight = None

            try:
                spike = refixer(characteristics[map_rows['spike']].text)
            except:
                spike = None

            try:
                block = refixer(characteristics[map_rows['block']].text)
            except:
                block = None
    
            player = [full_name, date_of_birth, height,
                        weight, spike, block, link]

            PLAYERS.append(player)

        print(f"""Parsed table for '{kwargs['page_name']}'""")
    else:
        print('No table to parse')


def html_page_parser(parser, map_rows:dict, table_has_header=True, 
        target_folder=None, output_name=None, exclude=[], attrs: dict={}):
    pages = create_paths(target_folder=target_folder)
    threads = []

    if not callable(parser):
        raise TypeError('Parser should be a callable definition.')

    for index, page in enumerate(pages):
        file_name = os.path.basename(page)
        country, _ = file_name.split('.')

        if file_name not in exclude:
            with open(page, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f, 'html.parser')
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


if __name__ == "__main__":
    html_page_parser(
        table,
        {
            'name': 1,
            'surname': 2,
            # 'full_name': 1,
            'date_of_birth': 4,
            'height': 5,
            'weight': 6,
            'spike': 7,
            'block': 8
        },
        target_folder='wgp_2013',
        output_name='wgp_2013.csv',
    )
