import asyncio
import re
import threading
from urllib.parse import urljoin

import pandas
import requests
from bs4 import BeautifulSoup

from volleyball.conf import csv_explorer

FILE = csv_explorer('wgp_2013_copy.csv')

class Profile:
    positions = []
    def __init__(self, url, request_range:list=[0, 20], write_to_file=False, proxy=None):
        data = pandas.read_csv(FILE)
        self.df = pandas.DataFrame(data)

        spliced_df = self.df[request_range[0]:request_range[1]]
        # spliced_df = spliced_df[spliced_df['position'] == 0]
        
        links = [
            [
                spliced_df['name'][i], 
                self._construct_link(url, spliced_df['link'][i])
            ] 
            for i in range(request_range[0], request_range[1])
        ]

        if links:
            self._create_threads(links, proxy=proxy)
            
            if self.positions:
                for item in self.positions:
                    self.df.loc[self.df.name == item[0], 'position'] = item[1]

                if write_to_file:
                    self.df.to_csv('new_file.csv', index=False, index_label='id')
        else:
            print('No links to work with')

    @classmethod
    def _get(cls, url, player_name, proxy=None):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'
        }

        if proxy:
            proxies = {'http': proxy}
        else:
            proxies = None

        try:
            response = requests.get(
                url,
                headers=headers
            )
        except:
            cls.positions.append([player_name, 0])
        else:
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                cls.positions.append([player_name, cls._parse_soup(soup)])
            else:
                cls.positions.append([player_name, 0])
        
    def _create_threads(self, urls, proxy=None):
        threads = [
            threading.Thread(
                target=self._get, 
                name=url[0],
                args=[url[1], url[0]],
                kwargs={'proxy': proxy}
            )
            for url in urls
        ]

        for thread in threads:
            thread.start()
            if thread.is_alive():
                print(f'Request for -- {thread.name}')
            thread.join()
    
    @staticmethod
    def _parse_soup(soup):
        positions = {
            'Middle-blocker': 3,
            'Wing-spiker': 2,
            'Universal': 2,
            'Libero': 6,
            'Setter': 1,
            'Unknown': 0
        }

        for position in positions.keys():
            html_text = soup.text.strip()
            is_match = re.search(rf'{position}', html_text)
            if is_match:
                break
            else:
                position = '0'
        return position

    @staticmethod
    def _construct_link(url, path):
        return urljoin(url, path)



url = 'http://www.fivb.org/EN/volleyball/competitions/WorldGrandPrix/2013/'
profile = Profile(url, request_range=[329, 350], proxy='14.207.70.90', write_to_file=True)
