import os
import pandas
from functools import cached_property, lru_cache

def collect_all_files():
    items = os.walk('json')
    for item in items:
        root, _, files = item
        for file in files:
            if file.endswith('.json'):
                yield os.path.join(root, file)


def collect_dataframes(**dtypes):
    mapped_dtypes = {
        'competition_year': int,
        'player_page': str,
        'team': str,
        'player_image': str,
        'nationality': str,
        'position': str,
        'short_position': str,
        'captain': bool,
        'firstname': str,
        'lastname': str,
        'slug': str
    }
    mapped_dtypes = mapped_dtypes | dtypes
    container = map(lambda x: pandas.read_json(x, dtype=mapped_dtypes), collect_all_files())
    return list(map(lambda d: pandas.DataFrame(d), container))


def solidified_dataframes(**dtypes):
    dfs = collect_dataframes(**dtypes)
    first_df = dfs.pop(0)
    for df in dfs:
        first_df = first_df.append(df)
    return first_df


def select_file(name: str):
    if not name.endswith('json'):
        raise ValueError('Filename should end with .json')

    items = os.walk('json')
    for item in items:
        root, _, files = item
        if name in files:
            break
    return os.path.join(root, name)
