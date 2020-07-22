#%%
import pandas
from volleyball.conf import csv_explorer

# %%

files = [
    csv_explorer('wgp_2009'),
    csv_explorer('wgp_2010'),
    csv_explorer('wgp_2011'),
    csv_explorer('wgp_2012'),
    csv_explorer('wgp_2013'),
]


# %%

# Create all the dataframes

dfs = []
columns = ['name', 'date_of_birth', 'height', 'weight', 'spike', 'block']

for item in files:
    dfs.append(pandas.DataFrame(data=pandas.read_csv(item, index_col='id'), columns=columns))

print(f'{len(dfs)} dataframes to append')

# %%

df = pandas.concat(dfs)


# %%
df

# %%
df.to_csv('world_grand_prix-final.csv')


# %%
