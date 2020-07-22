#%%
import pandas
from my_machine_learning.conf import csv_explorer

# %%
file1 = csv_explorer('u20_world_championships_2007.csv')
file2 = csv_explorer('u20_world_championships_2013.csv')
file3 = csv_explorer('u20_world_championships_2019.csv')

#%%
data1 = pandas.read_csv(file1)
data2 = pandas.read_csv(file2)
data3 = pandas.read_csv(file3)
# %%
df1 = pandas.DataFrame(data1)
df2 = pandas.DataFrame(data2)
df3 = pandas.DataFrame(data3)


# %%
new_grouped_df = df1.append([df2, df3], ignore_index=True, sort=False)

# %%
new_grouped_df.shape

# %%
# new_grouped_df.duplicated()


# %%
new_grouped_df.to_csv('u20_world_championships-final.csv', index=False)

