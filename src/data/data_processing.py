import itertools
import pandas as pd
import json


# Create a pandas dataframe from a csv file with scraped data
def create_df_from_scraped_profiles(csvpath):
  df = pd.read_csv(csvpath)
  comps = get_companies(df)
  cdf = pd.DataFrame(comps, columns = ['current', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6'])
  final = pd.concat([df.drop(['comp'], axis=1), cdf], axis=1)
  return final

# flatten list 
def flatten_list(nested_list):
    return list(itertools.chain(*nested_list))

# convert json webscraper logs json dump into list of lists
def get_companies(data, column_name='comp', reverse_sequence=False):
  c = []
  for v in data[column_name]:
    j = json.loads(v)
    p = list(filter(lambda x: len(x) > 0, [c['comp-alt'].replace(" logo", "") for c in j]))
    if reverse_sequence:
      p = p[::-1]
    if len(p) > 0:
      c.append(p)
  return c
