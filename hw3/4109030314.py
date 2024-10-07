import pandas as pd
import os

data = []

with open('Pubmed 220 Train 2022-01-21.txt', 'r') as f:
  origin = f.read()
  ori_str = origin.strip().split('\n\n')

  for i in ori_str:
    # Remove leading "###" from article ID
    lines = i.strip().split('\n')
    id = lines[0][3:]
    # Remove first title from each line and combine content
    combine = lines[1:]
    content = []

    for line in combine:
        index = line.find('\t')
        if index != -1:
            pline = line[index+1:]
        else:
            pline = line
        
        content.append(pline)
    
    processed = ' '.join(content)

    # Create a dictionary for each article
    article_dict = {'id': id, 'contents': processed}
    data.append(article_dict)

# Create DataFrame from list of dictionaries
df = pd.DataFrame(data)

# Write DataFrame to CSV file
df.to_csv('processed_data.csv', index=False)

print(f"Successfully converted Pubmed data to CSV file")


#refer:  https://github.com/castorini/pyserini#how-do-i-index-and-search-my-own-documents
import time
from pyserini.search.lucene import LuceneSearcher
searcher = LuceneSearcher('index')

#invert
start = time.time()
hits = searcher.search('cancer', k=200000)
end = time.time()
cost = end - start
print("invered : %f sec" % (cost))

#brutal
query = []
start = time.time()
with open('processed_data.csv', 'r') as f:
    origin = f.read()
    for i in data:
        if 'cancer' in i['contents']:
            query.append(data)
end = time.time()
cost = end - start
print("Brute: %f sec" % (cost))