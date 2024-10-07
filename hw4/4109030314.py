#pip install pyserini
"""python -m pyserini.index.lucene \
  --collection JsonCollection \
  --input tests/resources/sample_collection_jsonl_zh \
  --language zh \
  --index indexes/sample_collection_jsonl_zh \
  --generator DefaultLuceneDocumentGenerator \
  --threads 1 \
  --storePositions --storeDocvectors --storeRaw"""

import json
from pyserini.search.lucene import LuceneSearcher
searcher = LuceneSearcher('indexes/sample_collection_jsonl_zh')
searcher.set_language('zh')
hits = searcher.search('蝙蝠俠')

from pyserini.index.lucene import IndexReader
# Initialize from an index path:
index_reader = IndexReader('indexes/sample_collection_jsonl_zh')

term = '蝙蝠俠'

# Look up its document frequency (df) and collection frequency (cf).
# Note, we use the unanalyzed form:
df, cf = index_reader.get_term_counts(term)
print(f'term "{term}": df={df}, cf={cf}')

# Fetch and traverse postings for an unanalyzed term:
postings_list = index_reader.get_postings_list(term)
for posting in postings_list:
    print(f'docid={posting.docid}, tf={posting.tf}, pos={posting.positions}')
doc_vector = index_reader.get_document_vector('wiki_article_list_2023_tra.json')
#print(doc_vector)
term_positions = index_reader.get_term_positions('wiki_article_list_2023_tra.json')
#print(term_positions)

doc = []
for term, positions in term_positions.items():
    for p in positions:
        doc.append((term,p))

doc = ' '.join([t for t, p in sorted(doc, key=lambda x: x[1])])
#print(doc)

tf = index_reader.get_document_vector('wiki_article_list_2023_tra.json')
df = {term: (index_reader.get_term_counts(term, analyzer=None))[0] for term in tf.keys()}

query = '蝙蝠俠'
docids = ['wiki_article_list_2023_tra.json']

for i in range(0, len(docids)):
    score = index_reader.compute_query_document_score(docids[i], query)
    print(f'{i+1:2} {docids[i]:15} {score:.5f}')