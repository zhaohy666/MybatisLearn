from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
import csv

source = ['lid', 'title', 'time_limited', 'effective_date']

# 征求意见稿）or 征求意见稿 or 草案）or 草案

q_draft = Q('bool', must=[Q("regexp", title_term=".*(征求意见稿|草案)）?")],
                   must_not=[Q("match", time_limited="征求意见稿或草案")])

q_draft_2 = Q('bool', must=[Q("regexp", title_term=".*(征求意见稿|草案)）?")])

es = Elasticsearch(hosts="nes1:9206")
s = Search(using=es, index="law_regu_dev", doc_type="law_regu").source(source).query(q_draft)
with open('q_draft.csv', 'a') as f:
    w = csv.DictWriter(f, source)
    w.writeheader()
    for hit in s.scan():
        w.writerow(hit.to_dict())
