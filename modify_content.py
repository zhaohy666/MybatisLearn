import happybase
import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search,Q


conn = happybase.Connection("ds3", port=9090)
table = conn.table("lawgu_ds")

f=open('need_modify.csv','r')
spurce = ['lid','title']

q_getlid=Q('bool',must)
