from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search

client = Elasticsearch(hosts="ds1:9205")
law_regu_dev = 'law_regu_dev'
law_regu_test = 'law_regu_test'
law_regu_pre = 'law_regu_pre'
law_duplicate = 'law_duplicate'
law_regu_run = 'law_regu_run'
law_regu_inner = 'law_regu_inner'
law_regu_pro = 'law_regu_pro'
law_regu_ds = 'law_regu_ds'
case_dev = 'judgementsearch_dev'
case_test = 'judgementsearch_test'


def filter_index(key=""):
    for index in client.cat.indices():
        name = index['index']
        if key in name:
            print(name)


def list_alias():
    for alias in client.cat.aliases():
        print(alias)


def alias_index(alias, index):
    for a in client.cat.aliases():
        if a['alias'] == alias:
            client.indices.delete_alias(index=a['index'], name=alias)
    client.indices.put_alias(index=index, name=alias)


# target = "law_0524"
# target = "law_0623_law_extract"
target = "law_0707"
# target = "law_0623_law_extract"
# target = "law_regu_ds_0528_analyze"
# target = "law_0626_147_with_link"


if __name__ == '__main__':
    alias_index(law_regu_dev, target)

    # alias_index(law_regu_test, target)
    # alias_index(law_regu_dev, target)
    # alias_index(law_regu_test, target)
    # alias_index(law_regu_run, target)
    # alias_index(law_duplicate, 'law_0425')
    # list_alias()
