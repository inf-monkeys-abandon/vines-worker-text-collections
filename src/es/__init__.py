from elasticsearch import Elasticsearch, helpers
import os
import traceback

ELASTICSEARCH_URL = os.environ.get("ELASTICSEARCH_URL")
ELASTICSEARCH_USERNAME = os.environ.get("ELASTICSEARCH_USERNAME")
ELASTICSEARCH_PASSWORD = os.environ.get("ELASTICSEARCH_PASSWORD")

# 连接到 Elasticsearch
es = Elasticsearch(
    ELASTICSEARCH_URL,
    http_auth=(
        ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
    if ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD
    else None
)


def get_index_name(app_id, index_name):
    return (app_id + "-" + index_name).lower()


def insert_es_batch(app_id, index_name, documents):
    try:
        # 准备批量数据
        documents = [
            {
                "_index": get_index_name(app_id, index_name),
                "_type": "doc",
                "_id": document['_id'],
                "_source": document['_source']
            } for document in documents
        ]
        # 执行批量操作
        return helpers.bulk(es, documents)
    except Exception as e:
        traceback.print_exc()


def delete_es_document(app_id, index_name, pk):
    index_name = get_index_name(app_id, index_name)
    res = es.delete(
        index=index_name,
        id=pk
    )
    return res


def search_records(app_id, index_name, query, expr=None, size=10):
    must_statements = [
        {
            "match": {
                "page_content": query
            }
        }
    ]
    if expr:
        must_statements.append(expr)

    index_name = get_index_name(app_id, index_name)
    response = es.search(
        index=index_name,
        query={
            "bool": {
                "must": must_statements
            }
        },
        size=size
    )

    result = []
    for hit in response['hits']['hits']:
        result.append(hit['_source'])
    return result
