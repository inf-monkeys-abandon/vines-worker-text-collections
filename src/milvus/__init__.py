import os
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection,
)
from ..utils import generate_pk

MILVUS_ADDRESS = os.environ.get('MILVUS_ADDRESS')
if not MILVUS_ADDRESS:
    raise Exception("请在环境变量中配置 MILVUS_ADDRESS")

[MILVUS_HOST, MILVUS_PORT] = MILVUS_ADDRESS.split(':')
connections.connect("default", host=MILVUS_HOST, port=int(MILVUS_PORT))


def create_milvus_collection(name: str, description: str, dimension: int):
    fields = [
        FieldSchema(name="pk", dtype=DataType.VARCHAR, is_primary=True, auto_id=False, max_length=100),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=dimension)
    ]
    schema = CollectionSchema(fields, description or "")
    coll = Collection(name, schema, consistency_level="Strong")
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }
    coll.create_index("embeddings", index)


class MilvusClient:
    def __init__(self, collection_name: str):
        self.collection = Collection(
            collection_name,
            consistency_level="Strong"
        )
        self.collection.load()

    def insert_vectors(self, texts, embeddings):
        pks = [
            generate_pk() for text in texts
        ]
        res = self.collection.insert([
            pks,
            texts,
            embeddings
        ])
        return res

    def query_vector(self, expr, output_fields, limit, offset):
        return self.collection.query(
            expr=expr,
            output_fields=output_fields,
            limit=limit,
            offset=offset
        )

    def search_vector(self, embedding, limit):
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        result = self.collection.search(
            data=[embedding],
            anns_field="embeddings",
            output_fields=['pk', 'text'],
            param=search_params,
            limit=limit,
            consistency_level="Strong"
        )
        data = []
        for hits in result:
            for hit in hits:
                # get the value of an output field specified in the search request.
                # dynamic fields are supported, but vector fields are not supported yet.
                data.append({
                    "pk": hit.entity.get('pk'),
                    "text": hit.entity.get('text')
                })
        return data

    def delete_record(self, pk):
        expr = f"pk in ['{pk}']"
        result = self.collection.delete(expr)
        return result

    def upsert_record(self, pk, text, embedding):
        data = [
            [pk],
            [text],
            embedding
        ]
        result = self.collection.upsert(data)
