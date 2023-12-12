import os
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection,
    utility,
    Role
)
from vines_worker_sdk.utils.files import ensure_directory_exists
from src.utils import generate_pk, generate_embedding_of_model
from src.utils.document_loader import load_documents
from src.oss import oss_client
from src.database import FileProcessProgressTable

import time

MILVUS_ADDRESS = os.environ.get('MILVUS_ADDRESS')
MILVUS_PUBLIC_ADDRESS = os.environ.get('MILVUS_PUBLIC_ADDRESS')
MILVUS_USER = os.environ.get("MILVUS_USER")
MILVUS_PASSWORD = os.environ.get("MILVUS_PASSWORD")

if not (MILVUS_ADDRESS and MILVUS_USER and MILVUS_PASSWORD):
    raise Exception("请在环境变量中配置 MILVUS_ADDRESS, MILVUS_USER 和 MILVUS_PASSWORD")

[MILVUS_HOST, MILVUS_PORT] = MILVUS_ADDRESS.split(':')
connections.connect(
    "default",
    host=MILVUS_HOST,
    port=int(MILVUS_PORT),
    user=MILVUS_USER,
    password=MILVUS_PASSWORD
)


def create_milvus_collection(role_name: str, name: str, description: str, embedding_model: str, dimension: int):
    fields = [
        FieldSchema(name="pk", dtype=DataType.VARCHAR, is_primary=True, auto_id=False, max_length=100),
        FieldSchema(name="page_content", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=dimension),
        FieldSchema(name="metadata", dtype=DataType.JSON),
    ]
    schema = CollectionSchema(fields, description)
    coll = Collection(name, schema, consistency_level="Strong")
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }
    coll.create_index("embeddings", index)

    role = Role(role_name)
    role.grant("Collection", name, "*")


def drop_milvus_collection(name):
    coll = Collection(name, consistency_level="Strong")
    coll.drop()


def create_milvus_user(role_name, username, password):
    role = Role(role_name)
    if not role.is_exist():
        role.create()
    utility.create_user(
        user=username,
        password=password
    )
    role.add_user(username)


class MilvusClient:
    def __init__(self, collection_name: str):
        self.collection = Collection(
            collection_name,
            consistency_level="Strong"
        )
        self.collection.load()
        self.output_fields = [
            'pk',
            'page_content',
            'metadata'
        ]

    def insert_vectors(self, page_contents, embeddings, metadatas):
        pks = [
            generate_pk() for _ in page_contents
        ]

        for metadata in metadatas:
            if not metadata.get('createdAt'):
                metadata['createdAt'] = int(time.time())

        res = self.collection.insert([
            pks,
            page_contents,
            embeddings,
            metadatas
        ])
        return res

    def query_vector(self, expr, limit, offset):
        return self.collection.query(
            expr=expr,
            output_fields=self.output_fields,
            limit=limit,
            offset=offset
        )

    def search_vector(self, embedding, expr, limit):
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        result = self.collection.search(
            data=[embedding],
            anns_field="embeddings",
            output_fields=self.output_fields,
            param=search_params,
            limit=limit,
            consistency_level="Strong",
            expr=expr
        )
        data = []
        for hits in result:
            for hit in hits:
                # get the value of an output field specified in the search request.
                # dynamic fields are supported, but vector fields are not supported yet.
                data.append({
                    "pk": hit.entity.get('pk'),
                    "page_content": hit.entity.get('page_content'),
                    "metadata": hit.entity.get('metadata'),
                    "score": hit.score
                })
        return data

    def delete_record(self, pk):
        expr = f"pk in ['{pk}']"
        result = self.collection.delete(expr)
        return result

    def upsert_record(self, pk, text, embedding, metadata):
        data = [
            [pk],
            [text],
            embedding,
            [metadata]
        ]
        result = self.collection.upsert(data)
        return result

    def upsert_record_batch(self, pks, texts, embeddings, metadatas):
        data = [
            pks,
            texts,
            embeddings,
            metadatas
        ]
        result = self.collection.upsert(data)
        return result

    def insert_vector_from_file(self, embedding_model, file_url, metadata, task_id):
        folder = ensure_directory_exists("./download")
        file_path = oss_client.download_file(file_url, folder)
        FileProcessProgressTable.update_progress(task_id, 0.1, "已下载文件到服务器")
        texts = load_documents(file_path)
        FileProcessProgressTable.update_progress(task_id, 0.3, "已加载文件")
        metadatas = []
        for _ in texts:
            item = {
                "source": file_url,
            }
            if metadata and isinstance(metadata, dict):
                item.update(metadata)
            metadatas.append(item)
        texts = [text.page_content for text in texts]
        embeddings = generate_embedding_of_model(embedding_model, texts)
        FileProcessProgressTable.update_progress(task_id, 0.8, "已生成向量，正在写入向量数据库")
        res = self.insert_vectors(texts, embeddings, metadatas)
        FileProcessProgressTable.update_progress(task_id, 1.0, f"完成，共写入 {res.succ_count} 条向量数据")
        return res
