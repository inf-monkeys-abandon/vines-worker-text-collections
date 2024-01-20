import os
from pymilvus import (
    connections,
    FieldSchema, CollectionSchema, DataType,
    Collection,
    utility,
    Role
)
from vines_worker_sdk.utils.files import ensure_directory_exists
from src.utils import generate_embedding_of_model, generate_md5
from src.utils.document_loader import load_documents
from src.oss import oss_client
from src.database import FileProcessProgressTable, FileRecord
from src.es import insert_es_batch, delete_es_document

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


def calculate_max_m(dim):
    for m in range(dim, 0, -1):
        if dim % m == 0 and m != dim:
            return m


def create_milvus_collection(app_id: str, name: str, index_type: str, index_param: dict, description: str,
                             dimension: int):
    name = app_id + "_" + name
    fields = [
        FieldSchema(name="pk", dtype=DataType.VARCHAR, is_primary=True, auto_id=False, max_length=100),
        FieldSchema(name="page_content", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="embeddings", dtype=DataType.FLOAT_VECTOR, dim=dimension),
        FieldSchema(name="metadata", dtype=DataType.JSON),
    ]
    schema = CollectionSchema(fields, description)
    coll = Collection(name, schema, consistency_level="Strong")
    index = {
        "index_type": index_type,
    }
    index.update(index_param)
    coll.create_index("embeddings", index)

    # role = Role(role_name)
    # role.grant("Collection", name, "*")


def drop_milvus_collection(app_id, name):
    name = app_id + "_" + name
    coll = Collection(name, consistency_level="Strong")
    coll.drop()


def get_entity_count_batch(app_id, name_list):
    result = []
    for name in name_list:
        name = app_id + "_" + name
        collection = Collection(name, consistency_level="Strong")
        result.append(collection.num_entities)
    return result


def get_entity_count(app_id, name):
    name = app_id + "_" + name
    collection = Collection(name)
    return collection.num_entities


def create_milvus_user(role_name, username, password):
    role = Role(role_name)
    if not role.is_exist():
        role.create()
    utility.create_user(
        user=username,
        password=password
    )
    role.add_user(username)


def rename_collection(app_id, old_collection_name, new_collection_name):
    old_collection_name = app_id + "_" + old_collection_name
    new_collection_name = app_id + "_" + new_collection_name
    utility.rename_collection(old_collection_name=old_collection_name, new_collection_name=new_collection_name)


class MilvusClient:
    def __init__(self, app_id, collection_name: str):
        self.app_id = app_id
        self.collection_name = collection_name
        self.name = app_id + "_" + collection_name
        self.collection = Collection(
            self.name,
            consistency_level="Strong"
        )
        self.output_fields = [
            'pk',
            'page_content',
            'metadata'
        ]

    def __load_collection(self):
        start = time.time()
        print(f"Start to load collection: {self.name}")
        self.collection.load()
        end = time.time()
        print(f"Load collection success: {self.name}, takes {(end - start) * 1000} ms")

    def release_collection(self):
        start = time.time()
        print(f"Start to release collection: {self.name}")
        self.collection.release()
        end = time.time()
        print(f"Release collection success: {self.name}, takes {(end - start) * 1000} ms")

    def query_vector(self, expr, limit, offset):
        self.__load_collection()
        result = self.collection.query(
            expr=expr,
            output_fields=self.output_fields,
            limit=limit,
            offset=offset
        )
        return result

    def search_vector(self, embedding, expr, limit):
        self.__load_collection()
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
        delete_es_document(self.app_id, self.collection_name, pk)
        return result

    def upsert_record_batch(self, pks, texts, embeddings, metadatas):
        for metadata in metadatas:
            if not metadata.get('createdAt'):
                metadata['createdAt'] = int(time.time())
        data = [
            pks,
            texts,
            embeddings,
            metadatas
        ]
        # 写入 milvus
        result = self.collection.upsert(data)

        es_documents = []
        for index, pk in enumerate(pks):
            es_documents.append({
                "_id": pk,
                "_source": {
                    "page_content": texts[index],
                    "metadata": metadatas[index]
                }
            })
        # 写入 es
        insert_es_batch(
            self.app_id,
            self.collection_name,
            es_documents
        )
        return result

    def insert_vector_from_file(
            self,
            team_id,
            embedding_model,
            file_url,
            metadata,
            task_id,
            chunk_size=1000,
            chunk_overlap=0,
            separator='\n\n',
            pre_process_rules=[],
            jqSchema=None
    ):
        folder = ensure_directory_exists("./download")
        file_path = oss_client.download_file(file_url, folder)
        if not file_path:
            raise Exception("下载文件失败")
        progress_table = FileProcessProgressTable(app_id=self.app_id)
        progress_table.update_progress(task_id, 0.1, "已下载文件到服务器")
        texts = load_documents(file_path, chunk_size=chunk_size, chunk_overlap=chunk_overlap, separator=separator,
                               pre_process_rules=pre_process_rules,
                               jqSchema=jqSchema
                               )
        if len(texts) == 0:
            raise Exception("解析到的段落数为 0")

        progress_table.update_progress(task_id, 0.3, "已加载文件")
        metadatas = []
        for _ in texts:
            item = {
                "source": file_url,
            }
            if metadata and isinstance(metadata, dict):
                item.update(metadata)
            metadatas.append(item)
        texts = [text.page_content for text in texts]
        pks = [
            generate_md5(text) for text in texts
        ]
        embeddings = generate_embedding_of_model(embedding_model, texts)
        progress_table.update_progress(task_id, 0.8, "已生成向量，正在写入向量数据库")
        res = self.upsert_record_batch(pks, texts, embeddings, metadatas)
        progress_table.update_progress(task_id, 1.0, f"完成，共写入 {res.succ_count} 条向量数据")

        file_table = FileRecord(app_id=self.app_id)
        file_table.create_record(team_id, self.collection_name, file_url, {
            "chunkSize": chunk_size,
            "chunkOverlap": chunk_overlap,
            "separator": separator,
            "preProcessRules": pre_process_rules,
            "jqSchema": jqSchema
        })
        return res
