from elasticsearch import Elasticsearch, helpers
import os
import traceback
from vines_worker_sdk.utils.files import ensure_directory_exists

from src.database import FileProcessProgressTable, FileRecord
from src.oss import oss_client
from src.utils import generate_md5, generate_embedding_of_model, chunk_list
from src.utils.document_loader import load_documents

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


class ESClient:
    def __init__(self, app_id, index_name):
        self.app_id = app_id
        self.index_name_with_no_suffix = index_name
        self.index_name = get_index_name(app_id, index_name)

    def create_es_template(self, dimension: int):
        template = {
            "template": self.index_name,  # 索引名称模式
            "settings": {
                "number_of_shards": 1
            },
            "mappings": {
                "properties": {
                    "page_content": {"type": "text"},
                    "embeddings": {"type": "dense_vector", "dims": dimension},
                    "metadata": {
                        "type": "object",
                        "properties": {
                            "createdAt": {
                                "type": "date"
                            },
                            "userId": {
                                "type": "keyword"
                            },
                            "workflowId": {
                                "type": "keyword"
                            },
                            "fileUrl": {
                                "type": "text"
                            }
                        },
                    }
                }
            }
        }
        es.indices.put_template(
            name=self.index_name,
            body=template
        )

    def upsert_document(self, pk, document):
        return es.index(index=self.index_name, document=document, id=pk)

    def upsert_documents_batch(self, all_documents):
        # 准备批量数据
        chunks = chunk_list(all_documents, 100)
        for chunk in chunks:
            documents = [
                {
                    "_index": self.index_name,
                    "_type": "doc",
                    "_id": document['_id'],
                    "_source": document['_source']
                } for document in chunk
            ]
            # 执行批量操作
            helpers.bulk(es, documents)

    def delete_es_document(self, pk):
        res = es.delete(
            index=self.index_name,
            id=pk
        )
        return res

    def full_text_search(self, query=None, expr=None, metadata_filter=None, from_=0, size=10):
        """ Full Text Search
        :param query: 搜索关键词
        :param expr:
        :param metadata_filter:
        :param size:
        :return:
        """
        must_statements = []
        if query:
            must_statements.append({
                "match": {
                    "page_content": query
                }
            })

        if metadata_filter:
            for key, value in metadata_filter.items():
                must_statements.append({
                    "term": {
                        f"metadata.{key}.keyword": value
                    }
                })
        if expr:
            must_statements.append(expr)

        response = es.search(
            index=self.index_name,
            query={
                "bool": {
                    "must": must_statements
                }
            },
            from_=from_,
            size=size
        )

        return response['hits']['hits']

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
        texts = [text.page_content for text in texts]
        if len(texts) == 0:
            raise Exception("解析到的段落数为 0")

        progress_table.update_progress(task_id, 0.3, "已加载文件")

        pks = [
            generate_md5(text) for text in texts
        ]
        embeddings = generate_embedding_of_model(embedding_model, texts)
        es_documents = []
        for index, pk in enumerate(pks):
            metadata_to_save = {
                "source": file_url,
            }
            if metadata and isinstance(metadata, dict):
                metadata_to_save.update(metadata)
            embedding = embeddings[index]
            es_documents.append({
                "_id": pk,
                "_source": {
                    "page_content": texts[index],
                    "metadata": metadata_to_save,
                    "embeddings": embedding
                }
            })

        progress_table.update_progress(task_id, 0.8, "已生成向量，正在写入向量数据库")
        res = self.upsert_documents_batch(es_documents)
        progress_table.update_progress(task_id, 1.0, f"完成，共写入 {len(es_documents)} 条向量数据")

        file_table = FileRecord(app_id=self.app_id)
        file_table.create_record(team_id, self.index_name_with_no_suffix, file_url, {
            "chunkSize": chunk_size,
            "chunkOverlap": chunk_overlap,
            "separator": separator,
            "preProcessRules": pre_process_rules,
            "jqSchema": jqSchema
        })
        return res
