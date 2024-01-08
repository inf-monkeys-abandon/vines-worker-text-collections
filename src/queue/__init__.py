import os
import redis
import json
import traceback

from src.database import CollectionTable, FileProcessProgressTable
from src.milvus import MilvusClient

REDIS_URL = os.environ.get("REDIS_URL")
redis = redis.from_url(REDIS_URL)

PROCESS_FILE_QUEUE_NAME = 'queue:vines-worker-milvus:process-file'


def submit_task(queue_name, task_data):
    task_json = json.dumps(task_data)
    redis.rpush(queue_name, task_json)


# 从队列中获取并处理任务
def consume_task_forever(queue_name):
    while True:
        # 使用 blpop 阻塞等待任务
        _, task_json_str = redis.blpop(queue_name)
        task_data = json.loads(task_json_str)
        print(f"Processing task: {task_data}")

        app_id = task_data['app_id']
        team_id = task_data['team_id']
        collection_name = task_data['collection_name']
        embedding_model = task_data['embedding_model']
        file_url = task_data['file_url']
        metadata = task_data['metadata']
        task_id = task_data['task_id']
        chunk_size = task_data['chunk_size']
        chunk_overlap = task_data['chunk_overlap']
        separator = task_data['separator']
        pre_process_rules = task_data['pre_process_rules']
        jqSchema = task_data['jqSchema']
        table = CollectionTable(
            app_id=app_id
        )
        progress_table = FileProcessProgressTable(app_id)
        try:
            milvus_client = MilvusClient(app_id=app_id, collection_name=collection_name)
            milvus_client.insert_vector_from_file(
                team_id,
                embedding_model, file_url, metadata, task_id,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                separator=separator,
                pre_process_rules=pre_process_rules,
                jqSchema=jqSchema
            )
            table.add_metadata_fields_if_not_exists(
                team_id, collection_name, metadata.keys()
            )
        except Exception as e:
            traceback.print_exc()
            progress_table.mark_task_failed(
                task_id=task_id, message=str(e)
            )
