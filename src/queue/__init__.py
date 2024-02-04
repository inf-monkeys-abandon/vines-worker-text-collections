import os
import redis
import json
import traceback
from src.utils.oss import TOSClient
from src.database import CollectionTable, FileProcessProgressTable
from src.es import ESClient

REDIS_URL = os.environ.get("REDIS_URL")
redis = redis.from_url(REDIS_URL)

PROCESS_FILE_QUEUE_NAME = 'queue:vines-worker-text-collections:process-file'


def submit_task(queue_name, task_data):
    task_json = json.dumps(task_data)
    redis.rpush(queue_name, task_json)


def consume_task(task_data):
    app_id = task_data['app_id']
    team_id = task_data['team_id']
    collection_name = task_data['collection_name']
    embedding_model = task_data['embedding_model']
    file_url = task_data['file_url']
    oss_config = task_data['oss_config']
    metadata = task_data['metadata']
    task_id = task_data['task_id']
    chunk_size = task_data['chunk_size']
    chunk_overlap = task_data['chunk_overlap']
    separator = task_data['separator']
    pre_process_rules = task_data['pre_process_rules']
    jqSchema = task_data['jqSchema']
    es_client = ESClient(app_id=app_id, index_name=collection_name)
    table = CollectionTable(
        app_id=app_id
    )
    progress_table = FileProcessProgressTable(app_id)
    # 如果是通过 oss 导入，先获取链接，然后再写入消息队列
    if oss_config:
        try:
            oss_type, oss_config = oss_config.get('ossType'), oss_config.get('ossConfig')
            if oss_type == 'TOS':
                endpoint, region, bucket_name, accessKeyId, accessKeySecret, baseFlder, fileExtensions, excludeFileRegex, importFileNameNotContent = oss_config.get(
                    'endpoint'), oss_config.get('region'), oss_config.get('bucketName'), oss_config.get(
                    'accessKeyId'), oss_config.get('accessKeySecret'), oss_config.get('baseFlder'), oss_config.get(
                    'fileExtensions'), oss_config.get('excludeFileRegex'), oss_config.get(
                    'importFileNameNotContent')
                if fileExtensions:
                    fileExtensions = fileExtensions.split(',')
                tos_client = TOSClient(
                    endpoint,
                    region,
                    bucket_name,
                    accessKeyId,
                    accessKeySecret,
                )
                all_files = tos_client.get_all_files_in_base_folder(
                    baseFlder,
                    fileExtensions,
                    excludeFileRegex
                )
                progress_table.update_progress(
                    task_id=task_id, progress=0.1, message=f"共获取到 {len(all_files)} 个文件"
                )
                if importFileNameNotContent:
                    texts_to_insert = []
                    for absolute_filename in all_files:
                        _, name_with_no, txt_filename = absolute_filename.split("/")
                        filename_without_suffix = name_with_no.split("-")[-1] + txt_filename.split(".")[0]
                        texts_to_insert.append({
                            "page_content": filename_without_suffix,
                            "metadata": {
                                "filepath": absolute_filename
                            }
                        })

                    es_client.insert_texts_batch(
                        embedding_model=embedding_model,
                        text_list=texts_to_insert
                    )
                    progress_table.update_progress(
                        task_id=task_id, progress=1, message=f"写入完成，共 {len(all_files)} 个文件"
                    )
                    table.add_metadata_fields_if_not_exists(
                        team_id, collection_name, ['filename', 'filepath']
                    )
                else:
                    processed = 0
                    for absolute_filename in all_files:
                        _, name_with_no, txt_filename = absolute_filename.split("/")
                        filename_without_suffix = name_with_no.split("-")[-1] + txt_filename.split(".")[0]
                        presign_url = tos_client.get_signed_url(absolute_filename)
                        signed_url = presign_url.signed_url
                        metadata = {
                            "filename": filename_without_suffix,
                            "filepath": absolute_filename
                        }
                        es_client.insert_vector_from_file(
                            team_id,
                            embedding_model, signed_url, metadata,
                            chunk_size=chunk_size,
                            chunk_overlap=chunk_overlap,
                            separator=separator,
                            pre_process_rules=pre_process_rules,
                            jqSchema=jqSchema
                        )
                        processed += 1
                        progress = "{:.2f}".format(processed / len(all_files))
                        progress_table.update_progress(
                            task_id=task_id, progress=0.1 + processed / len(all_files),
                            message=f"已写入 {progress} 个文件"
                        )
                    table.add_metadata_fields_if_not_exists(
                        team_id, collection_name, ['filename', 'filepath']
                    )
        except Exception as e:
            traceback.print_exc()
            progress_table.mark_task_failed(
                task_id=task_id, message=str(e)
            )

    elif file_url:
        try:
            es_client.insert_vector_from_file(
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


# 从队列中获取并处理任务
def consume_task_forever(queue_name):
    while True:
        try:
            # 使用 blpop 阻塞等待任务
            _, task_json_str = redis.blpop(queue_name)
            task_data = json.loads(task_json_str)
            print(f"Processing task: {task_data}")
            consume_task(task_data)
        except Exception as e:
            print("消费任务失败：")
            print("=============================")
            print(e)
            print("=============================")
