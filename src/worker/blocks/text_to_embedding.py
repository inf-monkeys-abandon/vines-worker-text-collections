from src.utils import generate_embedding_of_model
from src.utils import SUPPORTED_EMBEDDING_MODELS
import json
import numpy as np

from vines_worker_sdk.conductor.worker import Worker


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


class TextToEmbeddingWorker(Worker):
    block_name = 'text_to_embedding'
    block_def = {
        "type": "SIMPLE",
        "name": block_name,
        "categories": ['query', 'text'],
        "displayName": '文本向量化',
        "description": '文本转向量数据',
        "icon": 'emoji:💿:#e58c3a',
        "input": [
            {
                "displayName": '文本',
                "name": 'text',
                "type": 'string',
                "default": '',
                "required": True
            },
            {
                "displayName": 'Embedding 模型',
                "name": 'embeddingModel',
                "type": 'options',
                "options": [
                    {
                        "name": item.get("displayName"),
                        "value": item.get("name"),
                        "disabled": not item.get("enabled")
                    } for item in SUPPORTED_EMBEDDING_MODELS
                ],
                "default": SUPPORTED_EMBEDDING_MODELS[0].get("name"),
                "required": True,
            }
        ],
        "output": [
            {
                "name": 'vectorArray',
                "displayName": '向量数组',
                "type": 'collection',
            }
        ],
        "extra": {
            "estimateTime": 3,
        },
    }

    def handler(self, task, workflow_context, credential_data=None):
        workflow_id = task.get('workflowType')
        workflow_instance_id = task.get('workflowInstanceId')
        task_id = task.get('taskId')
        task_type = task.get('taskType')
        print(
            f"开始执行任务：workflow_id={workflow_id}, workflow_instance_id={workflow_instance_id}, task_id={task_id} task_type={task_type}")

        input_data = task.get("inputData")
        print(input_data)

        text = input_data.get('text')
        embedding_model = input_data.get('embeddingModel')
        embedding = generate_embedding_of_model(embedding_model, [text])
        json_dump = json.dumps(embedding, cls=NumpyEncoder)
        return {
            "vectorArray": json.loads(json_dump)[0]
        }
