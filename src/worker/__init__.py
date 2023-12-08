import os
import sys
import signal
from vines_worker_sdk.conductor import ConductorClient
from ..oss import oss_client
from .blocks.search_vector import BLOCK_NAME as SEARCH_VECTOR_BLOCK_NAME, BLOCK_DEF as SEARCH_VECTOR_BLOCK_DEF, \
    handler as search_vector_handler
from .blocks.insert_vector import BLOCK_NAME as INSERT_VECTOR_BLOCK_NAME, BLOCK_DEF as INSERT_VECTOR_BLOCK_DEF, \
    handler as insert_vector_handler
from .blocks.text_to_embedding import BLOCK_NAME as TEXT_TO_EMBEDDING_BLOCK_NAME, \
    BLOCK_DEF as TEXT_TO_EMBEDDING_BLOCK_DEF, \
    handler as text_to_embedding_handler

SERVICE_REGISTRATION_URL = os.environ.get("SERVICE_REGISTRATION_URL")
SERVICE_REGISTRATION_TOKEN = os.environ.get("SERVICE_REGISTRATION_TOKEN")
CONDUCTOR_BASE_URL = os.environ.get("CONDUCTOR_BASE_URL")

if not CONDUCTOR_BASE_URL:
    raise Exception("请在环境变量中配置 CONDUCTOR_BASE_URL")

CONDUCTOR_USERNAME = os.environ.get("CONDUCTOR_USERNAME")
CONDUCTOR_PASSWORD = os.environ.get("CONDUCTOR_PASSWORD")
WORKER_ID = os.environ.get("WORKER_ID")
if not WORKER_ID:
    raise Exception("请在环境变量中配置 WORKER_ID")

CONDUCTOR_CLIENT_NAME_PREFIX = os.environ.get("CONDUCTOR_CLIENT_NAME_PREFIX", None)
REDIS_URL = os.environ.get("REDIS_URL")

if not REDIS_URL:
    raise Exception("请在环境变量中配置 REDIS_URL")

conductor_client = ConductorClient(
    service_registration_url=SERVICE_REGISTRATION_URL,
    service_registration_token=SERVICE_REGISTRATION_TOKEN,
    conductor_base_url=CONDUCTOR_BASE_URL,
    redis_url=REDIS_URL,
    worker_id=WORKER_ID,
    worker_name_prefix=CONDUCTOR_CLIENT_NAME_PREFIX,
    authentication_settings={
        "username": CONDUCTOR_USERNAME,
        "password": CONDUCTOR_PASSWORD,
    },
    external_storage=oss_client,
)


def signal_handler(signum, frame):
    print("SIGTERM or SIGINT signal received.")
    print("开始标记所有 task 为失败状态 ...")

    conductor_client.set_all_tasks_to_failed_state()
    sys.exit()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

conductor_client.register_block(SEARCH_VECTOR_BLOCK_DEF)
conductor_client.register_block(INSERT_VECTOR_BLOCK_DEF)
conductor_client.register_block(TEXT_TO_EMBEDDING_BLOCK_DEF)
conductor_client.register_handler(SEARCH_VECTOR_BLOCK_NAME, search_vector_handler)
conductor_client.register_handler(INSERT_VECTOR_BLOCK_NAME, insert_vector_handler)
conductor_client.register_handler(TEXT_TO_EMBEDDING_BLOCK_NAME, text_to_embedding_handler)
