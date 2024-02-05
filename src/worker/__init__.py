import os
import sys
import signal
from vines_worker_sdk.conductor import ConductorClient
from ..oss import oss_client
from .blocks.fulltext_search_documents import FullTextSearchWorker
from .blocks.rerank import RerankerWorker
from .blocks.search_vector import SearchVectorWorker
from .blocks.insert_vector import InsertVectorWorker
from .blocks.text_to_embedding import TextToEmbeddingWorker

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

ADMIN_SERVER_URL = os.environ.get('ADMIN_SERVER_URL', None)

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
    admin_server_url=ADMIN_SERVER_URL
)


def signal_handler(signum, frame):
    print("SIGTERM or SIGINT signal received.")
    print("开始标记所有 task 为失败状态 ...")

    conductor_client.set_all_tasks_to_failed_state()
    sys.exit()


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

conductor_client.register_worker(FullTextSearchWorker())
conductor_client.register_worker(InsertVectorWorker())
conductor_client.register_worker(RerankerWorker())
conductor_client.register_worker(SearchVectorWorker())
conductor_client.register_worker(TextToEmbeddingWorker())
