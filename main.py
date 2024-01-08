from dotenv import load_dotenv

# 在最开始的时候加载 .env，不要挪到下面
load_dotenv()

from multiprocessing import Process
from src.http_server import app
from src.worker import conductor_client
from src.queue import consume_task_forever, PROCESS_FILE_QUEUE_NAME


def start_http_server():
    app.run(host='0.0.0.0', port=8899)


if __name__ == '__main__':
    http_server_process = Process(target=start_http_server, args=())
    http_server_process.start()

    consume_task_process = Process(target=consume_task_forever, args=(PROCESS_FILE_QUEUE_NAME,))
    consume_task_process.start()

    conductor_client.start_polling()
