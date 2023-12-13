from dotenv import load_dotenv

# 在最开始的时候加载 .env，不要挪到下面
load_dotenv()

from multiprocessing import Process
from src.http_server import app
# from src.worker import conductor_client


def start_http_server():
    app.run(host='0.0.0.0', port=8899)


if __name__ == '__main__':
    # p = Process(target=start_http_server, args=())
    # p.start()
    print("start")
    app.run(host='0.0.0.0', port=8899)

    # conductor_client.start_polling()
