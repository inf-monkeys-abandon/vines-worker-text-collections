from multiprocessing import Process
from dotenv import load_dotenv

from src.http_server import app
from src.worker import conductor_client

load_dotenv()


def start_http_server():
    app.run(host='0.0.0.0', port=8899)


if __name__ == '__main__':
    p = Process(target=start_http_server, args=())
    p.start()

    conductor_client.start_polling()
