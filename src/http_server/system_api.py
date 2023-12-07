from .server import app
from ..milvus import MILVUS_PUBLIC_ADDRESS


@app.get("/api/vector/system-info")
def get_system_info():
    [host, port] = MILVUS_PUBLIC_ADDRESS.split(":")
    return {
        "host": host,
        "port": port
    }
