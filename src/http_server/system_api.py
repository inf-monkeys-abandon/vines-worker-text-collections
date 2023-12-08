from .server import app
from ..milvus import MILVUS_PUBLIC_ADDRESS
from ..utils import SUPPORTED_EMBEDDING_MODELS


@app.get("/api/vector/system-info")
def get_system_info():
    [host, port] = MILVUS_PUBLIC_ADDRESS.split(":") if MILVUS_PUBLIC_ADDRESS else [None, None]
    return {
        "publicAccessEnabled": bool(MILVUS_PUBLIC_ADDRESS),
        "host": host,
        "port": port
    }


@app.get("/api/vector/supported-embedding-models")
def get_supported_embedding_models():
    return SUPPORTED_EMBEDDING_MODELS
