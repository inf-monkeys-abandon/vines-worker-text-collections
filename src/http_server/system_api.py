from .server import app
from ..utils import SUPPORTED_EMBEDDING_MODELS


@app.get("/api/vector/supported-embedding-models")
def get_supported_embedding_models():
    return SUPPORTED_EMBEDDING_MODELS
