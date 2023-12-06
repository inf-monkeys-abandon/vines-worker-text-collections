import uuid
from FlagEmbedding import FlagModel


def generate_pk():
    return str(uuid.uuid4())


def generate_embedding_of_model(model_name, sentences):
    model = FlagModel(
        model_name,
        use_fp16=True
    )  # Setting use_fp16 to True speeds up computation with a slight performance degradation
    embeddings = model.encode(sentences)
    return embeddings
