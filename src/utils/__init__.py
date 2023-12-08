import uuid
from FlagEmbedding import FlagModel
from random import choice
from string import ascii_letters
from shortid import ShortId

sid = ShortId()


def generate_pk():
    return str(uuid.uuid4())


def generate_short_id():
    return str(sid.generate())


def generate_random_string(length=12):
    return ''.join(choice(ascii_letters) for i in range(length))


def generate_embedding_of_model(model_name, q):
    model = FlagModel(
        model_name,
        use_fp16=True
    )  # Setting use_fp16 to True speeds up computation with a slight performance degradation
    embeddings = model.encode(q)
    return embeddings
