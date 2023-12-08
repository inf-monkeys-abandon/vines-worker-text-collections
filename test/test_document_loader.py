import unittest
from src.utils.document_loader import load_documents


class TestDocumentLoading(unittest.TestCase):
    def test_something(self):
        file_path = "/Users/jayl/Code/infmonkeys/vines-worker-milvus/download/Archive.zip"
        texts = load_documents(file_path)


if __name__ == '__main__':
    unittest.main()
