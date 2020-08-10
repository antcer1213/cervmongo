#  tests.py
#
#  Copyright 2020 Anthony "antcer1213" Cervantes <anthony.cervantes@cerver.info>
#
#  MIT License
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.
#
#
import cervmongo
import unittest
import os

# INFO: for use in tests, cleaner to have two var references
example_database_one = "example_database_one"
example_database_two = "example_database_two"
example_collection = "example_collection"


class BasicTests(unittest.TestCase):

    def test_cervmongo_client_is_pymongo_client_instance(self):
        """Assert cervmongo client instance is a valid pymongo MongoClient instance"""
        from pymongo import MongoClient as pymongo_client
        cervmongo_client = cervmongo.get_client()()
        self.assertIsInstance(cervmongo_client, pymongo_client)

    def test_modify_config(self):
        cervmongo.config.reset()
        pass

    def test_use_assign_db_fx_to_update_config(self):
        cervmongo.config.reset()
        pass

    def test_set_vars_and_create_client(self):
        cervmongo.config.reset()
        pass

    def test_open_and_close_client(self):
        cervmongo.config.reset()
        pass

    def test_mongodb_server_available(self):
        cervmongo.config.reset()
        #client.server_info()
        pass

    def test_post_get_update_delete(self):
        cervmongo.config.reset()
        pass

    def test_file_settings_with_dotenv(self):
        cervmongo.config.reset()

        sample_dotenv_one = """
MONGO_DB={}
""".format(example_database_one)
        with open(".env", "w") as _file:
            _file.write(sample_dotenv_one)
        cervmongo.config.reload_from_file(env_path=".env", override=True)
        os.remove(".env") # INFO: delete temp .env file
        self.assertEqual(cervmongo.config.MONGO_DB, example_database_one)

    def test_stream_settings_with_dotenv(self):
        cervmongo.config.reset()

        import io
        sample_dotenv_two = """
MONGO_DB={}
""".format(example_database_two)
        stream = io.StringIO(sample_dotenv_two)
        cervmongo.config.reload_from_stream(stream, override=True)
        self.assertEqual(cervmongo.config.MONGO_DB, example_database_two)

    def test_collection_access(self):
        example_document = {"unique_id": "testid", "description": "sample document"}

        cervmongo.config.reset()
        cervmongo.config.set_mongo_db(example_database_one)
        client = cervmongo.get_client()()
        collection = client.COLLECTION(example_collection)
        collection.POST(example_document) # INFO: create document
        document = collection.GET({"unique_id": "testid"}, one=True, empty={})
        doc_id = document.get("_id") # INFO: remove auto-created _id field
        collection.DELETE(doc_id) # INFO: delete record to keep it clean
        total_docs = collection.GET(count=True)
        self.assertEqual(document, example_document)
        self.assertEqual(total_docs, 0)



if __name__ == '__main__':
    unittest.main()
