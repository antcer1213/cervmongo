#  examples.py
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

AsyncIODoc = cervmongo.get_async_doc()
SyncIODoc = cervmongo.get_doc()

def generate_custom_sync_doc(name:str, unique_id:str=None):
    class CustomDoc(SyncIODoc):
        """
        {} Document object.
        """.format(name.title())
        __name__ = name
        UNIQUE_ID = unique_id
        DEFAULT_COLLECTION = DC = "_".join(name.lower().strip().split()) # INFO: required. supply as class attr, or pass on to __init__ kwarg 'collection'
        TEMPLATE_PATH = {"created_datetime": None} # INFO: path to example json document or dict
        SETTINGS_COLLECTION = None # REC: when saving settings related to document type for WebApp
        SCHEMA_PATH = None # INFO: used when strict schema in place, otherwise just use TEMPLATE_PATH for rapid, agile development

        @router.get("/purchases/{_id}", tags=["purchases"], response_model=ResponseDocument)
        async def get_document(self, _id:str):
            """Return document by unique id, if supplied, else '_id' field"""
            return self.load(_id)

        @router.post("/purchases/{_id}", tags=["purchases"], response_model=ResponseDocument)
        async def create_document(self, _id:str = None, datetime:str=None, username:str=None, user_collection:str=None, user_unique_id_field:str=None, **kwargs):
            """Create simple new document"""

            if self.UNIQUE_ID: # INFO: assign _id to UNIQUE_ID if supplied, else '_id'
                kwargs[self.UNIQUE_ID] = _id
            else:
                kwargs["_id"] = _id

            self.create(**kwargs)

            if username:
                user_relation = {
                    "collection": user_collection,
                    "field": user_unique_id_field,
                    "value": username
                    }
            else:
                user_relation = {}

            self.add_timestamp("created_datetime", value=datetime, relation=user_relation)

            return self.save()

    return CustomDoc

def generate_custom_async_doc():
    pass
