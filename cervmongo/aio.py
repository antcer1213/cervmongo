#  aio.py
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
__all__ = ["SUPPORT_ASYNCIO_CLIENT", "SUPPORT_ASYNCIO_BUCKET", "get_async_client", "get_async_doc", "AsyncIOClient", "AsyncIODoc"]

from os import path as os_path
from pymongo import WriteConcern
from dateutil.parser import parse as dateparse
import types
from jsonschema import validate
from functools import partial
import typing
import copy
import logging

from .models import (
                GenericResponse,
                StandardResponse,
                MongoListResponse,
                MongoDictResponse,
                )
from .vars import (
                StringEnum,
                IntEnum,
                PAGINATION_SORT_FIELDS,
                ENUM,
                DOC_ID,
                )
from .utils import (
                get_file_meta_information,
                parse_string_header,
                format_string_for_id,
                current_datetime,
                file_and_fileobj,
                detect_mimetype,
                clean_kwargs,
                current_date,
                json_load,
                json_dump,
                logger,
                )
from .config import Config

try:
    from motor.motor_asyncio import AsyncIOMotorClient as MongoClient
    from motor.motor_asyncio import AsyncIOMotorGridFSBucket as GridFSBucket
    SUPPORT_ASYNCIO_CLIENT = True
    SUPPORT_ASYNCIO_BUCKET = True
except:
    logger.warning("motor is not installed. needed if using asyncio")
    class MongoClient: pass
    class GridFSBucket: pass
    SUPPORT_ASYNCIO_CLIENT = False
    SUPPORT_ASYNCIO_BUCKET = False

def get_async_client():
    global SUPPORT_ASYNCIO_CLIENT
    if not SUPPORT_ASYNCIO_CLIENT:
        raise Exception("motor not installed")
    return AsyncIOClient

def get_async_doc():
    global SUPPORT_ASYNCIO_CLIENT
    if not SUPPORT_ASYNCIO_CLIENT:
        raise Exception("motor not installed")
    return AsyncIODoc


class AsyncIOClient(MongoClient):
    """
High-level AsyncIOMotorClient subclass with additional methods added for ease-of-use,
having some automated conveniences and default argument values.
    """
    _MONGO_URI = lambda _: getattr(Config, "MONGO_URI", None)
    _DEFAULT_COLLECTION = None
    _KWARGS = None
    _LOGGING_COND_GET = None
    _LOGGING_COND_POST = None
    _LOGGING_COND_PUT = None
    _LOGGING_COND_PATCH = None
    _LOGGING_COND_DELETE = None

    def __init__(self, mongo_uri=None, default_collection=None, **kwargs):
        self._MONGO_URI = mongo_uri or self._MONGO_URI
        if callable(self._MONGO_URI):
            self._MONGO_URI = self._MONGO_URI()
        self._DEFAULT_COLLECTION = default_collection or self._DEFAULT_COLLECTION

        if kwargs:
            self._KWARGS = kwargs.copy()

        for kwarg in kwargs.keys():
            if kwarg.lower() in ('logging_cond_get', 'logging_cond_post',
                                'logging_cond_put', 'logging_cond_patch',
                                'logging_cond_delete'):
                setattr(self, kwarg.upper(), kwargs.pop(kwarg))

        MongoClient.__init__(self, self._MONGO_URI, **kwargs)

        db = self.get_default_database()
        logger.info("db detected '{}' of type '{}'".format(db.name, type(db.name)))
        if not getattr(db, "name", None) or db.name == "None":
            logger.warning("database not provided in MONGO_URI, assign with method set_database")
            logger.warning("gridfsbucket not instantiated due to missing database")
        else:
            global SUPPORT_ASYNCIO_BUCKET
            if SUPPORT_ASYNCIO_BUCKET:
                logger.debug("gridfsbucket instantiated under self.FILES")
                self.FILES = GridFSBucket(db)
            else:
                logger.warning("gridfsbucket not instantiated due to missing 'tornado' package")
                self.FILES = GridFSBucket() # Using empty object

    def __repr__(self):
        return "<cervmongo.AsyncIOClient>"

    def _process_record_id_type(self, record):
        one = False
        if isinstance(record, str):
            if "$oid" in record:
                record = json_load(record)
                one = True
            elif isinstance(record, DOC_ID.__supertype__):
                record = record
                one = True
            else:
                try:
                    record = DOC_ID(record)
                    one = True
                except:
                    pass
        elif isinstance(record, dict):
            if "$oid" in record or "$regex" in record:
                record = json_dump(record)
                record = json_load(record)
                one = True
        return (record, one)

    def set_database(self, database):
        Config.set_mongo_db(database)
        if self._KWARGS:
            AsyncIOClient.__init__(self, mongo_uri=Config.MONGO_URI, default_collection=self._DEFAULT_COLLECTION, **self._KWARGS)
        else:
            AsyncIOClient.__init__(self, mongo_uri=Config.MONGO_URI, default_collection=self._DEFAULT_COLLECTION)

    def COLLECTION(self, collection:str):
        self._DEFAULT_COLLECTION = collection
        class CollectionClient:
            __parent__ = CLIENT = self
            # INFO: variables
            _DEFAULT_COLLECTION = collection
            _MONGO_URI = self._MONGO_URI
            # INFO: general methods
            GENERATE_ID = self.GENERATE_ID
            COLLECTION = self.COLLECTION
            # INFO: GridFS file operations
            UPLOAD = self.UPLOAD
            DOWNLOAD = self.DOWNLOAD
            ERASE = self.ERASE
            # INFO: truncated Collection methods
            INDEX = partial(self.INDEX, collection)
            ADD_FIELD = partial(self.ADD_FIELD, collection)
            REMOVE_FIELD = partial(self.REMOVE_FIELD, collection)
            DELETE = partial(self.DELETE, collection)
            GET = partial(self.GET, collection)
            POST = partial(self.POST, collection)
            PUT = partial(self.PUT, collection)
            PATCH = partial(self.PATCH, collection)
            REPLACE = partial(self.REPLACE, collection)
            SEARCH = partial(self.SEARCH, collection)
            PAGINATED_QUERY = partial(self.PAGINATED_QUERY, collection)
            def __repr__(s):
                return "<cervmongo.AsyncIOClient.CollectionClient>"
            def get_client(s):
                return s.CLIENT
        return CollectionClient()

    async def PAGINATED_QUERY(self, collection, limit:int=20,
                                sort:PAGINATION_SORT_FIELDS=PAGINATION_SORT_FIELDS["_id"],
                                after:str=None, before:str=None,
                                page:int=None, endpoint:str="/",
                                query:dict={}, **kwargs):
        """Returns paginated results of collection w/ query.

Available pagination methods:
 - __Cursor-based (default)__
    - after
    - before
    - limit (results per page, default 20)
 - __Time-based__ (a datetime field must be selected)
    - sort (set to datetime field)
    - after (records after this time)
    - before (records before this time)
    - limit (results per page, default 20)
 - __Offset-based__ (not recommended)
    - limit (results per page, default 20)
    - page
"""
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        if isinstance(sort, ENUM):
            sort = sort.value

        total_docs = await self.GET(collection, query=query, count=True, empty=0)

        if not page:
            if sort == "_id":
                pagination_method = "cursor"
            else:
                pagination_method = "time"
            cursor = await self.GET(collection, query=query,
                                    limit=limit, key=sort, before=before,
                                    after=after, empty=[])
        else:
            pagination_method = "offset"
            cursor = await self.GET(collection, query=query,
                                    perpage=limit, sort=sort, page=page,
                                    empty=[])

        results = [ record async for record in cursor ]

        # INFO: determine 'cursor' template
        if sort == "_id":
            template = "_{_id}"
        else:
            template = "{date}_{_id}"

        new_after = None
        new_before = None

        if results:
            _id = results[-1]['_id']
            try:
                date = results[-1][sort].isoformat()
            except:
                date = None
            if len(results) == limit:
                new_after = template.format(_id=_id, date=date)

            _id = results[0]['_id']
            try:
                date = results[0][sort].isoformat()
            except:
                date = None
            if any((after, before)):
                new_before = template.format(_id=_id, date=date)

        response = {
            "data": results,
            "details": {
                "pagination_method": pagination_method,
                "query": self.JSON_DUMP(query),
                "unique_id": self._UNIQUE_ID,
                "sort": sort,
                "total": total_docs,
                "count": len(results),
                "limit": limit
                }
            }

        endpoint = endpoint

        # TODO: Refactor
        if pagination_method in ("cursor", "time"):
            response["details"]["cursors"] = {
                  "after": new_after,
                  "before": new_before
                }
            before_url_template = "{endpoint}?sort={sort}&limit={limit}&before={before}"
            after_url_template = "{endpoint}?sort={sort}&limit={limit}&after={after}"
        else: # INFO: pagination_method == "offset"
            response["details"]["cursors"] = {
                  "prev_page": page - 1 if page > 1 else None,
                  "next_page": page + 1 if (page * limit) <= total_docs else None
                }
            before_url_template = "{endpoint}?sort={sort}&limit={limit}&page={page}"
            after_url_template = "{endpoint}?sort={sort}&limit={limit}&page={page}"

        if new_before:
            response["details"]["previous"] = before_url_template.format(
                                                                    endpoint=endpoint,
                                                                    sort=sort,
                                                                    page=page,
                                                                    limit=limit,
                                                                    after=new_after,
                                                                    before=new_before)
        else:
            response["details"]["previous"] = None

        if new_after:
            response["details"]["next"] = after_url_template.format(
                                                                    endpoint=endpoint,
                                                                    sort=sort,
                                                                    page=page,
                                                                    limit=limit,
                                                                    after=new_after,
                                                                    before=new_before)
        else:
            response["details"]["next"] = None

        return response
    PAGINATED_QUERY.clean_kwargs = lambda kwargs: _clean_kwargs(ONLY=("limit", "sort", "after",
                                            "before", "page", "endpoint", "query"), kwargs=kwargs)

    def GENERATE_ID(self, _id=None):
        if _id:
            return DOC_ID(_id)
        else:
            return DOC_ID()

    async def UPLOAD(self, fileobj, filename:str=None, content_type:str=None, extension:str=None, **kwargs):
        fileobj = file_and_fileobj(fileobj)
        metadata = get_file_meta_information(fileobj, filename=filename, content_type=content_type, extension=extension)
        filename = metadata['filename']
        metadata.update(kwargs)
        file_id = await self.FILES.upload_from_stream(filename, fileobj, metadata=metadata)
        return file_id

    async def ERASE(self, filename_or_id, revision:int=-1):
        fs_doc = await self.DOWNLOAD(filename_or_id, revision=revision)
        await self.FILES.delete(fs_doc._id)
        await fs_doc.close()

    async def DOWNLOAD(self, filename_or_id=None, revision:int=-1, skip:int=None, limit:int=None, sort:int=-1, **query):
        revision = int(revision)
        if filename_or_id:
            if isinstance(filename_or_id, DOC_ID.__supertype__):
                return await self.FILES.open_download_stream(filename_or_id)
            else:
                return await self.FILES.open_download_stream_by_name(filename_or_id, revision=revision)

        return self.FILES.find(query, limit=limit, skip=skip, sort=sort, no_cursor_timeout=True)

    async def DELETE(self, collection, record, soft:bool=False):
        db = self.get_default_database()
        if not collection:
            if hasattr(self, '_DEFAULT_COLLECTION'):
                collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        collection = db[collection]

        record = self._process_record_id_type(record)[0]

        if soft:
            data_record = await self.GET(collection, record)
            try:
                await self.PUT("deleted."+collection, data_record)
            except:
                data_record.pop("_id")
                await self.PUT("deleted."+collection, data_record)

        if isinstance(record, (str, ObjectId)):
            return await collection.delete_one({'_id': record})
        elif isinstance(record, dict):
            return await collection.delete_one(record)
        else:
            results = []
            for _id in record:
                results.append(await collection.delete_one({'_id': _id}))
            return results

    def INDEX(self, collection, key:str="_id", sort:int=1, unique:bool=False, reindex:bool=False):
        db = self.get_default_database()
        if not collection:
            if hasattr(self, '_DEFAULT_COLLECTION'):
                collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        collection = db[collection]

        name = "%sIndex%s" % (key, "Asc" if sort == 1 else "Desc")
        try:
            if not name in collection.index_information():
                collection.create_index([
                    (key, sort)], name=name, background=True, unique=unique)
        except:
            #print((_traceback()))
            pass

    async def ADD_FIELD(self, collection, field:str, value='', data=False, query:dict={}):
        if not collection:
            if hasattr(self, '_DEFAULT_COLLECTION'):
                collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        query.update({field: {'$exists': False}})
        if data:
            records = await self.GET(collection, query=query, fields={
                data: True}, empty=[])
        else:
            records = await self.GET(collection, query=query, fields={
                '_id': True}, empty=[])

        for record in records:
            if data:
                await self.PATCH(collection, record['_id'], {"$set": {
                    field: record[data]}})
            else:
                await self.PATCH(collection, record['_id'], {"$set": {
                    field: value}})

    async def REMOVE_FIELD(self, collection, field:str, value='', query:dict={}):
        if not collection:
            collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        query.update({field: {'$exists': True}})
        records = await self.GET(collection, query=query, lst=True)

        for record in records:
            await self.PATCH(collection, record, {"$unset": {field: value}})

    async def GET(self, collection, record=None, query:dict={}, sort:int=1, key:str="_id", lst:bool=None, count:bool=None, search:str=None, fields:dict=None, page:int=None, perpage:int=False, limit:int=None, after:str=None, before:str=None, empty=None, one:bool=False, distinct:str="_id", **kwargs):
        db = self.get_default_database()
        if not collection:
            collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        if not isinstance(collection, (list, tuple, types.GeneratorType)):
            collection = [collection]

        cols = list(collection)
        record = self._process_record_id_type(record)[0]
        results = []
        number_of_results = len(cols)

        query = kwargs.pop("query", {})

        if record:
            record, _one = self._process_record_id_type(record)
            one = _one if _one else one
            if one:
                query.update({"_id": record})
            else:
                query.update(record)

        for collection in cols:
            collection = db[collection]

            if any([query, not record and not search]):
                if count:
                    results.append(await collection.count_documents(query, **kwargs))
                elif lst:
                    cursor = await collection.distinct(distinct, filter=query, **kwargs)
                    results.append(sorted(cursor))
                elif perpage:
                    total = (page - 1) * perpage
                    cursor = collection.find(query, fields, **kwargs)
                    results.append(cursor.sort([(key, sort)]).skip(total).limit(perpage))
                elif limit:
                    query = {"$and": [
                                query
                            ]}
                    if after or before:
                        if after:
                            sort_value, _id_value = after.split("_")
                            _id_value = ObjectId(_id_value)
                            query["$and"].append({"$or": [
                                            {key: {"$lt": _id_value}}
                                        ]})
                            if key != "_id":
                                sort_value = dateparse(sort_value)
                                query["$and"][-1]["$or"].append({key: {"$lt": sort_value}, "_id": {"$lt": _id_value}})
                        elif before:
                            sort_value, _id_value = before.split("_")
                            _id_value = ObjectId(_id_value)
                            query["$and"].append({"$or": [
                                            {key: {"$gt": _id_value}}
                                        ]})
                            if key != "_id":
                                sort_value = dateparse(sort_value)
                                query["$and"][-1]["$or"].append({key: {"$gt": sort_value}, "_id": {"$gt": _id_value}})

                    cursor = collection.find(query, fields, **kwargs).sort([(key, -1)]).limit(limit)
                    results.append(cursor)
                elif one:
                    val = await collection.find_one(query, fields, **kwargs)
                    results.append(val if val else empty)
                else:
                    cursor = collection.find(query, fields, **kwargs).sort([(key, sort)])
                    results.append(cursor)
            elif search:
                try:
                    if count:
                        results.append(await cursor.count_documents({"$text": {"$search": search}}))
                    elif lst:
                        results.append(await collection.distinct(distinct, filter={"$text": {"$search": search}}))
                    else:
                        cursor = collection.find({"$text": {"$search": search}})
                        if perpage:
                            total = (page - 1) * perpage
                            results.append(cursor.sort([(key, sort)]).skip(total).limit(perpage))
                        else:
                            results.append(cursor.sort([(key, sort)]))
                except:
                    cursor = await collection.command('textIndex', search=search)
                    if count:
                        results.append(cursor.count())
                    elif lst:
                        results.append(cursor.distinct(distinct))
                    else:
                        if perpage:
                            total = (page - 1) * perpage
                            results.append(cursor.sort([(key, sort)]).skip(total).limit(perpage))
                        else:
                            results.append(cursor.sort([(key, sort)]))
            else:
                raise Error("unidentified error")

        if number_of_results == 1:
            return results[0]
        else:
            return results

    async def SEARCH(self, collection, search:str, **kwargs):
        if not collection:
            if hasattr(self, '_DEFAULT_COLLECTION'):
                collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        return await self.GET(collection, search=search, **kwargs)

    async def PUT(self, collection, record, many=False):
        db = self.get_default_database()
        if not collection:
            if hasattr(self, '_DEFAULT_COLLECTION'):
                collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        collection = db[collection]

        if many:
            return await collection.insert_many(record)
        else:
            if not "_id" in record:
                await self.logs.tool.insert_one({'name': 'db_put_noid',
                                             'data': record,
                                             'time': current_datetime()})
            return await collection.insert_one(record)

    async def REPLACE(self, collection, original, replacement:dict, upsert=False):
        db = self.get_default_database()
        if not collection:
            if hasattr(self, '_DEFAULT_COLLECTION'):
                collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        collection = db[collection]

        return await collection.replace_one({'_id': original},
                    replacement, upsert=upsert)

    async def PATCH(self, collection, original, updates, upsert:bool=False, w:int=1, multi:bool=False, log=False):
        db = self.get_default_database()
        if not collection:
            if hasattr(self, '_DEFAULT_COLLECTION'):
                collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        collection = db[collection]

        if w != 1:
            WRITE = WriteConcern(w=0)
            _collection = collection.with_options(write_concern=WRITE)
        else:
            _collection = collection
        if log:
            try:
                pass # TODO: optional, may not incorporate
            except:
                pass

        original = self._process_record_id_type(original)

        if multi:
            results = await _collection.update_many(original, updates, upsert=upsert)

            return results
        elif isinstance(original, (str, ObjectId)):
            query = {'_id': original}
            updates["$setOnInsert"] = query
            results = await _collection.update_one(query, updates, upsert=upsert)
            return results
        else:
            results = []
            for i, _id in enumerate(original):
                _id = self._process_record_id_type(_id)
                query = {'_id': _id}
                updates["$setOnInsert"] = query
                result = await collection.update_one(query, updates[i], upsert=upsert)
                results.append(result)
            await self.logs.other.insert_one({'name': 'reindex',
                                        'db': db,
                                        'collection': collection,
                                        'datetime': current_datetime()})
            return results



class AsyncIODoc(AsyncIOClient):
    """
        Custom MongoClient subclass with customizations for creating
        standardized documents and adding json schema validation.
    """
    _DEFAULT_COLLECTION:str = None
    _UNIQUE_ID:str = None
    _TEMPLATE_PATH:str = None
    _SCHEMA_PATH:str = None
    _MARSHMALLOW:str = False
    _DEFAULT_VALUES:dict = {}
    _RESTRICTED_KEYS:typing.Union[typing.List, typing.Tuple] = []

    def __init__(self, _id=None, collection=None, template_path=None, schema_path=None, unique_id=None, **kwargs):
        # INFO: set default collection
        self._DEFAULT_COLLECTION = collection or self._DEFAULT_COLLECTION
        assert self._DEFAULT_COLLECTION, "collection must be of type str"
        # INFO: location for sample record, used as template
        self._TEMPLATE_PATH = template_path or self._TEMPLATE_PATH
        # INFO: path to validation schema
        self._SCHEMA_PATH = schema_path or self._SCHEMA_PATH
        # INFO: sets the unique id field for the document, if any (cannot be _id)
        self._UNIQUE_ID = unique_id or self._UNIQUE_ID

        for kwarg in kwargs.keys():
            if kwarg.lower() in ('marshmallow', 'default_values', 'restricted_keys'):
                setattr(self, kwarg.upper(), kwargs.pop(kwarg))

        # Initial Record object with template else start blank dict
        if self._TEMPLATE_PATH:
            if isinstance(self._TEMPLATE_PATH, str):
                template_full_path = os_path.join(Config.JSON_SAMPLE_PATH, self._TEMPLATE_PATH)
                with open(template_full_path) as file1:
                    template = json_load(file1.read())
            elif isinstance(self._TEMPLATE_PATH, dict):
                template = self._TEMPLATE_PATH
            else:
                raise TypeError("_TEMPLATE_PATH is invalid type '{}', valid types are dict and str".format(type(self._TEMPLATE_PATH)))
            parent_found = template.pop("__parent__", None)
            while parent_found:
                parent_full_path = os_path.join(Config.JSON_SAMPLE_PATH, parent_found)
                with open(parent_full_path) as file1:
                    parent = json_load(file1.read())
                parent.update(template)
                template = parent
                parent_found = template.pop("__parent__", None)
            self.template = template
        else:
            self.template = {}

        # INFO: Load schema else start blank dict to add manual validation entries
        if self._SCHEMA_PATH:
            with open(os_path.join(Config.JSON__SCHEMA_PATH, self._SCHEMA_PATH)) as file1:
                self.schema = json_load(file1.read())
        else:
            self.schema = {}

        AsyncIOClient.__init__(self, self.MONGO_URI)

        # INFO: If class has a _UNIQUE_ID assigned, create unique index
        if self._UNIQUE_ID:
            self.INDEX(self._DEFAULT_COLLECTION, key=self._UNIQUE_ID,
                                                        sort=1, unique=True)

        self.load(_id)

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def _process_restrictions(self, record:dict=None):
        """removes restricted keys from record and return record"""
        if record:
            assert type(record) == dict, "Needs to be a dictionary, got {}".format(type(record))
            return {key: value for key, value in record.items() if not key in self._RESTRICTED_KEYS}
        else:
            return {key: value for key, value in self.RECORD.items() if not key in self._RESTRICTED_KEYS}

    def _p_r(self, record:dict=None):
        """shortname for _process_restrictions"""
        return self._process_restrictions(record=record)

    def _generate_unique_id(self, template:str="{total}", **kwargs):
        return template.format(**kwargs).upper()

    def _timestamp(self, value=None):
        if value:
            try:
                value = dateparse(value)
            except:
                value = datetime.datetime.now()
        else:
            value = datetime.datetime.now()
        return value

    def _guess_corresponding_fieldname(self, _type="unknown", related_field:str=""):
        time_fields = ("date", "datetime", "time")
        if _type in time_fields:
            # NOTE: a timestamp is 'mostly' accompanied by a user or relation
            if related_field:
                if "_" in related_field:
                    field_parts = related_field.split("_")
                elif "-" in related_field:
                    field_parts = related_field.split("-")
                else:
                    field_parts = related_field.split()

                for field_part in field_parts:
                    field_part = field_part.strip(" _-").lower()
                    if any(x in field_part for x in time_fields):
                        continue
                    else:
                        return f"{field_part}_by"
                return "for"
            else:
                return "by"
        else:
            # NOTE: an unknown type field has a timestamp pairing or desc
            if related_field:
                return f"{related_field}_description"
            else:
                return "field_description"

    async def _related_record(self, collection:str=None, field:str="_id", value=False, additional:dict={}):
        additional.update({
            field: True
            })

        record = await self.GET(collection, query={field: value}, fields=additional, one=True, empty={})
        assert record, 'Error: No record found'

        record['key'] = field
        record['collection'] = collection
        record[field] = record[field]

        return record

    async def load(self, _id=None):
        # If _id specified on init, load actual record versus blank template
        if _id:
            if self._UNIQUE_ID:
                self.RECORD = await self.GET(self._DEFAULT_COLLECTION, query={self._UNIQUE_ID: _id}, one=True)
            else:
                self.RECORD = await self.GET(self._DEFAULT_COLLECTION, _id)
        else:
            self.RECORD = copy.deepcopy(self.template)

        if not self.RECORD:
            self.RECORD = {}

        return StandardResponse(
                    data=self._p_r(self.RECORD),
                    details={
                "state": "unsaved" if not self.RECORD.get("_id", None) else "saved",
                "unique_id": self._UNIQUE_ID if self._UNIQUE_ID else "_id"
                })

    async def view(self, _id=False):
        if not _id:
            return StandardResponse(data=self._p_r(self.RECORD))
        else:
            if self._UNIQUE_ID:
                return StandardResponse(
                            data=self._p_r(self.GET(self._DEFAULT_COLLECTION, query={self._UNIQUE_ID: _id}, one=True, empty={})),
                            details={
                        "unique_id": self._UNIQUE_ID if self._UNIQUE_ID else "_id"
                        }
                    )
            else:
                return StandardResponse(
                            data=self._p_r(self.GET(self._DEFAULT_COLLECTION, query={"_id": _id}, one=True, empty={})),
                            details={
                        "unique_id": self._UNIQUE_ID if self._UNIQUE_ID else "_id"
                        }
                    )

    async def reload(self):
        assert self.RECORD.get('_id')
        self.RECORD = await self.GET(self._DEFAULT_COLLECTION, self.RECORD['_id'])

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "unique_id": self._UNIQUE_ID if self._UNIQUE_ID else "_id"
                }
            }

    def id(self):
        if self._UNIQUE_ID:
            return self.RECORD.get(self._UNIQUE_ID, None)
        else:
            return self.RECORD.get("_id", None)

    async def create(self, save:bool=False, trigger=None, template:str="{total}", query:dict={}, **kwargs):
        assert self.RECORD.get('_id') is None, """Cannot use create method on
 an existing record. Use patch method instead."""

        if self._MARSHMALLOW:
            self._MARSHMALLOW().load(kwargs)
            self.RECORD.update(kwargs)
        elif self.template:
            assert all([ x in self.template for x in kwargs.keys()])
            self.RECORD.update(kwargs)
        else:
            self.RECORD.update(kwargs)

        kwargs['total'] = str(self.GET(self._DEFAULT_COLLECTION, query=query).count() + 1).zfill(6)

        if self._UNIQUE_ID and not self.RECORD.get(self._UNIQUE_ID):
            self.RECORD[self._UNIQUE_ID] = self._generate_unique_id(template=template, **kwargs)

        if save:
            await self.save(trigger=None)

        return {
            "data": self._p_r(self.RECORD),
            "details": {"unique_id": self._UNIQUE_ID, "collection": self._DEFAULT_COLLECTION, "_id": self.RECORD[self._UNIQUE_ID]}
            }

    async def push(self, **kwargs):
        assert self.RECORD.get('_id'), """Cannot use push method on
 a non-existing record. Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        await self.PATCH(None, self.RECORD['_id'], {"$push": kwargs})
        await self.reload()

        keys = list(kwargs.keys())
        values = [ kwargs[key] for key in keys ]

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "action": "push",
                "desc": "push a value to end of array, not set",
                "field": kwargs.keys(),
                "value": kwargs.values()
                }
            }

    async def pull(self, **kwargs):
        assert self.RECORD.get('_id'), """Cannot use pull method on
 a non-existing record. Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        await self.PATCH(None, self.RECORD['_id'], {"$pull": kwargs})
        await self.reload()

        keys = list(kwargs.keys())
        values = [ kwargs[key] for key in keys ]

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "action": "pull",
                "desc": "pull all instances of a value from an array",
                "field": kwargs.keys(),
                "value": kwargs.values()
                }
            }

    async def increment(self, query:dict={}, **kwargs):
        assert self.RECORD.get('_id'), """Cannot use increment method on
 a non-existing record. Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        query.update({"_id": self.RECORD['_id']})

        await self.PATCH(None, query, {"$inc": kwargs}, multi=True)
        await self.reload()

        keys = list(kwargs.keys())
        values = [ kwargs[key] for key in keys ]

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "action": "increment",
                "desc": "increment the integer fields by the amount provided",
                "field": keys,
                "increment": values
                }
            }

    async def update(self, query:dict={}, **kwargs):
        assert self.RECORD.get('_id'), """Cannot use update method on
 a non-existing record. Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        query.update({"_id": self.RECORD['_id']})

        keys = list(kwargs.keys())
        old_values = [ self.RECORD.get(key, None) for key in keys ]

        await self.PATCH(None, query, {"$set": kwargs}, multi=True)
        await self.reload()

        new_values = [ self.RECORD.get(key, None) for key in keys ]

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "action": "update",
                "desc": "replace existing field values with new values",
                "field": keys,
                "values_old": old_values,
                "values_new": new_values
                }
            }

    async def patch(self, save:bool=False, trigger=None, **kwargs):
        assert self.RECORD.get('_id'), """Cannot use patch method on
 a non-existing record.Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        if self._MARSHMALLOW:
            _MARSHMALLOW().load(kwargs, partial=True)
            self.RECORD.update(kwargs)
        elif self.template:
            assert all([ x in self.template for x in kwargs.keys()])
            self.RECORD.update(kwargs)
        else:
            self.RECORD.update(kwargs)
        if save:
            await self.save(trigger=trigger)

        return {
            "data": self._p_r(self.RECORD),
            "details": {}
            }

    async def save(self, trigger=None):
        _id = None

        if self._DEFAULT_VALUES:
            for key, value in self._DEFAULT_VALUES.items():
                if not self.RECORD.get(key):
                    self.RECORD[key] = value

        if self.RECORD.get('_id'):
            _id = self.RECORD.pop("_id")
        try:
            if self._MARSHMALLOW:
                self._MARSHMALLOW().load(self.RECORD)
            else:
                validate(self.RECORD, self.schema)
        except:
            raise
        else:
            if _id:
                await self.PATCH(None, _id, {"$set": self.RECORD})
                self.RECORD['_id'] = _id
            else:
                result = await self.PUT(None, self.RECORD)
                self.RECORD['_id'] = result.inserted_id
            if trigger:
                trigger()

        return {
            "data": self._p_r(self.RECORD),
            "details": {}
            }

    async def close(self):
        # TODO: clean closing logic
        await self.load()

        return {
            "data": self.RECORD,
            "details": {}
            }

    async def add_relation(self, key:str, *args, **kwargs):
        self.RECORD[key] = await self._related_record(*args, **kwargs)

        return {
            "data": self._p_r(self.RECORD),
            "details": {"key": key, "relation": self.RECORD[key]}
            }

    async def add_timestamp(self, key:str, value=None, relation:dict={}):
        self.RECORD[key] = self._timestamp(value)

        if relation:
            related_key = relation.pop("key", self._guess_corresponding_fieldname(_type="datetime", related_field=key))
            self.add_relation(related_key, **relation)
        return {
            "data": self._p_r(self.RECORD),
            "details": {"key": key, "value": self.RECORD[key]}
            }

    async def add_object(self, field:str, object_name:str=None, key:str=None, value=None, **kwargs):
        if not object_name:
            if not key:
                self.RECORD[field] = {}
            else:
                self.RECORD[field] = {key: value if value else self.GENERATE_ID()}
        else:
            template_path = os_path.join(_TEMPLATE_PATH, object_name + ".json")
            assert os.path.exists(template_path), "path does not exist"
            with open(template_path) as file1:
                self.RECORD[field] = json_util.loads(file1.read())
            if key:
                self.RECORD[field][key] = value if value else self.GENERATE_ID()

        return {
            "data": self._p_r(self.RECORD),
            "details": {field: self.RECORD[field], "state": "unsaved"}
            }
