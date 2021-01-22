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
                silent_drop_kwarg,
                current_datetime,
                file_and_fileobj,
                detect_mimetype,
                dict_to_query,
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
    SUPPORT_ASYNCIO_CLIENT = True #: True if motor package is installed else False
    SUPPORT_ASYNCIO_BUCKET = True #: True if motor package is installed else False
except:
    logger.warning("motor is not installed. needed if using asyncio")
    class MongoClient: pass
    class GridFSBucket: pass # NOTE: in case of refereneces
    SUPPORT_ASYNCIO_CLIENT = False #: True if motor package is installed else False
    SUPPORT_ASYNCIO_BUCKET = False #: True if motor package is installed else False


class AsyncIOClient(MongoClient):
    """
High-level AsyncIOMotorClient subclass with additional methods added for ease-of-use,
having some automated conveniences and defaults.
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
                self.FILES = None

    def __repr__(self):
        db = self.get_default_database()

        if not getattr(db, "name", None) or db.name == "None":
            return "<cervmongo.AsyncIOClient>"
        else:
            return f"<cervmongo.AsyncIOClient.{db.name}>"

    def _process_record_id_type(self, record):
        one = False
        if isinstance(record, str):
            one = True
            if "$oid" in record:
                record = json_load(record)
            else:
                try:
                    record = DOC_ID.__supertype__(record)
                except:
                    pass
        elif isinstance(record, DOC_ID.__supertype__):
            record = record
            one = True
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
                                ordering:int=-1, query:dict={}, **kwargs):
        """
            Returns paginated results of collection w/ query.

            Available pagination methods:
             - **Cursor-based (default)**
                - after
                - before
                - limit (results per page, default 20)
             - **Time-based** (a datetime field must be selected)
                - sort (set to datetime field)
                - after (records after this time)
                - before (records before this time)
                - limit (results per page, default 20)
             - **Offset-based** (not recommended)
                - limit (results per page, default 20)
                - page
        """
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        if isinstance(sort, ENUM.__supertype__):
            sort = sort.value

        total_docs = await self.GET(collection, query, count=True, empty=0)

        if not page:
            if sort == "_id":
                pagination_method = "cursor"
            else:
                pagination_method = "time"
            cursor = await self.GET(collection, query,
                                    limit=limit, key=sort, before=before,
                                    after=after, sort=ordering, empty=[])
        else:
            assert page >= 1, "page must be equal to or greater than 1"
            pagination_method = "offset"
            cursor = await self.GET(collection, query,
                                    perpage=limit, key=sort, page=page,
                                    sort=ordering, empty=[])

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

            if pagination_method in ("cursor", "time"):
                if before:
                    check_ahead = await self.GET(collection, query,
                                            limit=limit, key=sort, before=new_before, empty=0, count=True)
                    if not check_ahead:
                        new_before = None
                elif after:
                    check_ahead = await self.GET(collection, query,
                                            limit=limit, key=sort, after=new_after, empty=0, count=True)
                    if not check_ahead:
                        new_after = None

        response = {
            "data": results,
            "details": {
                "pagination_method": pagination_method,
                "query": dict_to_query(query),
                "sort": sort,
                "unique_id": getattr(self, "_UNIQUE_ID", "_id"),
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
            return DOC_ID.__supertype__(_id)
        else:
            return DOC_ID.__supertype__()

    async def UPLOAD(self, fileobj, filename:str=None, content_type:str=None, extension:str=None, **kwargs):
        assert self.FILES, "GridFS instance not initialized, run method 'set_database' with the desired database and try again"
        fileobj = file_and_fileobj(fileobj)
        metadata = get_file_meta_information(fileobj, filename=filename, content_type=content_type, extension=extension)
        filename = metadata['filename']
        metadata.update(kwargs)
        file_id = await self.FILES.upload_from_stream(filename, fileobj, metadata=metadata)
        return file_id

    async def ERASE(self, filename_or_id, revision:int=-1):
        assert self.FILES, "GridFS instance not initialized, run method 'set_database' with the desired database and try again"
        fs_doc = await self.DOWNLOAD(filename_or_id, revision=revision)
        await self.FILES.delete(fs_doc._id)
        await fs_doc.close()

    async def DOWNLOAD(self, filename_or_id=None, revision:int=-1, skip:int=None, limit:int=None, sort:int=-1, **query):
        assert self.FILES, "GridFS instance not initialized, run method 'set_database' with the desired database and try again"
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

    async def ADD_FIELD(self, collection, field:str, value:typing.Union[typing.Dict, typing.List, str, int, float, bool]='', data=False, query:dict={}):
        if not collection:
            if hasattr(self, '_DEFAULT_COLLECTION'):
                collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        query.update({field: {'$exists': False}})
        if data:
            records = await self.GET(collection, query, fields={
                data: True}, empty=[])
        else:
            records = await self.GET(collection, query, fields={
                '_id': True}, empty=[])

        for record in records:
            if data:
                await self.PATCH(collection, record['_id'], {"$set": {
                    field: record[data]}})
            else:
                await self.PATCH(collection, record['_id'], {"$set": {
                    field: value}})

    async def REMOVE_FIELD(self, collection, field:str, query:dict={}) -> None:
        if not collection:
            collection = self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        query.update({field: {'$exists': True}})
        records = await self.GET(collection, query, distinct=True)

        for record in records:
            await self.PATCH(collection, record, {"$unset": {field: ""}})

    async def GET(self, collection, id_or_query:typing.Union[DOC_ID, str, typing.Dict]={}, sort:int=1, key:str="_id", count:bool=None, search:str=None, fields:dict=None, page:int=None, perpage:int=False, limit:int=None, after:str=None, before:str=None, empty=None, distinct:str=None, one:bool=False, **kwargs):
        db = self.get_default_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection not provided"

        if not isinstance(collection, (list, tuple, types.GeneratorType)):
            collection = [collection]
        cols = list(set(collection))
        results = []
        number_of_results = len(cols)

        if distinct == True:
            distinct = "_id"

        id_or_query, _one = self._process_record_id_type(id_or_query)
        one = _one if _one else one
        if _one:
            query = {"_id": id_or_query}
        else:
            query = id_or_query

        for collection in cols:
            collection = db[collection]

            if query or not search:
                if count and not limit:
                    if query:
                        results.append(await collection.count_documents(query, **kwargs))
                    else:
                        results.append(await collection.estimated_document_count(**kwargs))
                elif distinct:
                    cursor = await collection.distinct(distinct, filter=query, **kwargs)
                    results.append(sorted(cursor))
                elif perpage:
                    total = (page - 1) * perpage
                    cursor = collection.find(query, projection=fields, **kwargs)
                    results.append(cursor.sort([(key, sort)]).skip(total).limit(perpage))
                elif limit:
                    if any((query, after, before)):
                        query = {"$and": [
                                    query
                                ]}
                    if after or before:
                        if after:
                            sort_value, _id_value = after.split("_")
                            _id_value = DOC_ID.__supertype__(_id_value)
                            query["$and"].append({"$or": [
                                            {key: {"$lt": _id_value}}
                                        ]})
                            if key != "_id":
                                sort_value = dateparse(sort_value)
                                query["$and"][-1]["$or"].append({key: {"$lt": sort_value}, "_id": {"$lt": _id_value}})
                        elif before:
                            sort_value, _id_value = before.split("_")
                            _id_value = DOC_ID.__supertype__(_id_value)
                            query["$and"].append({"$or": [
                                            {key: {"$gt": _id_value}}
                                        ]})
                            if key != "_id":
                                sort_value = dateparse(sort_value)
                                query["$and"][-1]["$or"].append({key: {"$gt": sort_value}, "_id": {"$gt": _id_value}})

                    if count:
                        try:
                            cursor = await collection.count_documents(query, limit=limit, hint=[(key, sort)], **kwargs)
                        except:
                            cursor = len(await collection.find(query, fields, **kwargs).sort([(key, sort)]).to_list(limit))
                        results.append(cursor)
                    else:
                        cursor = collection.find(query, projection=fields, **kwargs).sort([(key, sort)]).limit(limit)
                        results.append(cursor)
                elif one:
                    val = await collection.find_one(query, projection=fields, sort=[(key, sort)], **kwargs)
                    results.append(val if val else empty)
                else:
                    cursor = collection.find(query, projection=fields, **kwargs).sort([(key, sort)])
                    results.append(cursor)
            elif search:
                try:
                    if count:
                        results.append(await cursor.count_documents({"$text": {"$search": search}}))
                    elif distinct:
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
                    elif distinct:
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

    async def POST(self, collection, record_or_records:typing.Union[typing.List, typing.Dict]):
        db = self.get_default_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        collection = db[collection]

        if isinstance(record_or_records, (list, tuple)):
            return await collection.insert_many(record_or_records)
        elif isinstance(record_or_records, dict):
            return await collection.insert_one(record_or_records)
        else:
            raise TypeError("invalid record type '{}' provided".format(type(record_or_records)))

    async def PUT(self, collection, record_or_records:typing.Union[typing.List, typing.Dict]):
        """
            creates or replaces record(s) with exact _id provided, _id is required with record object(s)

            returns original document, if replaced
        """
        db = self.get_default_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        collection = db[collection]

        if isinstance(record_or_records, (list, tuple)):
            assert all([ record.get("_id", None) for record in record_or_records ]), "not all records provided contained an _id"
            return await collection.insert_many(record_or_records, ordered=False)
        elif isinstance(record_or_records, dict):
            assert record_or_records.get("_id", None), "no _id provided"
            query = {"_id": record_or_records["_id"]}
            return await collection.find_one_and_replace(query, record_or_records, upsert=True)
        else:
            raise TypeError("invalid record type '{}' provided".format(type(record_or_records)))

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
    _MONGO_URI = lambda _: getattr(Config, "MONGO_URI", None)
    _DOC_TYPE:str = None #: MongoDB collection to use
    _DOC_ID:str = "_id"
    _DOC_SAMPLE:str = None
    _DOC_SCHEMA:str = None
    _DOC_MARSHMALLOW:str = False
    _DOC_DEFAULTS:dict = {}
    _DOC_RESTRICTED_KEYS:list = []
    _DOC_ENUMS:list = []
    _DOC_SETTINGS:str = None

    def __init__(self, _id=None, doc_type:str=None, doc_sample:typing.Union[typing.Dict, str]=None, doc_schema:typing.Union[typing.Dict, str]=None, doc_id:str=None, mongo_uri:str=None, **kwargs):
        self._MONGO_URI = mongo_uri or self._MONGO_URI
        if callable(self._MONGO_URI):
            self._MONGO_URI = self._MONGO_URI()
        # INFO: set default collection
        self._DOC_TYPE = doc_type or self._DOC_TYPE
        assert self._DOC_TYPE, "collection must be of type str"
        self._DEFAULT_COLLECTION = self._DOC_TYPE
        # INFO: location for sample record, used as template
        self._DOC_SAMPLE = doc_sample or self._DOC_SAMPLE
        # INFO: path to validation schema
        self._DOC_SCHEMA = doc_schema or self._DOC_SCHEMA
        # INFO: sets the unique id field for the document type, if any (cannot be _id)
        self._DOC_ID = doc_id or self._DOC_ID
        assert self._DOC_ID, "unique id field name must be of type str"

        for kwarg in kwargs.keys():
            if kwarg.lower() in ('doc_settings', 'doc_marshmallow', 'doc_defaults', 'doc_restricted_keys'):
                setattr(self, "_{}".format(kwarg.upper()), kwargs.pop(kwarg))

        # Initial Record object with sample else start blank dict
        if self._DOC_SAMPLE:
            if isinstance(self._DOC_SAMPLE, str):
                sample_full_path = os_path.join(Config.JSON_SAMPLE_PATH, self._DOC_SAMPLE)
                with open(sample_full_path) as file1:
                    sample = json_load(file1.read())
            elif isinstance(self._DOC_SAMPLE, dict):
                sample = self._DOC_SAMPLE
            else:
                raise TypeError("_DOC_SAMPLE is invalid type '{}', valid types are dict and str".format(type(self._DOC_SAMPLE)))
            sample_parent_found = sample.pop("__parent__", None)
            while sample_parent_found:
                parent_sample_full_path = os_path.join(Config.JSON_SAMPLE_PATH, sample_parent_found)
                with open(parent_sample_full_path) as _file:
                    parent_sample = json_load(_file.read())
                parent_sample.update(sample)
                sample = parent_sample
                sample_parent_found = sample.pop("__parent__", None)
            self.sample = sample
        else:
            self.sample = {}

        # INFO: Load schema else start blank dict to add manual validation entries
        if self._DOC_SCHEMA:
            if isinstance(self._DOC_SCHEMA, str):
                schema_full_path = os_path.join(Config.JSON_SCHEMA_PATH, self._DOC_SCHEMA)
                with open(schema_full_path) as _file:
                    self.schema = json_load(_file.read())
            elif isinstance(self._DOC_SCHEMA, dict):
                self.schema = self._DOC_SCHEMA
            else:
                raise TypeError("_DOC_SCHEMA is invalid type '{}', valid types are dict and str".format(type(self._DOC_SCHEMA)))
        else:
            self.schema = {}

        AsyncIOClient.__init__(self, **kwargs)

        db = self.get_default_database()

        if not getattr(db, "name", None) or db.name == "None":
            raise Exception("database not provided in MongoDB URI")
        else:
            self._DOC_DB = db.name

        # Initialize enums
        if not self._DOC_ENUMS:
            pass
            #enums_record = self.GET(self._DOC_SETTINGS, "enums"

        # INFO: If class has a _DOC_ID assigned, create unique index
        if self._DOC_ID != "_id":
            self.INDEX(self._DOC_TYPE, key=self._DOC_ID,
                                                        sort=1, unique=True)

        self.load(_id)

    def __repr__(self):
        if self.RECORD.get("_id", None):
            _id = self.id()
            return f"<cervmongo.AsyncIODoc.{self._DOC_DB}.{self._DOC_TYPE}.{self._DOC_ID}:{_id}>"
        else:
            return "<cervmongo.AsyncIODoc>"

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def _process_restrictions(self, record:dict=None):
        """removes restricted keys from record and return record"""
        try:
            if record:
                assert isinstance(record, (MongoDictResponse, dict)), "Needs to be a dictionary, got {}".format(type(record))
                return {key: value for key, value in record.items() if not key in self._DOC_RESTRICTED_KEYS}
            else:
                return {key: value for key, value in self.RECORD.items() if not key in self._DOC_RESTRICTED_KEYS}
        except:
            logger.exception("encountered error when cleaning self.RECORD, returning empty dict")
            return {}

    def _p_r(self, record:dict=None):
        """truncated alias for _process_restrictions"""
        return self._process_restrictions(record=record)

    def _generate_unique_id(self, template:str="{total}", **kwargs):
        return template.format(**kwargs).upper()

    def _timestamp(self, value=None):
        if value:
            try:
                value = dateparse(value)
            except:
                value = current_datetime()
        else:
            value = current_datetime()
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

        record = await self.GET(collection, {field: value}, fields=additional, one=True, empty={})
        assert record, 'Error: No record found'

        record['key'] = field
        record['collection'] = collection
        record[field] = record[field]

        return record

    async def load(self, _id=None):
        # If _id specified on init, load actual record versus blank template
        if _id:
            if self._DOC_ID:
                self.RECORD = await self.GET(self._DOC_TYPE, {self._DOC_ID: _id}, one=True)
            else:
                self.RECORD = await self.GET(self._DOC_TYPE, _id)
        else:
            self.RECORD = copy.deepcopy(self.sample)

        if not self.RECORD:
            self.RECORD = {}

        return StandardResponse(
                    data=self._p_r(self.RECORD),
                    details={
                "state": "unsaved" if not self.RECORD.get("_id", None) else "saved",
                "unique_id": self._DOC_ID
                })

    async def view(self, _id=False):
        if not _id:
            return StandardResponse(data=self._p_r(self.RECORD))
        else:
            if self._DOC_ID:
                return StandardResponse(
                            data=self._p_r(self.GET(self._DOC_TYPE, {self._DOC_ID: _id}, one=True, empty={})),
                            details={
                        "unique_id": self._DOC_ID
                        }
                    )
            else:
                return StandardResponse(
                            data=self._p_r(self.GET(self._DOC_TYPE, {"_id": _id}, one=True, empty={})),
                            details={
                        "unique_id": self._DOC_ID
                        }
                    )

    async def reload(self):
        assert self.RECORD.get('_id')
        self.RECORD = await self.GET(self._DOC_TYPE, self.RECORD['_id'])

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "unique_id": self._DOC_ID
                }
            }

    def id(self):
        return self.RECORD.get(self._DOC_ID, None)

    async def create(self, save:bool=False, trigger=None, template:str="{total}", query:dict={}, **kwargs):
        assert self.RECORD.get('_id') is None, """Cannot use create method on
 an existing record. Use patch method instead."""

        if self._DOC_MARSHMALLOW:
            self._DOC_MARSHMALLOW().load(kwargs)
            self.RECORD.update(kwargs)
        elif self.sample:
            # INFO: removing invalid keys based on sample record
            [ silent_drop_kwarg(kwargs, x, reason="not in self.sample") for x in list(kwargs.keys()) if not x in self.sample ]
            self.RECORD.update(kwargs)
        else:
            self.RECORD.update(kwargs)

        kwargs['total'] = str(self.GET(self._DOC_TYPE, query).count() + 1).zfill(6)

        if self._DOC_ID and not self.RECORD.get(self._DOC_ID):
            self.RECORD[self._DOC_ID] = self._generate_unique_id(template=template, **kwargs)

        if save:
            await self.save(trigger=None)

        return {
            "data": self._p_r(self.RECORD),
            "details": {"unique_id": self._DOC_ID, "collection": self._DOC_TYPE, "_id": self.RECORD[self._DOC_ID]}
            }

    async def add_enum(name:str, value):
        self.RECORD

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

        if self._DOC_MARSHMALLOW:
            _DOC_MARSHMALLOW().load(kwargs, partial=True)
            self.RECORD.update(kwargs)
        elif self.sample:
            assert all([ x in self.sample for x in kwargs.keys()])
            self.RECORD.update(kwargs)
        else:
            self.RECORD.update(kwargs)
        if save:
            await self.save(trigger=trigger)

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "processed": True,
                "diff": kwargs
                }
            }

    async def save(self, trigger=None):
        _id = None

        if self._DOC_DEFAULTS:
            for key, value in self._DOC_DEFAULTS.items():
                if not self.RECORD.get(key, None):
                    self.RECORD[key] = value

        if self.RECORD.get('_id', None):
            _id = self.RECORD.pop("_id", None)
        try:
            if self._DOC_MARSHMALLOW:
                self._DOC_MARSHMALLOW().load(self.RECORD)
            else:
                validate(self.RECORD, self.schema)
        except:
            raise
        else:
            if _id:
                self.RECORD['_id'] = _id
                await self.PUT(None, self.RECORD)
            else:
                result = await self.POST(None, self.RECORD)
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
            template_path = os_path.join(_DOC_SAMPLE, object_name + ".json")
            assert os.path.exists(template_path), "path does not exist"
            with open(template_path) as file1:
                self.RECORD[field] = json_util.loads(file1.read())
            if key:
                self.RECORD[field][key] = value if value else self.GENERATE_ID()

        return {
            "data": self._p_r(self.RECORD),
            "details": {field: self.RECORD[field], "state": "unsaved"}
            }


def get_async_client() -> AsyncIOClient:
    """returns AsyncIOClient class"""
    global SUPPORT_ASYNCIO_CLIENT
    if not SUPPORT_ASYNCIO_CLIENT:
        raise Exception("motor not installed")
    return AsyncIOClient

def get_async_doc() -> AsyncIODoc:
    """returns AsyncIODoc class"""
    global SUPPORT_ASYNCIO_CLIENT
    if not SUPPORT_ASYNCIO_CLIENT:
        raise Exception("motor not installed")
    return AsyncIODoc
