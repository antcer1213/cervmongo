#  main.py
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
__all__ = ["SUPPORT_GRIDFS", "get_client", "get_doc", "SyncIOClient", "SyncIODoc"]

from os import path as os_path
from pymongo import MongoClient
from pymongo import WriteConcern
from dateutil.parser import parse as dateparse
import types
from jsonschema import validate
from functools import partial
import typing
import copy

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
    from gridfs import GridFSBucket
    SUPPORT_GRIDFS = True #: True if gridfs functionality is available else False
except:
    logger.warning("gridfs is not installed")
    SUPPORT_GRIDFS = False #: True if gridfs functionality is available else False
    class GridFSBucket: pass # NOTE: in case of refereneces

_CollectionClientType = typing.TypeVar("CollectionClient")


class SyncIOClient(MongoClient):
    """
        High-level MongoClient subclass with additional methods added for ease-of-use,
        having some automated conveniences and defaults.
    """
    _MONGO_URI = lambda _: getattr(Config, "MONGO_URI", None) #: Valid MongoDB URI, defaults to Config.MONGO_URI if not supplied
    _DEFAULT_COLLECTION = None
    _KWARGS = None #: saves originally supplied kwargs, if a reload is required
    _LOGGING_COND_GET = None
    _LOGGING_COND_POST = None
    _LOGGING_COND_PUT = None
    _LOGGING_COND_PATCH = None
    _LOGGING_COND_DELETE = None

    def __init__(self, mongo_uri:typing.Optional[str]=None, default_collection:typing.Optional[str]=None, **kwargs):
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
        if not getattr(db, "name", None):
            logger.warning("database not provided in MONGO_URI, assign with method set_database")
            logger.warning("gridfsbucket not instantiated due to missing database")
        else:
            global SUPPORT_GRIDFS
            if SUPPORT_GRIDFS:
                logger.debug("gridfsbucket instantiated under self.FILES")
                self.FILES = GridFSBucket(db)
            else:
                logger.warning("gridfsbucket not instantiated due to missing 'gridfs' package")
                self.FILES = None

    def __repr__(self):
        db = self.get_default_database()

        if not getattr(db, "name", None) or db.name == "None":
            return "<cervmongo.SyncIOClient>"
        else:
            return f"<cervmongo.SyncIOClient.{db.name}>"

    def _process_record_id_type(self, record):
        one = False
        if isinstance(record, str):
            one = True
            if "$oid" in record:
                record = {"$in": [json_load(record), record]}
            else:
                try:
                    record = {"$in": [DOC_ID.__supertype__(record), record]}
                except:
                    pass
        elif isinstance(record, DOC_ID.__supertype__):
            record = record
            one = True
        elif isinstance(record, dict):
            keys = record.keys()
            if any(["$oid" in keys, "$regex" in keys]):
                record = json_dump(record)
                record = json_load(record)
                one = True
        return (record, one)

    def set_database(self, database:str) -> None:
        """
            is used to change or set database of client instance, also changes db in Config class
        """
        Config.set_mongo_db(database)
        if self._KWARGS:
            SyncIOClient.__init__(self, mongo_uri=Config.MONGO_URI, default_collection=self._DEFAULT_COLLECTION, **self._KWARGS)
        else:
            SyncIOClient.__init__(self, mongo_uri=Config.MONGO_URI, default_collection=self._DEFAULT_COLLECTION)

    def COLLECTION(self, collection:str):
        """
            returns unique `CollectionClient <#cervmongo.main.cervmongo.main.SyncIOClient.COLLECTION.CollectionClient>`_ class instance

            Be aware, CollectionClient is NOT a valid MongoClient. To access the original
            SyncIOClient instance, use method get_client of CollectionClient instance.
        """

        self._DEFAULT_COLLECTION = collection


        class CollectionClient:
            """Convenience class that auto-supplies collection to all upper-cased SyncIOClient methods, where required"""

            __parent__ = CLIENT = self #! the original SyncIOClient instance
            # INFO: variables
            _DEFAULT_COLLECTION = collection #: the default collection assigned
            _MONGO_URI = self._MONGO_URI #: the MongoDB URI supplied from SyncIOClient instance
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
                return "<cervmongo.SyncIOClient.CollectionClient>"
            def get_client(s):
                return s.CLIENT

        # ~ if function: # NOTE: Make CollectionClient class a function attr?
            # ~ setattr(function, "CollectionClient", CollectionClient) # NOTE: Make CollectionClient class a function attr?

        return CollectionClient()
    # ~ COLLECTION.__defaults__ = COLLECTION.__defaults__[:-1] + (COLLECTION,) # NOTE: Make CollectionClient class a function attr?

    def PAGINATED_QUERY(self, collection:typing.Optional[str], limit:int=20,
                                sort:PAGINATION_SORT_FIELDS=PAGINATION_SORT_FIELDS["_id"],
                                after:str=None, before:str=None,
                                page:int=None, endpoint:str="/",
                                ordering:int=-1, query:dict={}, **kwargs):
        """
            Returns paginated results of cursor from the collection query.

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

        total_docs = self.GET(collection, query, count=True, empty=0)

        if not page:
            if sort == "_id":
                pagination_method = "cursor"
            else:
                pagination_method = "time"
            results = self.GET(collection, query,
                                    limit=limit, key=sort, before=before,
                                    after=after, sort=ordering, empty=[]).list()

        else:
            assert page >= 1, "page must be equal to or greater than 1"
            pagination_method = "offset"
            results = self.GET(collection, query,
                                    perpage=limit, key=sort, page=page,
                                    sort=ordering, empty=[]).list()

        # INFO: determine 'cursor' template
        if sort == "_id":
            template = "_{_id}"
        else:
            template = "{date}_{_id}"

        new_after = None
        new_before = None

        if results:
            _id = results[-1]["_id"]
            try:
                date = results[-1][sort].isoformat()
            except:
                date = None
            if len(results) == limit:
                new_after = template.format(_id=_id, date=date)

            _id = results[0]["_id"]
            try:
                date = results[0][sort].isoformat()
            except:
                date = None
            if any((after, before)):
                new_before = template.format(_id=_id, date=date)

            if pagination_method in ("cursor", "time"):
                if before:
                    check_ahead = self.GET(collection, query,
                                            limit=limit, key=sort, before=new_before, count=True, empty=0)
                    if not check_ahead:
                        new_before = None
                elif after:
                    check_ahead = self.GET(collection, query,
                                            limit=limit, key=sort, after=new_after, count=True, empty=0)
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
    PAGINATED_QUERY.clean_kwargs = lambda kwargs: clean_kwargs(ONLY=("limit", "sort", "after",
                                            "before", "page", "endpoint", "query"), kwargs=kwargs)

    def GENERATE_ID(self, _id:str=None) -> DOC_ID:
        """
            returns a unique ObjectID, simply for convenience
        """
        if _id:
            return DOC_ID.__supertype__(_id)
        else:
            return DOC_ID.__supertype__()

    def DELETE(self, collection:typing.Optional[str], record_or_records, soft:bool=False) -> typing.Union[MongoDictResponse, MongoListResponse]:
        """
            deletes the requested document(s)

            returns MongoDB response document

            If soft=true, creates collection ('deleted.{collection}')
            and inserts the deleted document there. field 'oid' is
            guaranteed to equal the original document's "_id".
        """
        db = self.get_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"

        collection = db[collection]

        record_or_records = self._process_record_id_type(record_or_records)[0]

        if isinstance(record_or_records, (str, DOC_ID.__supertype__)):
            record_or_records = {"_id": record_or_records}

        if isinstance(record_or_records, dict):
            data_record = collection.find_one_and_delete(record_or_records)
            if soft:
                try:
                    data_record["oid"] = data_record["_id"]
                    self.PUT("deleted."+collection, data_record)
                except:
                    data_record["oid"] = data_record.pop("_id")
                    self.PUT("deleted."+collection, data_record)
            return MongoDictResponse(data_record)
        elif isinstance(record_or_records, (list, tuple)):
            results = []
            for _id in record_or_records:
                data_record = collection.find_one_and_delete({"_id": _id})
                if soft:
                    try:
                        data_record["oid"] = data_record["_id"]
                        self.PUT("deleted."+collection, data_record)
                    except:
                        data_record["oid"] = data_record.pop("_id")
                        self.PUT("deleted."+collection, data_record)
                results.append(data_record)

            return MongoListResponse(results)
        else:
            raise TypeError("record_or_records was of invalid type '{}'".format(type(record_or_records)))

    def INDEX(self, collection, key:str="_id", sort:int=1, unique:bool=False, reindex:bool=False) -> None:
        """
            creates an index, however most useful in constraining certain fields as unique
        """
        db = self.get_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        collection = db[collection]

        if reindex:
            collection.reindex()
        else:
            name = "%sIndex%s" % (key, "Asc" if sort == 1 else "Desc")
            try:
                if not name in collection.index_information():
                    collection.create_index([
                        (key, sort)], name=name, background=True, unique=unique)
                try:
                    # INFO: a full text-index is recommended when no fixed schema / document structure is in place
                    collection.create_index([("$**", "text")], name="textIndex", background=True)
                except:
                    pass
            except:
                # TODO: provide a short clear exception?
                raise

    def ADD_FIELD(self, collection, field:str, value:typing.Union[typing.Dict, typing.List, str, int, float, bool]="", data=False, query:dict={}) -> None:
        """
            adds field with value provided to all records in collection that match query

            kwarg 'data', if provided, is an existing field in the record that can be used as the default value.
            useful for name changes of fields in schema.
        """
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        query.update({field: {"$exists": False}})
        if data:
            records = self.GET(collection, query, fields={
                data: True}, empty=[])
        else:
            records = self.GET(collection, query, fields={
                "_id": True}, empty=[])

        for record in records:
            if data:
                self.PATCH(collection, record["_id"], {"$set": {
                    field: record[data]}})
            else:
                self.PATCH(collection, record["_id"], {"$set": {
                    field: value}})

    def REMOVE_FIELD(self, collection, field:str, query:dict={}) -> None:
        """
            removes field of all records in collection that match query

            useful for complete field removal, once field is no longer needed.
        """
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        query.update({field: {"$exists": True}})
        records = self.GET(collection, query, distinct=True)

        for record in records:
            self.PATCH(collection, record, {"$unset": {field: ""}})

    def UPLOAD(self, fileobj:typing.Union[typing.IO, str], filename:str=None, content_type:str=None, extension:str=None, **kwargs):
        """
            returns GridFS response document after successful upload

            fileobj can be either valid filepath or file/file-like object,
            all other kwargs are stored as meta data
        """
        assert self.FILES, "GridFS instance not initialized, run method 'set_database' with the desired database and try again"
        fileobj = file_and_fileobj(fileobj)
        metadata = get_file_meta_information(fileobj, filename=filename, content_type=content_type, extension=extension)
        filename = metadata['filename']
        metadata.update(kwargs)
        return self.FILES.upload_from_stream(filename, fileobj, metadata=metadata)

    def ERASE(self, filename_or_id:typing.Union[str, DOC_ID], revision:int=-1) -> None:
        """
            deletes the GridFS file. if multiple revisions, deletes most recent by default.

            filename_or_id can be the _id field value or the filename value.
        """
        assert self.FILES, "GridFS instance not initialized, run method 'set_database' with the desired database and try again"
        fs_doc = self.DOWNLOAD(filename_or_id, revision=revision)
        self.FILES.delete(fs_doc._id)
        fs_doc.close()

    def DOWNLOAD(self, filename_or_id:typing.Optional[typing.Union[str, DOC_ID]]=None, revision:int=-1, skip:int=None, limit:int=None, sort:int=-1, **query):
        """
            returns download stream of file if filename_or_id is provided, else returns a cursor of files matching query
        """
        assert self.FILES, "GridFS instance not initialized, run method 'set_database' with the desired database and try again"
        revision = int(revision)
        if filename_or_id:
            if isinstance(filename_or_id, DOC_ID.__supertype__):
                return self.FILES.open_download_stream(filename_or_id)
            else:
                return self.FILES.open_download_stream_by_name(filename_or_id, revision=revision)

        return self.FILES.find(query, limit=limit, skip=skip, sort=sort, no_cursor_timeout=True)

    def GET(self, collection, id_or_query:typing.Union[DOC_ID, typing.Dict, str]={}, sort:int=1, key:str="_id", count:bool=None, search:str=None, fields:dict=None, page:int=None, perpage:int=False, limit:int=None, after:str=None, before:str=None, empty=None, distinct:str=None, one:bool=False, **kwargs):
        """
            record can be either _id (accepts unicode form of ObjectId, as well as extended JSON bson format) or query

            Unless certain kwargs are used (count, distinct, one), behaviour is as follows:

            - if _id is recognized, one is set to true and will return the exact matching document
            - if query is recognized, will return MongoList response of cursor
            - if count is provided and _id is not recognized, returns number of documents in cursor
            - if distinct is provided, returns a unique list of the field values (accepts dot notation)
            - if one is provided, returns the first matching document of cursor

            kwargs count, distinct, one cannot be used together, priority is as follows if all are provided:

            1. count
            2. distinct
            3. one
        """
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
                        results.append(collection.count_documents(query, **kwargs))
                    else:
                        results.append(collection.estimated_document_count(**kwargs))
                elif distinct:
                    cursor = collection.find(query, **kwargs)
                    results.append(cursor.sort([(key, sort)]).distinct(distinct))
                elif perpage:
                    total = (page - 1) * perpage
                    cursor = collection.find(query, projection=fields, **kwargs)
                    results.append(MongoListResponse(cursor.sort([(key, sort)]).skip(total).limit(perpage)))
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
                            cursor = collection.find(query, fields, **kwargs).sort([(key, sort)]).limit(limit).count(with_limit_and_skip=True)
                        except:
                            cursor = collection.count_documents(query, limit=limit, hint=[(key, sort)], **kwargs)
                        results.append(cursor)
                    else:
                        cursor = collection.find(query, projection=fields, **kwargs).sort([(key, sort)]).limit(limit)
                        results.append(MongoListResponse(cursor))
                elif one:
                    val = collection.find_one(query, projection=fields, sort=[(key, sort)], **kwargs)
                    results.append(MongoDictResponse(val) if val else empty)
                else:
                    cursor = collection.find(query, projection=fields, **kwargs).sort([(key, sort)])
                    results.append(MongoListResponse(cursor))
            elif search:
                try:
                    cursor = collection.find({"$text": {"$search": search}})
                    if count:
                        results.append(cursor.count())
                    elif distinct:
                        results.append(cursor.distinct(distinct))
                    if perpage:
                        total = (page - 1) * perpage
                        results.append(MongoListResponse(cursor.sort([(key, sort)]).skip(total).limit(perpage)))
                    results.append(MongoListResponse(cursor.sort([(key, sort)])))
                except:
                    cursor = collection.command('textIndex', search=search)
                    if count:
                        results.append(cursor.count())
                    elif distinct:
                        results.append(cursor.distinct(distinct))
                    if perpage:
                        total = (page - 1) * perpage
                        results.append(MongoListResponse(cursor.sort([(key, sort)]).skip(total).limit(perpage)))
                    results.append(MongoListResponse(cursor.sort([(key, sort)])))
            else:
                raise Error("unidentified error")

        if number_of_results == 1:
            return results[0]
        else:
            return results

    def SEARCH(self, collection, search:str, **kwargs):
        """
            returns the results of querying textIndex of collection
        """
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        return self.GET(collection, search=search, **kwargs)

    def POST(self, collection, record_or_records:typing.Union[typing.List, typing.Dict]):
        """
            creates new record(s) and returns MongoDB response document
        """
        db = self.get_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        collection = db[collection]

        if isinstance(record_or_records, (list, tuple)):
            return collection.insert_many(record_or_records)
        elif isinstance(record_or_records, dict):
            return collection.insert_one(record_or_records)
        else:
            raise TypeError("invalid record_or_records type '{}' provided".format(type(record_or_records)))

    def PUT(self, collection, record_or_records:typing.Union[typing.List, typing.Dict]):
        """
            creates or replaces record(s) with exact _id provided, _id is required with record object(s)

            returns original document, if replaced
        """
        db = self.get_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        collection = db[collection]

        if isinstance(record_or_records, (list, tuple)):
            assert all([ record.get("_id", None) for record in record_or_records ]), "not all records provided contained an _id"
            return collection.insert_many(record_or_records, ordered=False)
        elif isinstance(record_or_records, dict):
            assert record_or_records.get("_id", None), "no _id provided"
            query = {"_id": record_or_records["_id"]}
            return collection.find_one_and_replace(query, record_or_records, upsert=True)
        else:
            raise TypeError("invalid record_or_records type '{}' provided".format(type(record_or_records)))

    def REPLACE(self, collection, original, replacement, upsert:bool=False):
        db = self.get_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection must be of type str"
        collection = db[collection]

        return collection.replace_one({"_id": original},
                    replacement, upsert=upsert)

    def PATCH(self, collection, id_or_query:typing.Union[DOC_ID, typing.Dict, typing.List, str], updates:typing.Union[typing.Dict, typing.List], upsert:bool=False, w:int=1):
        db = self.get_default_database()
        collection = collection or self._DEFAULT_COLLECTION
        assert collection, "collection not provided"
        collection = db[collection]

        if w != 1:
            WRITE = WriteConcern(w=w)
            collection = collection.with_options(write_concern=WRITE)

        if isinstance(id_or_query, (str, DOC_ID.__supertype__)):
            assert isinstance(updates, dict), "updates must be dict"
            id_or_query, _ = self._process_record_id_type(id_or_query)
            query = {"_id": id_or_query}

            set_on_insert_id = {"$setOnInsert": query}
            updates.update(set_on_insert_id)

            return collection.update_one(query, updates, upsert=upsert)
        elif isinstance(id_or_query, dict):
            assert isinstance(updates, dict), "updates must be dict"
            return collection.update_many(id_or_query, updates, upsert=upsert)
        elif isinstance(id_or_query, (tuple, list)):
            assert isinstance(updates, (tuple, list)), "updates must be list or tuple"

            results = []
            for i, _id in enumerate(id_or_query):
                _id, _ = self._process_record_id_type(id_or_query)
                query = {"_id": _id}
                set_on_insert_id = {"$setOnInsert": query}
                updates[i].update(set_on_insert_id)

                results.append(collection.update_one(query, updates[i], upsert=upsert))

            return results
        else:
            raise Error("unidentified error")


class SyncIODoc(SyncIOClient):
    """
        Custom MongoClient subclass with customizations for creating
        standardized documents and adding json schema validation.
    """
    _MONGO_URI = lambda _: getattr(Config, "MONGO_URI", None)
    _DOC_TYPE:str = None #: MongoDB collection to use
    _DOC_ID:str = "_id" #:
    _DOC_SAMPLE:str = None
    _DOC_SCHEMA:str = None
    _DOC_MARSHMALLOW:str = False
    _DOC_DEFAULTS:dict = {}
    _DOC_RESTRICTED_KEYS:list = []
    _DOC_ENUMS:list = []
    _DOC_SETTINGS:str = "settings"

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
        # INFO: sets the unique id field for the document, if any (cannot be _id)
        self._DOC_ID = doc_id or self._DOC_ID
        assert self._DOC_ID, "unique id field name must be of type str"

        for kwarg in kwargs.keys():
            if kwarg.lower() in ('doc_marshmallow', 'doc_defaults', 'doc_restricted_keys', "doc_enums"):
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

        SyncIOClient.__init__(self, **kwargs)

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
            return f"<cervmongo.SyncIODoc.{self._DOC_DB}.{self._DOC_TYPE}.{self._DOC_ID}:{_id}>"
        else:
            return "<cervmongo.SyncIODoc>"

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

    def _timestamp(self, value:str=None):
        if value:
            try:
                value = dateparse(value)
            except:
                value = current_datetime()
        else:
            value = current_datetime()
        return value

    def _guess_corresponding_fieldname(self, _type:str="unknown", related_field:str=""):
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

    def _related_record(self, collection=None, field:str="_id", value=False, additional:dict={}):
        additional.update({
            field: True
            })

        record = self.GET(collection, {field: value}, fields=additional, one=True, empty={})
        assert record, 'Error: No record found'

        record['key'] = field
        record['collection'] = collection
        record[field] = record[field]

        return record

    def load(self, _id=None):
        # If _id specified on init, load actual record versus blank template
        if _id:
            if self._DOC_ID:
                self.RECORD = self.GET(self._DOC_TYPE, {self._DOC_ID: _id}, one=True)
            else:
                self.RECORD = self.GET(self._DOC_TYPE, _id)
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

    def view(self, _id=False):
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

    def reload(self):
        assert self.RECORD.get("_id")
        self.RECORD = self.GET(self._DOC_TYPE, self.RECORD["_id"])

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "unique_id": self._DOC_ID
                }
            }

    def id(self):
        return self.RECORD.get(self._DOC_ID, None)

    def create(self, save:bool=False, trigger=None, template:str="{total}", query:dict={}, **kwargs):
        assert self.RECORD.get("_id") is None, """Cannot use create method on
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
            self.save(trigger=None)

        return {
            "data": self._p_r(self.RECORD),
            "details": {"unique_id": self._DOC_ID, "collection": self._DOC_TYPE, "_id": self.RECORD[self._DOC_ID]}
            }

    def push(self, **kwargs):
        assert self.RECORD.get("_id"), """Cannot use push method on
 a non-existing record. Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        self.PATCH(None, self.RECORD["_id"], {"$push": kwargs})
        self.reload()

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

    def pull(self, **kwargs):
        assert self.RECORD.get("_id"), """Cannot use pull method on
 a non-existing record. Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        self.PATCH(None, self.RECORD["_id"], {"$pull": kwargs})
        self.reload()

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

    def increment(self, query:dict={}, **kwargs):
        assert self.RECORD.get("_id"), """Cannot use increment method on
 a non-existing record. Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        query.update({"_id": self.RECORD["_id"]})

        self.PATCH(None, query, {"$inc": kwargs}, multi=True)
        self.reload()

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

    def update(self, query:dict={}, **kwargs):
        assert self.RECORD.get("_id"), """Cannot use increment method on
 a non-existing record. Use create method instead."""

        if "_id" in kwargs:
            kwargs.pop("_id")

        query.update({"_id": self.RECORD["_id"]})

        keys = list(kwargs.keys())
        old_values = [ self.RECORD.get(key, None) for key in keys ]

        self.PATCH(None, query, {"$set": kwargs}, multi=True)
        self.reload()

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

    def patch(self, save=False, trigger=None, **kwargs):
        assert self.RECORD.get("_id"), """Cannot use patch method on
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
            self.save(trigger=trigger)

        return {
            "data": self._p_r(self.RECORD),
            "details": {
                "processed": True,
                "diff": kwargs
                }
            }

    def save(self, trigger=None):
        _id = None

        if self._DOC_DEFAULTS:
            for key, value in self._DOC_DEFAULTS.items():
                if not self.RECORD.get(key, None):
                    self.RECORD[key] = value

        if self.RECORD.get("_id", None):
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
                self.RECORD["_id"] = _id
                self.PUT(None, self.RECORD)
            else:
                result = self.POST(None, self.RECORD)
                self.RECORD["_id"] = result.inserted_id
            if trigger:
                trigger()

        return {
            "data": self._p_r(self.RECORD),
            "details": {}
            }

    def close(self):
        # TODO: clean closing logic
        self.load()

        return {
            "data": self.RECORD,
            "details": {}
            }

    def add_relation(self, key, *args, **kwargs):
        self.RECORD[key] = self._related_record(*args, **kwargs)

        return {
            "data": self._p_r(self.RECORD),
            "details": {"key": key, "relation": self.RECORD[key]}
            }

    def add_timestamp(self, key, value=None, relation={}):
        self.RECORD[key] = self._timestamp(value)

        if relation:
            related_key = relation.pop("key", self._guess_corresponding_fieldname(_type="datetime", related_field=key))
            self.add_relation(related_key, **relation)
        return {
            "data": self._p_r(self.RECORD),
            "details": {"key": key, "value": self.RECORD[key]}
            }

    def add_object(self, field, object_name=None, key=None, value=None, **kwargs):
        if not object_name:
            if not key:
                self.RECORD[field] = {}
            else:
                self.RECORD[field] = {key: value if value else self.GENERATE_ID()}
        else:
            template_path = os_path.join(TEMPLATE_PATH, object_name + ".json")
            assert os.path.exists(template_path), "path does not exist"
            with open(template_path) as file1:
                self.RECORD[field] = json_load(file1.read())
            if key:
                self.RECORD[field][key] = value if value else self.GENERATE_ID()

        return {
            "data": self._p_r(self.RECORD),
            "details": {field: self.RECORD[field], "state": "unsaved"}
            }


def get_client() -> SyncIOClient:
    """returns SyncIOClient class"""
    return SyncIOClient

def get_doc() -> SyncIODoc:
    """returns SyncIODoc class"""
    return SyncIODoc
