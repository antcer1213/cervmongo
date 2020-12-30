[![GitHub stars](https://img.shields.io/github/stars/antcer1213/cervmongo)](https://github.com/antcer1213/cervmongo/stargazers) [![GitHub forks](https://img.shields.io/github/forks/antcer1213/cervmongo)](https://github.com/antcer1213/cervmongo/network) [![GitHub issues](https://img.shields.io/github/issues/antcer1213/cervmongo)](https://github.com/antcer1213/cervmongo/issues) [![GitHub license](https://img.shields.io/github/license/antcer1213/cervmongo)](https://github.com/antcer1213/cervmongo) [![Build Status](https://travis-ci.com/antcer1213/cervmongo.svg?branch=master)](https://travis-ci.com/antcer1213/cervmongo)


# cervmongo

A convenience-based approach to MongoDB w/ Python that works as a drop-in replacement to the IO `pymongo` and AIO `motor` respective clients. Packaged due to excessive reuse in private projects, where it was used to facilitate agile, rapid development of multiple web applications. Database is intentionally loaded by default using URI (can be changed after instance is created), with an optional default collection param. Heads up, most commonly used methods of the client are in UPPERCASE, to ensure names are not taken by the parent classes and keep them nice and short.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install cervmongo.

```bash
pip install cervmongo
```

## Usage

```python
import cervmongo

col_client = cervmongo.quick_load_client(
                                database="test_db",
                                collection="test_col",
                                replica_set=None,
                                async_=False
                                ) # convenience function

col_recs = col_client.GET() # returns cursor as very cool MongoListResponse
col_recs.count() # returns number of total documents in cursor
col_recs.list() # returns list of documents in cursor
col_recs.distinct() # returns list of unique values, default field '_id'
col_recs.sort() # returns self, allows sorting
# example of creating a document
result = col_client.POST({"key": "value"}) # returns pymongo Response document
# example of fetching a document
col_client.GET(result.inserted_id) # returns the created document as dict
# example of an update (patch)
col_client.PATCH(result.inserted_id, {"$set": {"key": "newvalue"}}) # update the document
# example of a query
col_client.GET({"key": "newvalue"}) # returns the cursor resulting from query
# will replace existing document if exists, else create new document with _id provided
col_client.PUT({"_id": result.inserted_id, "key": "finalvalue"})
# will delete document
col_client.DELETE(result.inserted_id) # returns deleted document


# OPTIONALLY
count = col_client.GET(count=True) # returns number of total documents in cursor
count_of_query = col_client.GET({"key": "value"}, count=True)
distinct_values_of_field_key = col_client.GET(distinct="key")
distinct_ids = col_client.GET(distinct=True) # _id is default
distinct_ids_with_query = col_client.GET({"key": "value"}, distinct=True)
sorted_query_one = col_client.GET(key="key", sort=cervmongo.DESC) # sorts in descending order by field 'key'
sorted_query_two = col_client.GET({"key": "value"}, key="key", sort=cervmongo.DESC)


# OPTIONALLY
cervmongo.get_config().set_mongo_db("test_db")
client_class = cervmongo.get_client() # gets client class
client = client_class() # SyncIOClient (subclass of pymongo.MongoClient
# ~ motor.motor_asyncio.AsyncIOMotorClient, if async)
# same functionality as col_client above,
# but collection must be explicitly declared as first arg and
# query or record _id, if any, has to be second arg
# Example:
count = client.GET("test_col", count=True)
query_results = client.GET("test_col", {"key": "value"})


# OTHER FUNCTIONALITY
cursor_paged_results = client.PAGINATED_QUERY(after=None, before=None, limit=5) # returns cursor-based initial page
time_paged_results = client.PAGINATED_QUERY(sort="created_date", after=None, before=None, limit=5) # returns time-based initial page
offset_paged_results = client.PAGINATED_QUERY(page=1, limit=5) # returns offset-based initial page
count_of_multi_cols = client.GET(["test_col1", "test_col2"], count=True) # returns list of counts
multi_col_results = client.GET(["test_col1", "test_col2"], {
                "$or": [
                    {"child": "value"},
                    {"related_child": "value"}
                    ]}) # returns list of cursors matching query

```

### TODO:
 1. full testing on AIO Client & Doc classes
 2. finish type hints, function hints, and docstrings for readability
 3. pydantic first-class treatment
 4. restructuring/refactoring/optimizing
 5. web api
    - datatable mongodb plugin + web endpoint
    - restful fastapi server - extra
    - possible datatable html + javascript code generator

### REQUIRES
 - python 3.6+
 - python packages:
    - `pymongo`
    - `python-dateutil`
    - `jsonschema`
    - `dataclasses`

### RECOMMENDED
 - `motor` (for aio options)
 - `pydantic` (for obj/model validation, ORM)
 - `marshmallow` (json schema validation)
 - `python-dotenv` 0.12.0>= (for configuration of MongoDB client and cervmongo)
    - cervmongo Settings
        - __DEBUG_LEVEL__ (default 30, i.e. `logging.WARNING`)

    - mongodb Settings
        - Can optionally provide either:
            - __MONGO_HOST__ (_default "127.0.0.1"_)
            - __MONGO_PORT__ (_default 27017_)
            - __MONGO_DB__ (_default None_)
            - __MONGO_REPLICA_SET__ (_default None_)
            - __MONGO_MAX_POOL_SIZE__ (_default 20_)
            - __MONGO_MIN_POOL_SIZE__ (_default 10_)
            - __MONGO_USER__ (_default None_)
            - __MONGO_PASSWORD__ (_default None_)
        - or:
            - __MONGO_URI__ (_default None, ex. "mongodb://localhost:27017/app?replicaSet=appSet"_)
            - For more information on a MongoDB URI, see here: [Connection String URI Format](https://docs.mongodb.com/manual/reference/connection-string/).

## Documentation

Full documentation available [here](https://cerver.info/packages/cervmongo/).

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
