#  vars.py
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
import typing
from enum import Enum
import datetime
from bson.objectid import ObjectId
from pymongo import MongoClient
from dateutil.parser import parse as dateparse

# INFO: Objects
try:
    from pydantic import BaseConfig, BaseModel
    MODEL = typing.NewType("DataModel", BaseModel)
except:
    from dataclasses import dataclass
    MODEL = typing.NewType("DataModel", dataclass)

# INFO: Custom Types
MONGODB_URI = typing.NewType("MongoDB URI", str)
YAML = typing.NewType("YAML Document", str)
JSON = typing.NewType("JSON Document", str)
ENUM = typing.NewType("Enum", Enum)

# INFO: Static
ASCENDING = 1
DESCENDING = -1

# INFO: Fields
OBJECT_ID = OBJ_ID = ObjectId
DOC_ID = typing.NewType("Document ID", OBJ_ID)
DETAILS = typing.NewType("Meta Details", dict)

class StringEnum(str, Enum): pass
class IntEnum(int, Enum): pass

# NOTE: defaults to recommended fields; overwrite depending on your schema, use utils.generate_enum
PAGINATION_SORT_FIELDS = Enum(value="Pagination Sort Fields", names=[(item, item) for item in ("_id", "created_datetime", "updated_datetime")])


class ObjectIdStr(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, ObjectId):
            raise ValueError("Not a valid ObjectId")
        return str(v)


def str2bool(v):
    return str(v).lower() in ("yes", "true", "t", "1")


def str2datetime(v):
    if isinstance(v, (datetime.date, datetime,datetime)):
        return v
    else:
        return dateparse(v)

TYPES = {}
TYPES["str"] = str
TYPES["float"] = float
TYPES["int"] = int
TYPES["abs"] = abs
TYPES["dict"] = dict
TYPES["oid"] = ObjectId
TYPES["bool"] = ObjectId
TYPES["date"] = TYPES["datetime"] = str2datetime
TYPES["bool"] = str2bool

SCHEMA_TYPES = {}
SCHEMA_TYPES["str"] = SCHEMA_TYPES["string"] = SCHEMA_TYPES["text"] = "str"
SCHEMA_TYPES["number"] = SCHEMA_TYPES["num"] = SCHEMA_TYPES["decimal"] = SCHEMA_TYPES["float"] = "float"
SCHEMA_TYPES["int"] = SCHEMA_TYPES["integer"] = "int"
SCHEMA_TYPES["absolute"] = SCHEMA_TYPES["abs"] = "abs"
SCHEMA_TYPES["object"] = SCHEMA_TYPES["dict"] = SCHEMA_TYPES["obj"] = "dict"
SCHEMA_TYPES["oid"] = SCHEMA_TYPES["objectid"] = SCHEMA_TYPES["object_id"] = "oid"
SCHEMA_TYPES["date"] = "date"
SCHEMA_TYPES["datetime"] = "datetime"
SCHEMA_TYPES["bool"] = SCHEMA_TYPES["boolean"] = "bool"


