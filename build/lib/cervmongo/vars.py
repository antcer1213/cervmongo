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
from bson.objectid import ObjectId
from pymongo import MongoClient
from .utils import json_dump

# INFO: Objects
try:
    from pydantic import BaseConfig, BaseModel
    MODEL = typing.TypeVar("DataModel", bound=BaseModel)
except:
    from dataclasses import dataclass
    MODEL = typing.TypeVar("DataModel", bound=dataclass)
YAML = typing.TypeVar("YAML Document", bound=str)
JSON = typing.TypeVar("JSON Document", bound=str)
ENUM = typing.TypeVar("Enum", bound=Enum)

# INFO: Static
ASCENDING = 1
DESCENDING = -1

# INFO: Fields
DOC_ID = typing.NewType("Document ID", ObjectId)
DETAILS = typing.NewType("Meta Details", dict)

class StringEnum(str, Enum): pass
class IntEnum(int, Enum): pass

# NOTE: defaults to recommended fields; overwrite depending on your schema, use utils.generate_enum
PAGINATION_SORT_FIELDS = Enum(value="Pagination Sort Fields", names=[(item, item) for item in ("_id", "created_datetime", "updated_datetime")])

# INFO: required to desired web response documents
class MetaConfig(type):
    def __str__(cls):
        return json_dump({
                        attr: value for attr, value in cls
                    }, pretty=True)

    def __iter__(cls):
        for attr, value in cls.__dict__.items():
            if attr.isupper():
                yield attr, value
