#  models.py
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
from functools import partial
from .utils import snake2camel
from .utils import (
                    json_dump,
                    json_load,
                    yaml_dump,
                    yaml_load,
                    sort_list,
                    )
from .vars import (
                    DOC_ID,
                    DETAILS,
                    PAGINATION_SORT_FIELDS,
                    YAML,
                    JSON,
                    ASCENDING,
                    DESCENDING,
                    ObjectIdStr,
                    )
from pymongo.cursor import Cursor
try:
    from pydantic import BaseConfig, BaseModel
    SUPPORT_PYDANTIC = True

    class DefaultModel(BaseModel):
        """
        Intended for use as a base class for externally-facing models.
        Any models that inherit from this class will:
        * accept fields using snake_case or camelCase keys
        * use camelCase keys in the generated OpenAPI spec (when supported)
        * have orm_mode on by default

        - from fastapi-utils
        """

        class Config(BaseConfig):
            orm_mode = True
            allow_population_by_field_name = True
            alias_generator = partial(snake2camel, start_lower=True)
except:
    from dataclasses import dataclass
    SUPPORT_PYDANTIC = False

    @dataclass
    class DefaultModel:
        """
        Intended for use as a base class for externally-facing models.
        Any models that inherit from this class will:
        * accept fields using snake_case or camelCase keys
        * use camelCase keys in the generated OpenAPI spec (when supported)
        * create an orm_mode Config attrib and have it on by default

        - from fastapi-utils
        """

        class Config():
            orm_mode = True
            allow_population_by_field_name = True
            alias_generator = partial(snake2camel, start_lower=True)


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


class _GenericResponse(DefaultModel):
    def __repr__(self):
        return "GenericResponse"

    class Config:
        schema_extra = {
            'example': [
                {}
            ]
        }


class GenericResponse(_GenericResponse):
    """a premade web API friendly response object"""
    @property
    def __name__(self):
        return "GenericResponse"
    def __init__(self, *args, **kwargs):
        if SUPPORT_PYDANTIC:
            if args:
                data = args[0]
                if isinstance(data, (dict, _GenericResponse)):
                    return super().__init__(**dict(data))
                else:
                    return super().__init__(*data)
            else:
                return super().__init__(*args, **kwargs)
        else:
            super().__init__()
            if args:
                data = args[0]
                if isinstance(data, (dict, _GenericResponse)):
                    for data_kw in data:
                        setattr(self, data_kw, data[data_kw])
                        self.__dict__[data_kw] = data[data_kw]
            else:
                for kwarg in kwargs:
                    setattr(self, kwarg, kwargs[kwarg])
                    self.__dict__[kwarg] = kwargs[kwarg]


class _StandardResponse(DefaultModel):
    data: typing.Union[typing.Dict, typing.List, typing.Text, int, float, bool]
    details: DETAILS = DETAILS(...)

    def __repr__(self):
        return "StandardResponse"

    class Config:
        schema_extra = {
            'example': [
                {
                    'data': {"_id": "000000", "other": "data"},
                    'details': {"field": "other", "value": "data", "count": 1}
                }
            ]
        }
        json_encoders = {type(DOC_ID.__supertype__()): lambda x: str(x)}
        orm_mode = True
        allow_population_by_field_name = True
        alias_generator = partial(snake2camel, start_lower=True)

class StandardResponse(_StandardResponse):
    """a premade web API friendly response object"""
    def __init__(self, *args, **kwargs):
        if SUPPORT_PYDANTIC:
            if args:
                data = args[0]
                if isinstance(data, (dict, _StandardResponse)):
                    return super().__init__(**dict(data))
                else:
                    return super().__init__(*data)
            else:
                return super(_StandardResponse, self).__init__(*args, **kwargs)
        else:
            super().__init__()
            if args:
                data = args[0]
                if isinstance(data, (dict, _StandardResponse)):
                    for data_kw in data:
                        setattr(self, data_kw, data[data_kw])
                        self.__dict__[data_kw] = data[data_kw]
            else:
                for kwarg in kwargs:
                    setattr(self, kwarg, kwargs[kwarg])
                    self.__dict__[kwarg] = kwargs[kwarg]


class YAMLStandardResponse(_StandardResponse):
    """a premade web API friendly response object"""
    def __init__(self, *args, **kwargs):
        if SUPPORT_PYDANTIC:
            if args:
                data = args[0]
                if isinstance(data, (dict, _StandardResponse)):
                    return super().__init__(**dict(data))
                else:
                    return super().__init__(*data)
            else:
                return super(_StandardResponse, self).__init__(*args, **kwargs)
        else:
            super().__init__()
            if args:
                data = args[0]
                if isinstance(data, (dict, _StandardResponse)):
                    for data_kw in data:
                        setattr(self, data_kw, data[data_kw])
                        self.__dict__[data_kw] = data[data_kw]
            else:
                for kwarg in kwargs:
                    setattr(self, kwarg, kwargs[kwarg])
                    self.__dict__[kwarg] = kwargs[kwarg]

    def __str__(self) -> YAML:
        return yaml_dump(self.dict())


class MongoDictResponse(dict):
    """the normal response for a single document when using cervmongo.main.SyncIOClient or cervmongo.aio.AsyncIOClient"""

    pass

class MongoListResponse(list):
    """the normal response for multiple documents when using cervmongo.main.SyncIOClient or cervmongo.aio.AsyncIOClient"""
    _sort = ASCENDING
    _index = 0
    def __init__(self, cursor=[]):
        if isinstance(cursor, Cursor):
            self._cursor = cursor
            self._cursor.rewind()
            self.__self__ = self._cursor
        else:
            self._cursor = None
            super().__init__(cursor)

    def __repr__(self):
        if self._cursor:
            return str(self._cursor)
        else:
            return super().__repr__()

    def __del__(self):
        if self._cursor:
            self._cursor.close()
        else:
            self.clear()

    def get(self) -> typing.Union[Cursor, typing.List]:
        """returns cursor or list instance"""
        if self._cursor:
            return self._cursor
        else:
            return self

    def close(self) -> None:
        """closes cursor or clears list"""
        if self._cursor:
            self._cursor.close()
        else:
            self.clear()

    # ~ @property
    # ~ def retrieved(self):
        # ~ """The number of documents retrieved so far"""
        # ~ if self._cursor:
            # ~ return self._cursor.retrieved
        # ~ else:
            # ~ return self._index + 1

    def rewind(self) -> None:
        """rewinds cursor, if any"""
        if self._cursor:
            self._cursor.rewind()

    def distinct(self, field:str="_id") -> typing.List[typing.Any]:
        """returns list of distinct values based on field, defaults to '_id'. supports dot notation for nested values."""
        if self._cursor:
            self._cursor.rewind()
            return sorted(self._cursor.distinct(field), reverse=True if self._sort == -1 else False)
        else:
            try:
                if "." in field:
                    results = set()
                    fields = field.split(".")
                    total = len(fields) - 1 # INFO: 0 start index
                    for item in self:
                        for index, field in enumerate(fields):
                            if not index == total:
                                if field.isdigit():
                                    try:
                                        item = item[int(field)]
                                    except:
                                        continue
                                elif field in item:
                                    item = item[field]
                                else:
                                    continue
                            else:
                                if field.isdigit():
                                    try:
                                        results.add(item.index(int(field)))
                                    except:
                                        continue
                                elif field in item:
                                    results.add(item[field])
                                else:
                                    continue
                    return sorted(list(results), reverse=True if self._sort == -1 else False)
                else:
                    return sorted([item[field] for item in self if field in item], reverse=True if self._sort == -1 else False)
            except:
                return []

    def count(self) -> int:
        """returns the count of the number of records in cursor or list results"""
        if self._cursor:
            try:
                self._cursor.rewind()
                return self._cursor.count(with_limit_and_skip=True)
            except:
                col = self._cursor._Cursor__collection
                query = self._cursor._Cursor__spec or {}
                limit = self._cursor._Cursor__limit
                sort = self._cursor._Cursor__ordering
                if sort:
                    sort = sort.items()
                return col.count_documents(query, limit=limit, hint=sort)
        else:
            return len(self)

    def sort(self, sort:int=1, key:str="_id") -> 'self':
        """sorts cursor results or list results, default is cervmongo.ASC (int 1)

        Options:
            - cervmongo.ASC or 1
            - cervmongo.DESC or -1
        """
        assert sort in (-1, 1), "sort must be option -1, 1"
        self._sort = sort
        if self._cursor:
            self._cursor.rewind()
            self._cursor = self._cursor.sort([(key, sort)])
            self.__self__ = self._cursor
            return self
        else:
            if sort == -1:
                super().sort(reverse=True, key=lambda item, field=key: sort_list(item, field))
            else:
                super().sort(key=lambda item, field=key: sort_list(item, field))
            return self

    def list(self) -> typing.List[typing.Dict]:
        """returns a new list representation of the current cursor"""
        if self._cursor:
            self._cursor.rewind()
            return MongoListResponse(list(self._cursor))
        else:
            return self


