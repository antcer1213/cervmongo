#  utils.py
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
from bson import json_util, SON
from traceback import format_exc as _traceback
import datetime
import string
import re
import os
import io

import uuid
from enum import Enum
from typing import List, Optional, Sequence, Type, TypeVar, Union

from bson.objectid import ObjectId
from .vars import TYPES, SCHEMA_TYPES
import inspect
import yaml
import mimetypes
import urllib

import logging

logger = logging.getLogger("cervmongo")

PUNCTUATION_TRANSLATOR = str.maketrans('', '', string.punctuation)
GENERIC_MIMETYPE = "application/octet-stream"

def detect_mimetype(filename) -> str:
    mimetype = 'application/octet-stream'
    try:
        import magic
        mime = magic.Magic(magic_file="bin/magic", mime=True)

        if isinstance(filename, str):
            mimetype = mime.from_file(filename)
        else:
            mimetype = mime.id_buffer(filename)
            filename.seek(0)
    except:
        mimetype = mimetypes.guess_type(filename)[0]

    return mimetype


def flatten_dict(dictionary: dict) -> dict:
    new_dict = {}
    for key, value in dictionary.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                new_dict["{}.{}".format(key, subkey)] = subvalue
                if isinstance(subvalue, dict):
                    subvalue = flatten_dict(subvalue)
                    for subsubkey, subsubvalue in subvalue.items():
                        new_dict["{}.{}.{}".format(key, subkey, subsubkey)] = subvalue
        elif isinstance(value, (tuple, list)):
            for subkey, subvalue in enumerate(value):
                new_dict["{}.{}".format(key, subkey)] = subvalue
        else:
            new_dict[key] = value

    return new_dict


def file_and_fileobj(fileobj):
    if isinstance(fileobj, str):
        if os.path.exists(fileobj):
            return open(fileobj, 'rb')
        else:
            return fileobj
    elif isinstance(fileobj, (io.TextIOBase, io.BufferedIOBase, io.RawIOBase, io.IOBase)):
        fileobj.seek(0)
        return fileobj
    else:
        raise TypeError("fileobj is not a valid str or file-like obj; received '{}'".format(type(fileobj)))

def get_file_meta_information(fileobj, filename:str=None, content_type:str=None, extension:str=None, **kwargs) -> dict:
    filename = filename or getattr(fileobj, "filename", str(ObjectId()))
    content_type = content_type or getattr(fileobj, "content_type", None)
    extension = extension or getattr(fileobj, "extension", None)

    if not extension:
        if "." in filename:
            extension = ".{}".format(filename.split(".")[-1].lower())
        elif content_type:
            extension = mimetypes.guess_extension(content_type)
        else:
            if os_path.exists(filename):
                content_type = detect_mimetype(filename)
                extension = mimetypes.guess_extension(content_type)
            else:
                content_type = detect_mimetype(fileobj)
                extension = mimetypes.guess_extension(content_type)

    if not content_type:
        if extension:
            content_type = mimetypes.types_map.get(extension, GENERIC_MIMETYPE)
        else:
            content_type = getattr(fileobj, "content_type", detect_mimetype(fileobj))

    if filename and extension:
        filename = filename.lower()
        extension = extension.lower()
        if not extension in filename:
            filename = "{}{}".format(filename, extension)

    assert all((extension, content_type)), "extension and/or content_type not found"

    return {
        "filename": filename,
        "content_type": content_type,
        "extension": extension,
        "aliases": [filename.split(".")[0], filename.upper()]
        }

def dict_to_query(dictionary:dict) -> str:
    return urllib.parse.urlencode(dictionary)

def sort_list(item, field:str):
    try:
        fields = field.split(".")
        total = len(fields) - 1
        for index, field in enumerate(fields):
            if index != total:
                if field.isdigit():
                    item = item[int(field)]
                elif field in item:
                    item = item[field]
                else:
                    return None
            else:
                if field.isdigit():
                    return item[int(field)]
                elif field in item:
                    return item[field]
                else:
                    return None
    except:
        return None

def getenv_boolean(var_name, default_value=False):
    result = default_value
    env_value = os.getenv(var_name)
    if env_value is not None:
        result = env_value.upper() in ("TRUE", "1")
    return result

def current_datetime(alt:str=False) -> datetime.datetime:
    """Returns current datetime object by default. Accepts alternate format for string format result."""
    if alt:
        return datetime.datetime.now().strftime(alt)
    return datetime.datetime.now()

def current_date(alt:str=False) -> datetime.date:
    """Returns current date object by default. Accepts alternate format for string format result."""
    if alt:
        return datetime.date.today().strftime(alt)
    return datetime.date.today()

def clean_kwargs(*, ONLY:list=[], kwargs:dict={}) -> dict:
    """Allows for sanitization of keyword args before passing to another function"""
    if ONLY:
        return {only_key: kwargs.get(only_key, None) for only_key in ONLY if only_key in kwargs}
    else:
        return kwargs

def parse_string_header(string:str) -> str:
    """For use when parsing nested data from tabular data formats, such as spreadsheets"""
    if string.startswith("{"):
        return ".".join(re.findall(r'(\w+)\b', string, re.DOTALL))
    else:
        return string

def format_string_for_id(string:str) -> str:
    """Cleans string to allow for functional, readable, and permissible MongoDB ID"""
    string = string.translate(PUNCTUATION_TRANSLATOR)
    string = string.replace(" ", "")
    return string.lower()

def return_value_from_dict(dictionary:dict, key:str, if_not:str=" "):
    value = dictionary.get(key if key != "__id" else "_id", if_not)

    if isinstance(value, datetime.date):
        value = value.strftime('%Y/%m/%d')
    elif isinstance(value, ObjectId):
        pass
    elif isinstance(value, (set, tuple, list, iter)):
        try:
            value = ", ".join(value)
        except:
            # INFO: silent fail, intentional
            value = value
    else:
        try:
            value = value
        except:
            # INFO: silent fail, intentional
            pass
    if instance(value, str):
        return value.replace('"', '\\"')
    else:
        return value

def snake2camel(snake:str, start_lower:bool=False) -> str:
    """
    Converts a snake_case string to camelCase.
    The `start_lower` argument determines whether the first letter in the generated camelcase should
    be lowercase (if `start_lower` is True), or capitalized (if `start_lower` is False).
    """
    camel = snake.title()
    camel = re.sub("([0-9A-Za-z])_(?=[0-9A-Z])", lambda m: m.group(1), camel)
    if start_lower:
        camel = re.sub("(^_*[A-Z])", lambda m: m.group(1).lower(), camel)
    return camel

def camel2snake(camel:str) -> str:
    """
    Converts a camelCase string to snake_case.
    """
    snake = re.sub(r"([a-zA-Z])([0-9])", lambda m: f"{m.group(1)}_{m.group(2)}", camel)
    snake = re.sub(r"([a-z0-9])([A-Z])", lambda m: f"{m.group(1)}_{m.group(2)}", snake)
    return snake.lower()

def objectid_representer(dumper, data):
    return dumper.represent_scalar("!_id", str(data))

def objectid_constructor(loader, data):
    return ObjectId(loader.construct_scalar(data))

yaml.SafeDumper.add_representer(ObjectId, objectid_representer)
yaml.add_constructor('!_id', objectid_constructor)

def _get_class_that_defined_method(meth):
    if inspect.ismethod(meth):
        for cls in inspect.getmro(meth.__self__.__class__):
            if cls.__dict__.get(meth.__name__) is meth:
                return cls
        meth = meth.__func__  # fallback to __qualname__ parsing
    if inspect.isfunction(meth):
        cls = getattr(inspect.getmodule(meth),
                      meth.__qualname__.split('.<locals>', 1)[0].rsplit('.', 1)[0])
        if isinstance(cls, type):
            return cls
    return None

def generate_new_id() -> str:
    return str(uuid.uuid4())

def ensure_enums_to_strs(items: Union[Sequence[Union[Enum, str]], Type[Enum]]):
    str_items = []
    for item in items:
        if isinstance(item, Enum):
            str_items.append(str(item.value))
        else:
            str_items.append(str(item))
    return str

def yaml_dump(data:dict) -> str:
    return yaml.safe_dump(data, default_flow_style=False)

def yaml_load(data, _file:bool=False) -> dict:
    if _file:
        return yaml.load(open(data, 'r'))
    else:
        return yaml.safe_load(data)

def json_dump(data:dict, pretty:bool=False) -> str:
    if pretty:
        return json_util.dumps(data, indent=4, sort_keys=True)
    else:
        return json_util.dumps(data)

def json_load(data:str) -> dict:
    return json_util.loads(data)

def clean_traceback() -> str:
    traceback = _traceback
    # TODO: cleaning logic, to dict, maybe make class?
    return traceback

def silent_drop_kwarg(kwargs:dict, key:str, reason:str=""):
    if reason:
        logger.debug(f"dropping key {key} for reason {reason}")
    else:
        logger.debug(f"dropping key {key}")
    return kwargs.pop(key)

# TODO: custom jsonschema validator


# INFO: tools to use with JSON samples
def type_from_schema(schema_type:str):
    """retrieves type function based on JSON sample inferred data schema type"""
    schema_type = schema_type.lower()
    return TYPES[schema_type]


def schema_from_dict(dictionary:dict, additional:dict={}):
    """creates a simple JSON schema from JSON sample document"""
    schema = {"type": "object", "required": [], "properties" : {}}

    for key, value in dictionary.items():
        if ":" in key:
            key, _type = key.strip().split(":")
            _type = SCHEMA_TYPES[_type]
        else:
            if isinstance(value, str):
                _type = "str"
            elif isinstance(value, float):
                _type = "float"
            elif isinstance(value, int):
                _type = "int"
            elif isinstance(value, dict):
                _type = "dict"
            elif isinstance(value, ObjectId):
                _type = "oid"
            elif isinstance(value, datetime.datetime):
                _type = "datetime"
            elif isinstance(value, datetime.date):
                _type = "date"
            elif isinstance(value, bool):
                _type = "bool"
            else:
                raise TypeError("unrecognized type '{}' for value '{}'".format(type(value), value))
            _type = SCHEMA_TYPES[_type]

        required = False
        if key.endswith("*"):
            key = key.strip("*")
            required = True

        schema["properties"][key] = {
                "type": _type
            }

        if required:
            if not key in schema["required"]:
                schema["required"].append(key)

    return schema
