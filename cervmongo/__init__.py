#  cervmongo
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
__all__ = [
        "get_client", "get_doc", "SUPPORT_GRIDFS",
        "get_async_client", "get_async_doc", "SUPPORT_ASYNCIO_CLIENT", "SUPPORT_ASYNCIO_BUCKET",
        "SUPPORT_PYDANTIC",
        "ASC", "DESC",
        "quick_load_client",
        "get_config"
        ]

from ._version import (
            __version_info__,
            __version__,
            )

from . import (
            main,
            aio,
            models,
            vars,
            config,
            extra,
            )
from .extra import convenience as convenience

quick_load_client = convenience.quick_load_client

get_client = main.get_client
get_doc = main.get_doc
SUPPORT_GRIDFS = main.SUPPORT_GRIDFS

get_async_client = aio.get_async_client
get_async_doc = aio.get_async_doc
SUPPORT_ASYNCIO_CLIENT = aio.SUPPORT_ASYNCIO_CLIENT
SUPPORT_ASYNCIO_BUCKET = aio.SUPPORT_ASYNCIO_BUCKET

SUPPORT_PYDANTIC = models.SUPPORT_PYDANTIC

ASC = vars.ASCENDING
DESC = vars.DESCENDING

config = config.Config

def get_config() -> config:
    """returns the Config class to set MongoDB configuration"""
    return config
