#  config.py
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
__all__ = ["Config"]

import os
from .utils import (
                    getenv_boolean,
                    logger,
                    json_dump,
                    )
from .vars import (
                    MONGODB_URI,
                    )
from .models import MetaConfig
import typing
import logging
ch = logging.StreamHandler()

ConfigClass = typing.TypeVar('Config')


class Defaults:
    MONGO_DB:typing.Optional[str] = os.getenv("MONGO_DB", None)
    MONGO_HOST:str = os.getenv("MONGO_HOST", "127.0.0.1")
    MONGO_PORT:int = int(os.getenv("MONGO_PORT", 27017))
    MONGO_REPLICA_SET:typing.Optional[str] = os.getenv("MONGO_REPLICA_SET", None)
    MONGO_MAX_POOL_SIZE:int = int(os.getenv("MONGO_MAX_POOL_SIZE", 20))
    MONGO_MIN_POOL_SIZE:int = int(os.getenv("MONGO_MIN_POOL_SIZE", 10))
    MONGO_USER:typing.Optional[str] = os.getenv("MONGO_USER", None)
    MONGO_PASSWORD:typing.Optional[str] = os.getenv("MONGO_PASSWORD", None)
    MONGO_URI:typing.Optional[str] = os.getenv("MONGO_URI", None)
    DEBUG_LEVEL:int = int(os.getenv("DEBUG_LEVEL", logging.WARNING))
    JSON_SAMPLE_PATH:str = os.getenv("JSON_SAMPLE_PATH", "./")
    JSON_SCHEMA_PATH:str = os.getenv("JSON_SCHEMA_PATH", "./")

class Config(metaclass=MetaConfig):
    """
        MongoDB and cervmongo settings, loaded initially by environmental variables
    """
    MONGO_DB:typing.Optional[str] = Defaults.MONGO_DB #: The MongoDB database to use
    MONGO_HOST:str = Defaults.MONGO_HOST #: The host server for MongoDB
    MONGO_PORT:int = Defaults.MONGO_PORT #: The port for connection to host server
    MONGO_REPLICA_SET:typing.Optional[str] = Defaults.MONGO_REPLICA_SET #: The name of the replica set, if any
    MONGO_MAX_POOL_SIZE:int = Defaults.MONGO_MAX_POOL_SIZE
    MONGO_MIN_POOL_SIZE:int = Defaults.MONGO_MIN_POOL_SIZE
    MONGO_USER:typing.Optional[str] = Defaults.MONGO_USER #: The username for MongoDB connection
    MONGO_PASSWORD:typing.Optional[str] = Defaults.MONGO_PASSWORD #: The password for MongoDB connection
    MONGO_URI:typing.Optional[str] = Defaults.MONGO_URI #: The MongoDB URI that will be used when accessing clients
    DEBUG_LEVEL:int = Defaults.DEBUG_LEVEL #: The level at which to display information, defaults to logging.warning
    JSON_SAMPLE_PATH:str = Defaults.JSON_SAMPLE_PATH #: For use with JSON sample records to simplify schema process of new documents
    JSON_SCHEMA_PATH:str = Defaults.JSON_SCHEMA_PATH #: For use with JSON schema documents for validating new documents on creation

    @classmethod
    def reset(cls) -> None:
        """
            resets config values to the first values assigned when cervmongo was imported
        """
        for attr, value in Defaults.__dict__.items():
            if attr.isupper():
                setattr(cls, attr, value)

    @classmethod
    def set_debug_level(cls, debug_level:int) -> None:
        """
            sets debug level of application, default is logging.warning
        """
        assert isinstance(debug_level, int), "debug level must be a valid logging int"
        cls.DEBUG_LEVEL = debug_level
        logger.setLevel(debug_level)
        ch.setLevel(debug_level)

    @classmethod
    def set_mongo_db(cls:typing.Type[ConfigClass], database_name:str) -> ConfigClass:
        """
            assigns the MONGO_DB var in config and regenerates mongodb uri

            returns Class
        """
        assert isinstance(database_name, str), "database must be a string"
        cls.MONGO_DB = database_name
        cls.generate_mongo_uri()
        return cls

    @classmethod
    def set_mongo_host(cls:typing.Type[ConfigClass], host:str) -> ConfigClass:
        """
            assigns the MONGO_HOST var in config and regenerates mongodb uri

            returns Class
        """
        if host:
            assert isinstance(host, str), "host must be a string"
            cls.MONGO_HOST = host
            cls.generate_mongo_uri()
        return cls

    @classmethod
    def set_mongo_port(cls:typing.Type[ConfigClass], port:int) -> ConfigClass:
        """
            assigns the MONGO_PORT var in config and regenerates mongodb uri

            returns Class
        """
        if port:
            assert isinstance(port, int), "port must be an integer"
            cls.MONGO_PORT = port
            cls.generate_mongo_uri()
        return cls

    @classmethod
    def set_mongo_replica_set(cls:typing.Type[ConfigClass], replica_set=None) -> ConfigClass:
        """
            assigns the MONGO_REPLICA_SET var in config and regenerates mongodb uri

            returns Class
        """
        assert isinstance(replica_set, (str, type(None))), "database must be a string or NoneType"
        cls.MONGO_REPLICA_SET = replica_set
        cls.generate_mongo_uri()
        return cls

    @classmethod
    def generate_mongo_uri(cls) -> MONGODB_URI:
        """
            uses config class values to generate mongodb uri and returns connection string
        """
        if cls.MONGO_REPLICA_SET:
            cls.MONGO_URI = f"mongodb://{cls.MONGO_HOST}:{cls.MONGO_PORT}/{cls.MONGO_DB}?replicaSet={cls.MONGO_REPLICA_SET}"
        else:
            cls.MONGO_URI = f"mongodb://{cls.MONGO_HOST}:{cls.MONGO_PORT}/{cls.MONGO_DB}"
        return cls.MONGO_URI

    @classmethod
    def reload(cls) -> None:
        """
            Reloads config class values from environment, falls back to current values if none found
        """
        cls.MONGO_DB = os.getenv("MONGO_DB", cls.MONGO_DB)
        cls.MONGO_HOST = os.getenv("MONGO_HOST", cls.MONGO_HOST)
        cls.MONGO_PORT = int(os.getenv("MONGO_PORT", cls.MONGO_PORT))
        cls.MONGO_REPLICA_SET = os.getenv("MONGO_REPLICA_SET", cls.MONGO_REPLICA_SET)
        cls.MONGO_MAX_POOL_SIZE = int(os.getenv("MONGO_MAX_POOL_SIZE", cls.MONGO_MAX_POOL_SIZE))
        cls.MONGO_MIN_POOL_SIZE = int(os.getenv("MONGO_MIN_POOL_SIZE", cls.MONGO_MIN_POOL_SIZE))
        cls.MONGO_USER = os.getenv("MONGO_USER", cls.MONGO_USER)
        cls.MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", cls.MONGO_PASSWORD)
        cls.MONGO_URI = os.getenv("MONGO_URI", cls.MONGO_URI)
        cls.DEBUG_LEVEL = int(os.getenv("DEBUG_LEVEL", cls.DEBUG_LEVEL))
        cls.JSON_SAMPLE_PATH = os.getenv("JSON_SAMPLE_PATH", cls.JSON_SAMPLE_PATH)
        cls.JSON_SCHEMA_PATH = os.getenv("JSON_SCHEMA_PATH", cls.JSON_SCHEMA_PATH)

    @classmethod
    def reload_from_file(cls, env_path:str=".env", override:bool=False) -> None:
        """
            Re-assign class variables using .env config file
        """
        import dotenv
        dotenv.load_dotenv(dotenv_path=env_path, override=override)
        cls.reload()

    @classmethod
    def reload_from_stream(cls, stream:typing.IO, override:bool=False) -> None:
        """
            Re-assign class variables using stream in .env config format
        """
        import dotenv
        dotenv.load_dotenv(stream=stream, override=override)
        cls.reload()

Config.generate_mongo_uri()

logger.setLevel(Config.DEBUG_LEVEL)
# create console handler and set level to debug
ch.setLevel(Config.DEBUG_LEVEL)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)
