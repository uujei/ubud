import os
from datetime import datetime

import parse
from clutter.aws import get_secrets
from dotenv import load_dotenv

from ..const import KST, KEY_PARSER, KEY_RULE, CATEGORY


# parse redis address
def parse_redis_addr(redis_addr, decode_responses=True):
    """
    Examples
    --------
    >>> parse_redis_addr("localhost:6379")
        {
            "host": localhost,
            "port": 6379,
            "decode_responses": True
        }
    >>> parse_redis_addr("/var/run/redis/redis-server.sock")
        {
            "unix_socket_path": "/var/run/redis/redis-server.sock",
            "decode_responses": True,
        }
    """
    DEFAULT_PORT = 6379

    if redis_addr.endswith(".sock"):
        return {
            "unix_socket_path": redis_addr,
            "decode_responses": True,
        }

    x = redis_addr.split(":")
    host = x[0]
    port = x[1] if len(x) > 1 else DEFAULT_PORT

    return {
        "host": host,
        "port": int(port),
        "decode_responses": decode_responses,
    }


# split delimiter
def split_delim(*x, delim=","):
    """
    Examples
    --------
    >>> split_delim("a,b,c", "1,2,3", "x,y,z")
        (["a", "b", "c"], ["1", "2", "3"], ["x", "y", "z"])
    """
    return tuple([__x.strip() for __x in _x.split(delim)] for _x in x)


# for logging
def repr_conf(x):
    _x = {k: v for k, v in x.items() if k != "secret"}
    _secret = x.get("secret")
    if _secret is not None:
        _s = []
        for k, v in _secret.items():
            apiKey = v.get("apiKey")
            apiSecret = v.get("apiSecret")
            if apiKey is not None and apiSecret is not None:
                _s += [f"{k}({v['apiKey'][:4]}****/{v['apiSecret'][:4]}****)"]
        _x.update({"secret": ", ".join(_s)})
    return ", ".join([f"{k}: {str(v)}" for k, v in _x.items() if not k.endswith("token")])


# load secret
def load_secrets(secret_key):
    if secret_key is None:
        load_dotenv()
        return {
            "upbit": {
                "apiKey": os.getenv("UPBIT_API_KEY"),
                "apiSecret": os.getenv("UPBIT_API_SECRET"),
            },
            "bithumb": {
                "apiKey": os.getenv("BITHUMB_API_KEY"),
                "apiSecret": os.getenv("BITHUMB_API_SECRET"),
            },
            "ftx": {
                "apiKey": os.getenv("FTX_API_KEY"),
                "apiSecret": os.getenv("FTX_API_SECRET"),
            },
            "influxdb": {
                "influxdb_url": os.getenv("INFLUXDB_URL"),
                "influxdb_org": os.getenv("INFLUXDB_ORG"),
                "influxdb_token": os.getenv("INFLUXDB_TOKEN"),
            },
        }
    secrets = get_secrets(secret_key)
    return {
        "upbit": {
            "apiKey": secrets["UPBIT_API_KEY"],
            "apiSecret": secrets["UPBIT_API_SECRET"],
        },
        "bithumb": {
            "apiKey": secrets["BITHUMB_API_KEY"],
            "apiSecret": secrets["BITHUMB_API_SECRET"],
        },
        "ftx": {
            "apiKey": secrets["FTX_API_KEY"],
            "apiSecret": secrets["FTX_API_SECRET"],
        },
        "influxdb": {
            "influxdb_url": secrets["INFLUXDB_URL"],
            "influxdb_org": secrets["INFLUXDB_ORG"],
            "influxdb_token": secrets["INFLUXDB_TOKEN"],
        },
    }


# timestamp to string datetime (w/ ISO format)
def ts_to_strdt(ts, _float=True):
    # _flaot is deprecated
    return datetime.fromtimestamp(ts).astimezone(KST).isoformat(timespec="microseconds")


# universal parser
def key_parser(key):
    return KEY_PARSER[key.split("/", 2)[1]](key).named


# universal key maker
def key_maker(**kwargs):
    """
    [NOTE] It Returns Key Without Topic!
    """
    return "/".join([kwargs.get(str(k)) for k in KEY_RULE[kwargs[CATEGORY]]][1:])
