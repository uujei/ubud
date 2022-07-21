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


def split_delim(*x, delim=","):
    return tuple([__x.strip() for __x in _x.split(delim)] for _x in x)


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
