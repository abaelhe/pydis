"Core exceptions raised by the Redis client"
from ._compat import unicode


class RedisError(Exception):
    pass


# python 2.5 doesn't implement Exception.__unicode__. Add it here to all
# our exception types
if not hasattr(RedisError, '__unicode__'):
    def __unicode__(self):
        if isinstance(self.args[0], unicode):
            return self.args[0]
        return unicode(self.args[0])
    RedisError.__unicode__ = __unicode__


class UsageError(RedisError):
    pass


class AuthenticationError(RedisError):
    pass


class ConnectionError(RedisError):
    pass


class TimeoutError(RedisError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(RedisError):
    pass


class ResponseError(RedisError):
    pass


class DataError(RedisError):
    pass


class PubSubError(RedisError):
    pass


class WatchError(RedisError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass


class ReadOnlyError(ResponseError):
    pass


class LockError(RedisError, ValueError):
    "Errors acquiring or releasing a lock"
    # NOTE: For backwards compatability, this class derives from ValueError.
    # This was originally chosen to behave like threading.Lock.
    pass


SERVER_CLOSED_CONNECTION_ERROR = RedisError("Connection closed by server.")

EXCEPTION_CLASSES = {
    'ERR': {
        'max number of clients reached': ConnectionError,
        'value is not an integer or out of range': UsageError,
    },
    'LOADING': BusyLoadingError,
    'NOSCRIPT': NoScriptError,
    'EXECABORT': ExecAbortError,
    'READONLY': ReadOnlyError,
}
