#!/usr/bin/env python
# coding:utf-8

"""
@Author: Abael He<abaelhe@icloud.com>
@file: parser.py
@time: 21/11/2018 12:37
@license: All Rights Reserved, Abael.com
"""

import six, os, sys, stat, warnings, hiredis, logging, hashlib
from datetime import datetime
from collections import deque
from functools import partial
from itertools import imap, izip
from tornado.gen import Return, coroutine
from tornado.locks import Condition
from tornado.escape import native_str, utf8
from tornado.tcpclient import TCPClient
from . import defaults
from .tornado_adapter import Future, thread, FutureCommander, FutureConnection, BYTES_EMPTY, \
    initial_future_from_kwargs_callback

LOG = logging.getLogger(__name__)

urlparse = six.moves.urllib_parse.urlparse
parse_qs = six.moves.urllib_parse.parse_qs
unquote = six.moves.urllib_parse.unquote
iteritems = six.iteritems

SYM_EMPTY = ''
SYM_STAR = '*'
SYM_DOLLAR = '$'
SYM_CRLF = '\r\n'
SYM_EMPTY = ''
USER_RESPONSE = 'USER_RESPONSE'

FALSE_STRINGS = ('0', 'F', 'FALSE', 'N', 'NO')

(SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, MESSAGE, PMESSAGE) = (1, 2, 3, 4, 5, 6)

REDIS_PUB_COMMANDS = set(['MESSAGE', 'PMESSAGE'])
REDIS_SUB_COMMANDS = set(['SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE'])

REDIS_PUBSUB_REPLIES = {
    'subscribe': SUBSCRIBE,
    'unsubscribe': UNSUBSCRIBE,
    'psubscribe': PSUBSCRIBE,
    'punsubscribe': PUNSUBSCRIBE,

    'message': MESSAGE,
    'pmessage': PMESSAGE
}


SENTINEL_STATE_TYPES = {
    'can-failover-its-master': int,
    'config-epoch': int,
    'down-after-milliseconds': int,
    'failover-timeout': int,
    'info-refresh': int,
    'last-hello-message': int,
    'last-ok-ping-reply': int,
    'last-ping-reply': int,
    'last-ping-sent': int,
    'master-link-down-time': int,
    'master-port': int,
    'num-other-sentinels': int,
    'num-slaves': int,
    'o-down-time': int,
    'pending-commands': int,
    'parallel-syncs': int,
    'port': int,
    'quorum': int,
    'role-reported-time': int,
    's-down-time': int,
    'slave-priority': int,
    'slave-repl-offset': int,
    'voted-leader-epoch': int
}


def to_bool(value):
    if value is None or value == '':
        return None
    if isinstance(value, basestring) and value.upper() in FALSE_STRINGS:
        return False
    return bool(value)


URL_QUERY_ARGUMENT_PARSERS = {
    'socket_timeout': float,
    'socket_connect_timeout': float,
    'socket_keepalive': to_bool,
    'retry_on_timeout': to_bool,
    'max_connections': int,
}


class RedisError(Exception):
    pass


class RedisProtoError(RedisError):
    pass


class RedisReplyError(RedisError):
    pass


def parse_url(url, db=None, decode_components=False, **kwargs):
    """
    Return a connection pool configured from the given URL.

    For example::

        redis://[:password]@localhost:6379/0
        rediss://[:password]@localhost:6379/0
        unix://[:password]@/path/to/socket.sock?db=0

    Three URL schemes are supported:

    - ```redis://``
      <https://www.iana.org/assignments/uri-schemes/prov/redis>`_ creates a
      normal TCP socket connection
    - ```rediss://``
      <https://www.iana.org/assignments/uri-schemes/prov/rediss>`_ creates
      a SSL wrapped TCP socket connection
    - ``unix://`` creates a Unix Domain Socket connection

    There are several ways to specify a database number. The parse function
    will return the first specified option:
        1. A ``db`` querystring option, e.g. redis://localhost?db=0
        2. If using the redis:// scheme, the path argument of the url, e.g.
           redis://localhost/0
        3. The ``db`` argument to this function.

    If none of these options are specified, db=0 is used.

    The ``decode_components`` argument allows this function to work with
    percent-encoded URLs. If this argument is set to ``True`` all ``%xx``
    escapes will be replaced by their single-character equivalents after
    the URL has been parsed. This only applies to the ``hostname``,
    ``path``, and ``password`` components.

    Any additional querystring arguments and keyword arguments will be
    passed along to the ConnectionPool class's initializer. The querystring
    arguments ``socket_connect_timeout`` and ``socket_timeout`` if supplied
    are parsed as float values. The arguments ``socket_keepalive`` and
    ``retry_on_timeout`` are parsed to boolean values that accept
    True/False, Yes/No values to indicate state. Invalid types cause a
    ``UserWarning`` to be raised. In the case of conflicting arguments,
    querystring arguments always win.

    """
    url = urlparse(url)
    url_options = {}

    for name, value in iteritems(parse_qs(url.query)):
        if value and len(value) > 0:
            parser = URL_QUERY_ARGUMENT_PARSERS.get(name)
            if parser:
                try:
                    url_options[name] = parser(value[0])
                except (TypeError, ValueError):
                    warnings.warn(UserWarning("Invalid value for `%s` in connection URL." % name))
            else:
                url_options[name] = value[0]

    if decode_components:
        password = unquote(url.password) if url.password else None
        path = unquote(url.path) if url.path else None
        hostname = unquote(url.hostname) if url.hostname else None
    else:
        password = url.password
        path = url.path
        hostname = url.hostname

    # We only support redis:// and unix:// schemes.
    if url.scheme == 'unix':
        url_options.update({'password': password, 'path': path, 'connection_class': TCPClient, })

    else:
        url_options.update({'host': hostname, 'port': url.port or 6379, 'password': password, })

        # If there's a path argument, use it as the db argument if a querystring value wasn't specified
        if 'db' not in url_options and path:
            try:
                url_options['db'] = int(path.replace('/', ''))
            except (AttributeError, ValueError):
                pass

        if url.scheme == 'rediss':
            url_options['connection_class'] = TCPClient

    # last shot at the db value
    url_options['db'] = int(url_options.get('db', db or 0))

    # update the arguments from the URL values
    kwargs.update(url_options)

    # backwards compatability
    if 'charset' in kwargs:
        warnings.warn(DeprecationWarning('"charset" is deprecated. Use "encoding" instead'))
        kwargs['encoding'] = kwargs.pop('charset')
    if 'errors' in kwargs:
        warnings.warn(DeprecationWarning('"errors" is deprecated. Use "encoding_errors" instead'))
        kwargs['encoding_errors'] = kwargs.pop('errors')

    return kwargs


def timestamp_to_datetime(response):
    "Converts a unix timestamp to a Python datetime object"
    if not response:
        return None
    try:
        response = int(response)
    except ValueError:
        return None
    return datetime.fromtimestamp(response)


def parse_debug_object(response):
    "Parse the results of Redis's DEBUG OBJECT command into a Python dict"
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with a name
    response = native_str(response)
    response = 'type:' + response
    response = dict([kv.split(':') for kv in response.split()])

    # parse some expected int values from the string response
    # note: this cmd isn't spec'd so these may not appear in all redis versions
    int_fields = ('refcount', 'serializedlength', 'lru', 'lru_seconds_idle')
    for field in int_fields:
        if field in response:
            response[field] = int(response[field])

    return response


def parse_object(response, infotype):
    "Parse the results of an OBJECT command"
    if infotype in ('idletime', 'refcount'):
        return int_or_none(response)
    return response


def parse_info(response):
    "Parse the result of Redis's INFO command into a Python dict"
    info = {}
    response = native_str(response)

    def get_value(value):
        if ',' not in value or '=' not in value:
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
        else:
            sub_dict = {}
            for item in value.split(','):
                k, v = item.rsplit('=', 1)
                sub_dict[k] = get_value(v)
            return sub_dict

    for line in response.splitlines():
        if line and not line.startswith('#'):
            if line.find(':') != -1:
                key, value = line.split(':', 1)
                info[key] = get_value(value)
            else:
                # if the line isn't splittable, append it to the "__raw__" key
                info.setdefault('__raw__', []).append(line)

    return info


def parse_sentinel_state(item):
    result = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
    flags = set(result['flags'].split(','))
    for name, flag in (('is_master', 'master'), ('is_slave', 'slave'),
                       ('is_sdown', 's_down'), ('is_odown', 'o_down'),
                       ('is_sentinel', 'sentinel'),
                       ('is_disconnected', 'disconnected'),
                       ('is_master_down', 'master_down')):
        result[name] = flag in flags
    return result


def parse_sentinel_master(response):
    return parse_sentinel_state(imap(native_str, response))


def parse_sentinel_masters(response):
    result = {}
    for item in response:
        state = parse_sentinel_state(imap(native_str, item))
        result[state['name']] = state
    return result


def parse_sentinel_slaves_and_sentinels(response):
    return [parse_sentinel_state(imap(native_str, item)) for item in response]


def parse_sentinel_get_master(response):
    return response and (response[0], int(response[1])) or None


def pairs_to_dict(response):
    "Create a dict given a list of key/value pairs"
    it = iter(response)
    return dict(izip(it, it))


def pairs_to_dict_typed(response, type_info):
    it = iter(response)
    result = {}
    for key, value in izip(it, it):
        if key in type_info:
            try:
                value = type_info[key](value)
            except:
                # if for some reason the value can't be coerced, just use
                # the string value
                pass
        result[key] = value
    return result


def zset_score_pairs(response, **options):
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not options.get('withscores'):
        return response
    score_cast_func = options.get('score_cast_func', float)
    it = iter(response)
    return list(izip(it, imap(score_cast_func, it)))


def sort_return_tuples(response, **options):
    """
    If ``groups`` is specified, return the response as a list of
    n-element tuples with n being the value found in options['groups']
    """
    if not response or not options['groups']:
        return response
    n = options['groups']
    return list(izip(*[response[i::n] for i in range(n)]))


def int_or_none(response):
    if response is None:
        return None
    return int(response)


def float_or_none(response):
    if response is None:
        return None
    return float(response)


def bool_ok(response):
    return native_str(response) == 'OK'


def parse_client_list(response, **options):
    clients = []
    for c in native_str(response).splitlines():
        clients.append(dict([pair.split('=') for pair in c.split(' ')]))
    return clients


def parse_config_get(response, **options):
    response = [native_str(i) if i is not None else None for i in response]
    return response and pairs_to_dict(response) or {}


def parse_scan(response, **options):
    cursor, r = response
    return long(cursor), r


def parse_hscan(response, **options):
    cursor, r = response
    return long(cursor), r and pairs_to_dict(r) or {}


def parse_zscan(response, **options):
    score_cast_func = options.get('score_cast_func', float)
    cursor, r = response
    it = iter(r)
    return long(cursor), list(izip(it, imap(score_cast_func, it)))


def parse_slowlog_get(response, **options):
    return [{
        'id': item[0],
        'start_time': int(item[1]),
        'duration': int(item[2]),
        'command': ' '.join(item[3])
    } for item in response]


def parse_cluster_info(response, **options):
    return dict([line.split(':') for line in response.splitlines() if line])


def _parse_node_line(line):
    line_items = line.split(' ')
    node_id, addr, flags, master_id, ping, pong, epoch, \
    connected = line.split(' ')[:8]
    slots = [sl.split('-') for sl in line_items[8:]]
    node_dict = {
        'node_id': node_id,
        'flags': flags,
        'master_id': master_id,
        'last_ping_sent': ping,
        'last_pong_rcvd': pong,
        'epoch': epoch,
        'slots': slots,
        'connected': True if connected == 'connected' else False
    }
    return addr, node_dict


def parse_cluster_nodes(response, **options):
    raw_lines = response
    if isinstance(response, basestring):
        raw_lines = response.splitlines()
    return dict([_parse_node_line(line) for line in raw_lines])


def parse_georadius_generic(response, **options):
    if options['store'] or options['store_dist']:
        # `store` and `store_diff` cant be combined
        # with other command arguments.
        return response

    if type(response) != list:
        response_list = [response]
    else:
        response_list = response

    if not options['withdist'] and not options['withcoord'] \
            and not options['withhash']:
        # just a bunch of places
        return [native_str(r) for r in response_list]

    cast = {
        'withdist': float,
        'withcoord': lambda ll: (float(ll[0]), float(ll[1])),
        'withhash': int
    }

    # zip all output results with each casting functino to get
    # the properly native Python value.
    f = [native_str]
    f += [cast[o] for o in ['withdist', 'withhash', 'withcoord'] if options[o]]
    return [
        list(map(lambda fv: fv[0](fv[1]), zip(f, r))) for r in response_list
    ]


def parse_pubsub_numsub(response, **options):
    return list(zip(response[0::2], response[1::2]))


def string_keys_to_dict(key_string, callback):
    return dict.fromkeys(key_string.split(), callback)


def dict_merge(*dicts):
    merged = {}
    for d in dicts:
        merged.update(d)
    return merged


RESPONSE_PREPROCESSORS = {
    'AUTH': bool,
    'BGREWRITEAOF': lambda r: True,
    'BGSAVE': lambda r: True,
    'BITCOUNT': int,
    'BITPOS': int,
    'BLPOP': lambda r: r and tuple(r) or None,
    'BRPOP': lambda r: r and tuple(r) or None,
    'CLIENT GETNAME': lambda r: r and native_str(r),
    'CLIENT KILL': bool_ok,
    'CLIENT LIST': parse_client_list,
    'CLIENT SETNAME': bool_ok,
    'CLUSTER ADDSLOTS': bool_ok,
    'CLUSTER COUNT-FAILURE-REPORTS': lambda x: int(x),
    'CLUSTER COUNTKEYSINSLOT': lambda x: int(x),
    'CLUSTER DELSLOTS': bool_ok,
    'CLUSTER FAILOVER': bool_ok,
    'CLUSTER FORGET': bool_ok,
    'CLUSTER INFO': parse_cluster_info,
    'CLUSTER KEYSLOT': lambda x: int(x),
    'CLUSTER MEET': bool_ok,
    'CLUSTER NODES': parse_cluster_nodes,
    'CLUSTER REPLICATE': bool_ok,
    'CLUSTER RESET': bool_ok,
    'CLUSTER SAVECONFIG': bool_ok,
    'CLUSTER SET-CONFIG-EPOCH': bool_ok,
    'CLUSTER SETSLOT': bool_ok,
    'CLUSTER SLAVES': parse_cluster_nodes,
    'CONFIG GET': parse_config_get,
    'CONFIG RESETSTAT': bool_ok,
    'CONFIG SET': bool_ok,
    'DEBUG OBJECT': parse_debug_object,
    'DECRBY': int,
    'DEL': int,
    'EXISTS': bool,
    'EXPIRE': bool,
    'EXPIREAT': bool,
    'FLUSHALL': bool_ok,
    'FLUSHDB': bool_ok,
    'GEOADD': int,
    'GEODIST': float,
    'GEOHASH': lambda r: list(map(native_str, r)),
    'GEOPOS': lambda r: list(
        map(lambda ll: (float(ll[0]), float(ll[1])) if ll is not None else None, r)),
    'GEORADIUS': parse_georadius_generic,
    'GEORADIUSBYMEMBER': parse_georadius_generic,
    'GETBIT': int,
    'HDEL': int,
    'HEXISTS': bool,
    'HGETALL': lambda r: r and pairs_to_dict(r) or {},
    'HINCRBYFLOAT': float,
    'HLEN': int,
    'HMSET': bool,
    'HSCAN': parse_hscan,
    'HSTRLEN': int,
    'INCRBY': int,
    'INCRBYFLOAT': float,
    'INFO': parse_info,
    'LASTSAVE': timestamp_to_datetime,
    'LINSERT': int,
    'LLEN': int,
    'LPUSH': lambda r: isinstance(r, (long, int)) and r or native_str(r) == 'OK',
    'LPUSHX': int,
    'LSET': bool_ok,
    'LTRIM': bool_ok,
    'MOVE': bool,
    'MSET': bool_ok,
    'MSETNX': bool,
    'OBJECT': parse_object,
    'PERSIST': bool,
    'PFADD': int,
    'PFCOUNT': int,
    'PFMERGE': bool_ok,
    'PING': lambda r: native_str(r) == 'PONG',
    'PSETEX': bool,
    'PTTL': lambda r: r >= 0 and r or None,
    'PUBSUB NUMSUB': parse_pubsub_numsub,
    'RANDOMKEY': lambda r: r and r or None,
    'RENAME': bool_ok,
    'RENAMENX': bool,
    'RPUSH': lambda r: isinstance(r, (long, int)) and r or native_str(r) == 'OK',
    'RPUSHX': int,
    'SADD': int,
    'SAVE': bool_ok,
    'SCAN': parse_scan,
    'SCARD': int,
    'SCRIPT EXISTS': lambda r: list(imap(bool, r)),
    'SCRIPT FLUSH': bool_ok,
    'SCRIPT KILL': bool_ok,
    'SCRIPT LOAD': utf8,
    'SDIFF': lambda r: r and set(r) or set(),
    'SDIFFSTORE': int,
    'SELECT': bool_ok,
    'SENTINEL GET-MASTER-ADDR-BY-NAME': parse_sentinel_get_master,
    'SENTINEL MASTER': parse_sentinel_master,
    'SENTINEL MASTERS': parse_sentinel_masters,
    'SENTINEL MONITOR': bool_ok,
    'SENTINEL REMOVE': bool_ok,
    'SENTINEL SENTINELS': parse_sentinel_slaves_and_sentinels,
    'SENTINEL SET': bool_ok,
    'SENTINEL SLAVES': parse_sentinel_slaves_and_sentinels,
    'SET': lambda r: r and native_str(r) == 'OK',
    'SETBIT': int,
    'SETEX': bool,
    'SETNX': bool,
    'SETRANGE': int,
    'SHUTDOWN': bool_ok,
    'SINTER': lambda r: r and set(r) or set(),
    'SINTERSTORE': int,
    'SISMEMBER': bool,
    'SLAVEOF': bool_ok,
    'SLOWLOG GET': parse_slowlog_get,
    'SLOWLOG LEN': int,
    'SLOWLOG RESET': bool_ok,
    'SMEMBERS': lambda r: r and set(r) or set(),
    'SMOVE': bool,
    'SORT': sort_return_tuples,
    'SREM': int,
    'SSCAN': parse_scan,
    'STRLEN': int,
    'SUNION': lambda r: r and set(r) or set(),
    'SUNIONSTORE': int,
    'TIME': lambda x: (int(x[0]), int(x[1])),
    'TTL': lambda r: r >= 0 and r or None,
    'UNWATCH': bool_ok,
    'WATCH': bool_ok,
    'ZADD': int,
    'ZCARD': int,
    'ZINCRBY': float_or_none,
    'ZLEXCOUNT': int,
    'ZRANGE': zset_score_pairs,
    'ZRANGEBYSCORE': zset_score_pairs,
    'ZRANK': int_or_none,
    'ZREM': int,
    'ZREMRANGEBYLEX': int,
    'ZREMRANGEBYRANK': int,
    'ZREMRANGEBYSCORE': int,
    'ZREVRANGE': zset_score_pairs,
    'ZREVRANGEBYSCORE': zset_score_pairs,
    'ZREVRANK': int_or_none,
    'ZSCAN': parse_zscan,
    'ZSCORE': float_or_none,
}

RESPONSE_PREPROCESSORS_GET = RESPONSE_PREPROCESSORS.get



def set_response_preprocessor(command, callback):
    "Set a custom Response Callback"
    RESPONSE_PREPROCESSORS[command] = callback


def redis_parse_packet_future(command_name, reply_future, **options):
    " Parses a response from the Redis server "
    packet_callback = RESPONSE_PREPROCESSORS_GET(command_name, None)
    if packet_callback is not None:
        reply_preprocessed = packet_callback(reply_future.result(), **options)
        reply_future.set_result(reply_preprocessed, update=True)


def redis_parse_packet(client, command_name, packet, **options):
    " Parses a response from the Redis server "
    packet_callback = RESPONSE_PREPROCESSORS_GET(command_name, None)
    if packet_callback is not None:
        reply_preprocessed = packet_callback(packet, **options)
        return reply_preprocessed


class Pypeline(object):
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.

    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.

    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """

    UNWATCH_COMMANDS = set(('DISCARD', 'EXEC', 'UNWATCH'))

    def __init__(self, transport, transaction=True, shard_hint=None):
        self._transport = transport

        self._transaction = transaction
        self._shard_hint = shard_hint
        self._explicit_transaction = False
        self._command_stack = deque()
        self._scripts = set()

        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        self.reset()

    def __len__(self):
        return len(self._command_stack)

    @property
    def transport(self):
        return self._transport

    def reset(self):
        transport = self._transport
        reset_future = transport.Future()
        reset_future.add_done_callback(lambda cf: setattr(transport, 'watching', False))
        reset_future.add_done_callback(lambda cf: setattr(self, '_explicit_transaction', False))
        reset_future.add_done_callback(lambda cf: self._command_stack.clear())
        reset_future.add_done_callback(lambda cf: self._scripts.clear())

        if transport.watching:
            transport._command_call('UNWATCH').add_done_callback(lambda uf: reset_future.set_result(uf.result()))
        else:
            reset_future.set_result(True)

    def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self._scripts)
        _immediate = self._immediate_execute_command

        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists_future = _immediate('SCRIPT EXISTS', *[s.sha for s in scripts])

        def _load_script_sha(_script, _script_future):
            _script.sha = _script_future.result()

        def _load_script_exists(_scripts, _exists_future):
            _exists = _exists_future.result()
            if not all(_exists):
                for s, _exist in izip(_scripts, _exists):
                    if not _exist:
                        _script_future = _immediate('SCRIPT LOAD', s.script)
                        _script_future.add_done_callback(partial(_load_script_sha, s))

        exists_future.add_done_callback(partial(_load_script_exists, scripts))

    def watch(self, *names):
        "Watches the values at keys ``names``"
        if self._explicit_transaction:
            raise RedisError('Cannot issue a WATCH after a MULTI')

        cmd_name = 'WATCH'
        transport = self._transport
        parse_result_callback = partial(redis_parse_packet_future, cmd_name)
        watch_future = self._transport._command_call(cmd_name, *names, callback=parse_result_callback)
        watch_future.add_done_callback(lambda wf: setattr(transport, 'watching', True))
        return watch_future

    def unwatch(self):
        "Unwatches all previously specified keys"
        cmd_name = 'UNWATCH'
        transport = self._transport
        if transport.watching is False:
            uw_future = transport.Future()
            uw_future.set_result(False)
        else:
            parse_result_callback = partial(redis_parse_packet_future, cmd_name)
            uw_future = self._transport._command_call(cmd_name, callback=parse_result_callback)
            uw_future.add_done_callback(lambda _: setattr(transport, 'watching', False if _.result() is True else True))
        return uw_future

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self._explicit_transaction:
            raise RedisError('Cannot issue nested calls to MULTI')
        if len(self._command_stack) > 0:
            raise RedisError('Commands without an initial WATCH have already been issued')
        self._explicit_transaction = True

    def stack_call(self, *args, **kwargs):
        """Stacks a redis command inside the object.

        The syntax is the same than the call() method a Client class.

        Args:
            *args: full redis command as variable length argument list.

        Examples:
            >>> pipeline = Pipeline()
            >>> pipeline.stack_call("HSET", "key", "field", "value")
            >>> pipeline.stack_call("PING")
            >>> pipeline.stack_call("INCR", "key2")
        """

        cmd_name = args[0]
        transport = self._transport
        if (transport.watching or cmd_name == 'WATCH') and not self._explicit_transaction:
            # before start a TRANSACTION block.
            return self._immediate_call(args, kwargs)

        return self._pipeline_call(args, kwargs)

    # COMMAND EXECUTION AND PROTOCOL PARSING
    def execute(self, raise_on_error=True):
        "Execute all the commands in the current pipeline"
        if len(self._command_stack) < 1:
            return []
        if self._scripts:
            self.load_scripts()

        if self._transport.subscribed:
            raise RedisError('PUBSUB MODE pipeline(%s) command execution !', id(self))

        if self._transaction or self._explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        return execute(self._transport, self._command_stack, raise_on_error)

    def _immediate_call(self, args, options):
        "Execute a command and return a parsed response"
        cmd_name = args[0]
        transport = self._transport

        transport.redis_sync_watch_state(cmd_name)
        parse_result_callback = partial(redis_parse_packet_future, cmd_name)
        return transport._command_call(*args, callback=parse_result_callback, **options)

    def _pipeline_call(self, args, options):
        """
        Stage a command to be executed when execute() is next called

        Returns the current Pipeline object back so commands can be
        chained together, such as:

        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')

        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self._command_stack.append((args, options))
        return self

    def _execute_transaction(self, transport, commands, raise_on_error):
        call_future = transport.Future()

        def _redis_transaction_multi(reply):
            assert native_str(reply) == 'OK', 'Error begin transaction command: `MULTI`.'

        def _redis_transaction_command(_idx, _cmd, _options, reply):
            if not isinstance(reply, str) or native_str(reply) != 'QUEUED':
                raise RedisError('Error transaction command[%sth]:%s, %s, exception:%s' % (_idx, (_cmd,), _options, reply))

        def _redis_transaction_exec(_cmds, _future_callback, packets):
            if packets is None:
                raise RedisError('`WATCH` variables modified during this TRANSACTION.')
            for _idx, _r in enumerate(packets):
                (_cmd_name, _cmd_options) = _cmds[_idx]
                packet_preprocessor = RESPONSE_PREPROCESSORS_GET(_cmd_name)
                if packet_preprocessor is not None:
                    try:
                        preprocessor_value = packet_preprocessor(_r, **cmd_options)
                    except RedisReplyError:
                        preprocessor_value = sys.exc_info()[1]
                    packets[_idx] = preprocessor_value
            _future_callback(packets)

        idx = 0
        cmds = []

        stacked_commands = [(('MULTI',), {})]
        bulk_callback = [_redis_transaction_multi]
        while len(commands) > 0:
            idx += 1
            cmd = commands.popleft()
            stacked_commands.append(cmd)
            cmd_name = cmd[0][0]
            cmd_options = cmd[1]
            cmds.append((cmd_name, cmd_options))
            bulk_callback.append(partial(_redis_transaction_command, idx, cmd_name, cmd_options))
        stacked_commands.append((('EXEC',), {}))
        bulk_callback.append(partial(_redis_transaction_exec, cmds, call_future.set_result))
        packets_partial = partial(self.transport._command_packs, stacked_commands)
        transport.future_packet_send(packets_partial, bulk_callback)
        return call_future

    def _execute_pipeline(self, transport, stacked_commands, raise_on_error):
        # build up all commands into a single request to increase network perf

        class RedisPipelineCommand:
            def __init__(self, _cmd_num, _future_callback, _raise_first_error=True):
                self.num = _cmd_num
                self.cmds = []
                self.pkts = []
                self.future_callback = _future_callback
                self.raise_first_error = _raise_first_error

            def push_command(self, _cmd_name, _options):
                self.cmds.append((_cmd_name, _options))

            def __call__(self, _packet):
                self.pkts.append(_packet)
                if len(self.pkts) == self.num:
                    assert len(self.cmds) == self.num, 'commands number and replied packets number must be EQUAL !'

                    ppg = RESPONSE_PREPROCESSORS_GET
                    rfe = self.raise_first_error
                    for _idx, (_cmd_name, _options) in enumerate(self.cmds):
                        _r = self.pkts[_idx]
                        if rfe and isinstance(_r, RedisReplyError):
                            raise RedisError('Error Pipeline Command[%s]%s: %s' %(_idx, _cmd_name, _r.message))

                        _packet_preprocessor = ppg(_cmd_name)
                        if _packet_preprocessor is not None:
                            try:
                                _preprocessor_value = _packet_preprocessor(_r, **_options)
                            except RedisReplyError:
                                _preprocessor_value = sys.exc_info()[1]
                            self.pkts[_idx] = _preprocessor_value

                    _pkts = self.pkts
                    self.pkts = None
                    self.future_callback(_pkts)

        call_future = transport.Future()

        callback_queue = []
        callback_queue_append = callback_queue.append

        pplc = RedisPipelineCommand(len(stacked_commands), call_future.set_result, raise_on_error)
        pplc_push_command = pplc.push_command

        for idx, (args, options) in enumerate(stacked_commands):
            pplc_push_command(args[0], options)
            callback_queue_append(pplc)

        packets_partial = partial(self.transport._command_packs, stacked_commands)
        transport.future_packet_send(packets_partial, callback_queue)
        return call_future


class Script(object):
    "An executable Lua script object returned by ``register_script``"

    def __init__(self, client, script):
        self.registered_client = client
        self.script = script
        # Precalculate and store the SHA1 hex digest of the script.

        if isinstance(script, basestring):
            # We need the encoding from the client in order to generate an
            # accurate byte representation of the script
            script = client.redis_encode(script)
        self.sha = hashlib.sha1(script).hexdigest()

    @coroutine
    def __call__(self, keys=[], args=[], client=None):
        "Execute the script, passing any required ``args``"
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        if isinstance(client, Pypeline):
            # Make sure the pipeline can register the script before executing.
            client = client.transport
            client.scripts.add(self)

        ret = None
        try:
            ret = yield client.script_evalsha(self.sha, len(keys), *args)
        except RedisError:
            # Maybe the client is pointed to a differnet server than the client
            # that created this instance?
            # Overwrite the sha just in case there was a discrepancy.
            self.sha = yield client.script_load(self.script)
            ret = yield client.script_evalsha(self.sha, len(keys), *args)

        raise Return(ret)


class Pydis(FutureCommander, FutureConnection):
    """ Manages TCP communication to and from a Redis server
        >>> redis = Client(db=7)
        >>> keys = yield redis.call('KEYS', '*')
        >>> cameras = yield redis.call('SET', '123', '456')
        >>> bigchunk = '1' * 1024 * 1024 * 128
        >>> t_1 = time.time()
        >>> cameras = yield redis.call('SET', 'bigchunk', bigchunk)
        >>> t_2 = time.time()
        >>> ret = yield redis.call('GET', 'bigchunk')
        >>> t_3 = time.time()
        >>> print('t_set_bigchunk: %s' , t_2 - t_1)
        >>> print('t_get_bigchunk: %s', t_3 - t_2)
        >>> print('t_set_get: %s', t_3 - t_1)

        >>> pipeline = Pipeline()
        >>> pipeline.stack_call("HSET", "key", "field", "value")
        >>> pipeline.stack_call("PING")
        >>> pipeline.stack_call("INCR", "key2")
        >>> redis.call(pipeline)

    """

    description_format = "Connection<host=%(host)s,port=%(port)s,db=%(db)s>"
    UNWATCH_COMMANDS = set(('DISCARD', 'EXEC', 'UNWATCH'))
    PYDIS_DEFAULTS = dict(host='localhost', port=6379, db=0, password=None, encoding='utf-8', path=None, debug=False,
                          io_loop=None, socket_timeout=None, socket_connect_timeout=defaults.CONNECT_TIMEOUT,
                          socket_read_size=65536, socket_type=0, socket_keepalive=False, socket_keepalive_options=None,
                          encoding_errors='strict', decode_responses=False,
                          max_buffer_size=defaults.MAX_BUFFER_SIZE * defaults.DATA_UNIT,
                          retry_on_timeout=False, shard_hint=None, ignore_subscribe_messages=False)

    def __init__(self,  **kwargs):
        A = self.PYDIS_DEFAULTS.copy()
        A.update(kwargs)

        self.connection_kwargs = A

        self.Future = Future
        self.debug = A.get('debug')

        self.db = A.get('db')
        self.password = A.get('password')
        self.port = A.get('path') or A.get('port')
        self.is_unix_socket = os.path.exists(str(A.get('port'))) and stat.S_ISSOCK(os.stat(str(A.get('port'))).st_mode)
        self.host = '127.0.0.1' if self.is_unix_socket else A.get('host')

        self._pid = os.getpid()
        self._thread_main_id = thread.get_ident()
        self._thread_io_id = None
        self._encoding = A.get('encoding') or sys.getdefaultencoding()
        self._decode_responses = A.get('decode_responses')
        self._encoding_errors = A.get('encoding_errors')

        self._socket_timeout = A.get('socket_timeout')
        self._socket_connect_timeout = A.get('socket_connect_timeout') or A.get('socket_timeout')
        self._socket_keepalive = A.get('socket_keepalive')
        self._socket_keepalive_options = A.get('socket_keepalive_options') or {}
        self._socket_read_size = A.get('socket_read_size')
        self._socket_type = A.get('socket_type')
        self._retry_on_timeout = A.get('retry_on_timeout')
        self._description_args = {'host': self.host, 'port': self.port, 'db': self.db}

        ############################################
        self._max_buffer_size = A.get('max_buffer_size', 256 * 1024 * 1024)
        self._shard_hint = A.get('shard_hint')

        self.watching = False
        self.pubsubed = None

        self.channels = {}
        self.patterns = {}
        self._subqueue = deque()
        self._pub_cond = Condition()
        self._ignore_subscribe_messages = A.get('ignore_subscribe_messages')
        self.pubsubed = None
        # None: COMMAND MODE
        # Redis PUBSUB MODE:
        # -1: UNSUBSCRIBE/PUNSUBSCRIBE mode;
        #  0: first SUBSCRIBE/PSUBSCRIBE;
        # >0: following SUBSCRIBE/PSUBSCRIBE;

        hiredis_kwargs = {'encoding': self._encoding} if self._decode_responses else {}
        self._reader = hiredis.Reader(protocolError=RedisProtoError, replyError=RedisReplyError, **hiredis_kwargs)
        self._reader.setmaxbuf(self._max_buffer_size)

        self.future_initialize()
        self.add_handshake_callback(self.redis_handshake)
        self.add_initial_callback(self.redis_connect)

    def clone(self):
        return Pydis(**self.connection_kwargs.copy())

    @coroutine
    def redis_handshake(self, conn=None):
        if self.password is not None:
            authentication_status = yield self._command_call('AUTH', self.password)
            if authentication_status != 'OK':
                r = self.future_disconnect()
                raise RedisError("Incorrect AUTH password. stopped.")

        if self.db != 0:
            db_status = yield self._command_call('SELECT', self.db)
            if db_status != 'OK':
                r = self.future_disconnect()
                raise RedisError("Error on SELECT db: %s", self.db)

        raise Return(self)

    @coroutine
    def redis_connect(self, conn=None):
        "Re-subscribe to any channels and patterns previously subscribed to"
        # NOTE: for python3, we can't pass bytestrings as keyword arguments
        # so we need to decode channel/pattern names back to unicode strings
        # before passing them to [p]subscribe.
        assert self.future_connected()
        if self.channels:
            channels = {}
            for k, v in iteritems(self.channels):
                channels[self.redis_decode(k, force=False)] = v
            yield self.subscribe(**channels)
        if self.patterns:
            patterns = {}
            for k, v in iteritems(self.patterns):
                patterns[self.redis_decode(k, force=False)] = v
            yield self.psubscribe(**patterns)

    def future_packet_read(self, data):
        reply = False
        try:
            self._reader.feed(data)
            while True:
                reply = self._reader.gets()
                if reply is False:
                    break

                if isinstance(reply, RedisProtoError):
                    raise reply

                if self.pubsubed is None:
                    # REDIS MODE
                    if len(self._future_callback_queue) > 0:
                        # normal client (1 reply = 1 callback)
                        callback = self._future_callback_queue.popleft()
                        callback(reply)
                        continue

                # PUBSUB MODE
                if not isinstance(reply, list):
                    raise RedisError('Redis PUBSUB reply MUST be python `list` type.')

                kind = REDIS_PUBSUB_REPLIES[reply[0]]
                if kind == MESSAGE:
                    channel = native_str(reply[1])
                    data = reply[2]
                    channel_handler = self.channels.get(channel)
                    if callable(channel_handler):
                        channel_handler('message', channel_or_pattern, data=data, kind=kind, pattern=None)
                    else:
                        LOG.warn('message %r without a channel_handler.', channel)
                    continue
                elif kind == PMESSAGE:
                    pattern = native_str(reply[1])
                    channel = native_str(reply[2])
                    data = reply[3]
                    channel_handler = self.channels.get(channel)
                    if callable(channel_handler):
                        channel_handler('message', channel, data=data, kind=kind, pattern=pattern)
                    else:
                        channel_handler = self.patterns.get(pattern)
                        if callable(channel_handler):
                            channel_handler('message', channel, data=data, kind=kind, pattern=pattern)
                        else:
                            LOG.warn('pmessage %r(pattern:%r) without a channel_handler.', channel, pattern)
                    continue

                past = self.pubsubed
                _, channel_or_pattern, self.pubsubed = reply
                channel_or_pattern = native_str(channel_or_pattern)

                if kind == SUBSCRIBE:
                    if self.pubsubed > past:
                        if channel_or_pattern not in self.channels:
                            self.channels[channel_or_pattern] = {}
                        channel_handler = self.channels.get(channel_or_pattern, None)
                        if callable(channel_handler):
                            channel_handler('connect', channel_or_pattern, kind=kind)
                        else:
                            LOG.warn('SUBSCRIBE %r without a channel_handler.', channel_or_pattern)

                elif kind == PSUBSCRIBE:
                    if self.pubsubed > past:
                        if channel_or_pattern not in self.patterns:
                            self.patterns[channel_or_pattern] = None
                        channel_handler = self.patterns.get(channel_or_pattern, None)
                        if callable(channel_handler):
                            channel_handler('connect', channel_or_pattern, kind=kind)
                        else:
                            LOG.warn('PSUBSCRIBE %r without a channel_handler.', channel_or_pattern)

                elif kind == UNSUBSCRIBE:
                    if self.pubsubed < past:
                        if self.pubsubed == 0:
                            self.pubsubed = None
                            self.add_callback(self.redis_pubsub_reset)
                        channel_handler = self.channels.pop(channel_or_pattern, None)
                        if callable(channel_handler):
                            channel_handler('disconnect', channel_or_pattern, kind=kind)
                        else:
                            LOG.warn('UNSUBSCRIBE %r without a channel_handler.', channel_or_pattern)

                elif kind == PUNSUBSCRIBE:
                    if self.pubsubed < past:
                        if self.pubsubed == 0:
                            self.pubsubed = None
                            self.add_callback(self.redis_pubsub_reset)
                        channel_handler = self.patterns.pop(channel_or_pattern, None)
                        if callable(channel_handler):
                            channel_handler('disconnect', channel_or_pattern, kind=kind)
                        else:
                            LOG.warn('UNSUBSCRIBE %r without a channel_handler.', channel_or_pattern)

                else:
                    pass
                # subscribe | unsubscribe | psubscribe | punsubscribe :  (1 reply = 1 callback)
                if len(self._future_callback_queue) > 0:
                    callback = self._future_callback_queue.popleft()
                    callback(reply)  ## reply may be an instance of sub-class of RedisError

        except RedisProtoError as e:
            LOG.exception("RedisProtoError: Packet: %r \n  %s => disconnect", reply, e)
            self.future_stream.close()
            raise
        except RedisReplyError as e:
            LOG.exception("RedisReplyError: Packet: %r \n  %s => disconnect", reply, e)
            self.future_stream.close()
            raise
        except RedisError as re:
            LOG.exception("RedisError: Packet: %r \n  %s => disconnect", reply, re)
            self.future_stream.close()
            raise

    def redis_encode(self, value):
        "Return a bytestring representation of the value"
        if isinstance(value, bytes):
            return value
        elif isinstance(value, bool):
            # special case bool since it is a subclass of int
            return '1' if value else '0'
        elif isinstance(value, (int, long)):
            # python 2 repr() on longs is '123L', so use str() instead
            value = str(value).encode()
        elif isinstance(value, float):
            value = repr(value).encode()
        elif not isinstance(value, basestring):
            # a value we don't know how to deal with. throw an error
            typename = type(value).__name__
            raise RedisError("Invalid input of type: '%s'. Convert to a byte, string or number first." % typename)
        if isinstance(value, unicode):
            value = value.encode(self._encoding, self._encoding_errors)
        return value

    def redis_decode(self, value, force=False):
        "Return a unicode string from the byte representation"
        if (self._decode_responses or force) and isinstance(value, bytes):
            value = value.decode(self._encoding, self._encoding_errors)
        return value

    def redis_normalize_keys(self, args, kwargs):
        channels = dict.fromkeys([native_str(i) for i in args])
        for (k, v) in kwargs.items():
            k = native_str(k)
            if v is not None and not callable(v):
                raise RedisError('callback(channel:%s) MUST be a callable object.' % k)
            channels[k] = v

        new_channels = {}
        for k, v in channels.items():
            if k in self.channels:
                self.channels[k] = v
                continue

            if k in self.patterns:
                self.patterns[k] = v
                continue

            new_channels[k] = v
        return new_channels.keys()

    def redis_sync_watch_state(self, command_name):
        if command_name in self.UNWATCH_COMMANDS:
            self.watching = False
        elif command_name == 'WATCH':
            self.watching = True

    def redis_pubsub_reset(self):
        if self._subqueue:
            self._subqueue.clear()

        if self._pub_cond:
            self._pub_cond.notify_all()

        if self.channels:
            self.channels.clear()

        if self.patterns:
            self.patterns.clear()

    def _command_pack(self, *args, **kwargs):
        "Pack a series of arguments into the Redis protocol"
        output = []
        # the client might have included 1 or more literal arguments in
        # the command name, e.g., 'CONFIG GET'. The Redis server expects these
        # arguments to be sent separately, so split the first argument
        # manually. All of these arguements get wrapped in the Token class
        # to prevent them from being encoded.
        command = args[0].split()
        if len(command) > 1:
            args = tuple(command) + args[1:]
        command = command[0]

        if self.pubsubed is not None and command not in REDIS_SUB_COMMANDS:
            raise RedisError('ALLOWED COMMANDS( WHEN SUBSCRIBE | PSUBSCRIBE ISSUED ): %s' % (REDIS_SUB_COMMANDS,))

        output.extend((SYM_STAR, str(len(args)).encode(), SYM_CRLF))
        for arg in imap(self.redis_encode, args):
            output.extend((SYM_DOLLAR, str(len(arg)).encode(), SYM_CRLF, arg, SYM_CRLF))

        return output

    execute_command = FutureCommander._command_call

    ############################################################################################
    def script_evalsha(self, sha, numkeys, *keys_and_args):
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.

        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self._command_call('EVALSHA', sha, numkeys, *keys_and_args)

    def script_exists(self, *args):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.
        """
        return self._command_call('SCRIPT EXISTS', *args)

    def script_flush(self):
        "Flush all scripts from the script cache"
        return self._command_call('SCRIPT FLUSH')

    def script_kill(self):
        "Kill the currently executing Lua script"
        return self._command_call('SCRIPT KILL')

    def script_load(self, script):
        "Load a Lua ``script`` into the script cache. Returns the SHA."
        return self._command_call('SCRIPT LOAD', script)

    def script_register(self, script):
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)

    ############################################################################################
    def pipeline(self, transport=False, transaction=True, shard_hint=None):
        return Pypeline(self.clone() if transport else self, transaction=transaction, shard_hint=shard_hint)
    ############################################################################################
    @property
    def subscribed(self):
        "Indicates if there are subscriptions to any channels or patterns"
        return bool(len(self.channels) > 0 or len(self.patterns) > 0)

    def psubscribe(self, *args, **kwargs):
        """
        Subscribe to channel patterns. Patterns supplied as keyword arguments
        expect a pattern name as the key and a callable as the value. A
        pattern's callable will be invoked automatically when a message is
        received on that pattern rather than producing a message via
        ``listen()``.
        """
        call_future = self.Future()
        channels = self.redis_normalize_keys(args, kwargs)
        channels_len = len(channels)
        if channels_len == 0:
            return

        packet_partial = partial(self._command_pack, 'PSUBSCRIBE', *channels)
        bulk_callback = channels_len * [partial(self._future_packet_aggregator, channels_len, call_future.set_result)]
        send_partial = partial(self._future_socket_send, packet_partial, bulk_callback)

        # ensure `self._command_pack(*args, **kwargs)` after CONNECTED.
        if self.future_connected():
            if self.pubsubed is None:
                self.pubsubed = 0
            send_partial()
        else:
            future = self.future_connect()
            future.add_done_callback(lambda cf: setattr(self, 'pubsubed', 0) if self.pubsubed is None else None)
            future.add_done_callback(send_partial)

        return call_future

    def subscribe(self, *args, **kwargs):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        call_future = self.Future()
        channels = self.redis_normalize_keys(args, kwargs)
        channels_len = len(channels)
        if channels_len == 0:
            return

        packet_partial = partial(self._command_pack, 'SUBSCRIBE', *channels)

        bulk_callback = channels_len * [partial(self._future_packet_aggregator, channels_len, call_future.set_result)]
        send_partial = partial(self._future_socket_send, packet_partial, bulk_callback)

        # ensure `self._command_pack(*args, **kwargs)` after CONNECTED.
        if self.future_connected():
            if self.pubsubed is None:
                self.pubsubed = 0
            send_partial()
        else:
            future = self.future_connect()
            future.add_done_callback(lambda cf: setattr(self, 'pubsubed', 0) if self.pubsubed is None else None)
            future.add_done_callback(send_partial)
        return call_future

    def punsubscribe(self, *args):
        """
        Unsubscribe from the supplied patterns. If empy, unsubscribe from
        all ``patterns``( channels by `subscribe` exclusive. and that all can use `unsubscribe`). 
        """
        call_future = self.Future()
        channels = self.redis_normalize_keys(args, {})
        channels_len = len(channels)

        packet_partial = partial(self._command_pack, 'PUNSUBSCRIBE', *channels)
        if channels_len > 0:
            callbacks = channels_len * [partial(self._future_packet_aggregator, channels_len, call_future.set_result)]
        else:
            callbacks = call_future.set_result
        send_partial = partial(self._future_socket_send, packet_partial, callbacks)

        # ensure `self._command_pack(*args, **kwargs)` after CONNECTED.
        if self.future_connected():
            send_partial()
        else:
            future = self.future_connect()
            future.add_done_callback(send_partial)

        return call_future

    def unsubscribe(self, *args):
        # `dict.pop` the 'callback=callable()' first, so it's not conflict with self.redis_normalize_keys(args, kwargs)
        call_future = self.Future()
        channels = self.redis_normalize_keys(args, {})
        channels_len = len(channels)

        packet_partial = partial(self._command_pack, 'UNSUBSCRIBE', *channels)
        if channels_len > 0:
            callbacks = channels_len * [partial(self._future_packet_aggregator, channels_len, call_future.set_result)]
        else:
            callbacks = call_future.set_result
        send_partial = partial(self._future_socket_send, packet_partial, callbacks)

        # ensure `self._command_pack(*args, **kwargs)` after CONNECTED.
        if self.future_connected():
            send_partial()
        else:
            future = self.future_connect()
            future.add_done_callback(send_partial)
        return call_future

    def on(self, event, handler=None, namespace=None):
        """Register an event handler.

        :param event: The event name. It can be any string. The event names
                      ``'connect'``, ``'message'`` and ``'disconnect'`` are
                      reserved and should not be used.
        :param handler: The function that should be invoked to handle the
                        event. When this parameter is not given, the method
                        acts as a decorator for the handler function.
        :param namespace: The Socket.IO namespace for the event. If this
                          argument is omitted the handler is associated with
                          the default namespace.

        Example usage::

            # as a decorator:
            @socket_io.on('connect', namespace='/chat')
            def connect_handler(sid, environ):
                print('Connection request')
                if environ['REMOTE_ADDR'] in blacklisted:
                    return False  # reject

            # as a method:
            def message_handler(sid, msg):
                print('Received message: ', msg)
                eio.send(sid, 'response')
            socket_io.on('message', namespace='/chat', message_handler)

        The handler function receives the ``sid`` (session ID) for the
        client as first argument. The ``'connect'`` event handler receives the
        WSGI environment as a second argument, and can return ``False`` to
        reject the connection. The ``'message'`` handler and handlers for
        custom event names receive the message payload as a second argument.
        Any values returned from a message handler will be passed to the
        client's acknowledgement callback function if it exists. The
        ``'disconnect'`` handler does not take a second argument.
        """
        namespace = namespace or '/'

        def set_handler(handler):
            if namespace not in self.handlers:
                self.handlers[namespace] = {}
            self.handlers[namespace][event] = handler
            return handler

        if handler is None:
            return set_handler
        set_handler(handler)
