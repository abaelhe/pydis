#!/usr/bin/env python
# coding:utf-8

"""
@Author: Abael He<abaelhe@icloud.com>
@file: adapter.py
@time: 21/11/2018 13:29
@license: All Rights Reserved, Abael.com
"""

import os, sys, six, socket, logging, contextlib, struct, collections, inspect, stat
from functools import partial, reduce
from tornado import ioloop, gen, concurrent
from tornado.locks import CancelledError
from tornado.netutil import Resolver, OverrideResolver, DefaultExecutorResolver
from tornado.ioloop import IOLoop, PeriodicCallback, thread
from tornado.tcpclient import TCPClient
from tornado.gen import Return, coroutine, is_coroutine_function


LOG = logging.getLogger(__name__)



if six.PY2:
    # py2
    if sys.getdefaultencoding() != 'utf8':
        six.moves.reload_module(sys)
        sys.setdefaultencoding('utf8')

    from urlparse import urlparse
    from _multiprocessing import sendfd, recvfd, address_of_buffer
elif six.PY3:
    # py3
    from urllib.parse import urlparse

    import asyncio
    from tornado.platform.asyncio import AnyThreadEventLoopPolicy

    asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())

# as Tornado recommended: use `pycares` ONLY if the default `DefaultExecutorResolver` is not available
# netutil.Resolver.configure('tornado.platform.caresresolver.CaresResolver')
if ioloop.asyncio:
    from tornado.platform.asyncio import asyncio, AnyThreadEventLoopPolicy
    asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())
    ioloop.IOLoop.configure('tornado.platform.asyncio.AsyncIOMainLoop')


ACKNOWLEDGE = sys.platform == 'darwin'

TCP_SOCKET_STATES = (socket.AF_INET, socket.AF_INET6, socket.AF_UNSPEC)


BYTES_EMPTY = ''



## 参考 tornado.ioloop._run_callback(self, callback):## (tornado.ioloop.py:766),
#  对于 yield 的非 Future 类的处理, 当然也可以用 gen.convert_yielded.register 注册定制类的转换(  );
if gen.singledispatch is not None:
    null_future = concurrent.Future()
    null_future.set_result(None)


    @gen.convert_yielded.register(gen.Runner)
    def convert_gen_runner(runner):
        ret = runner.run()
        if runner.result_future is None:
            return null_future
        return runner.result_future


def __generator_validate__():
    yield 'A'


GENERATOR_TYPE = type(__generator_validate__())


class Future(concurrent.Future):
    ''' loop less Future '''

    def _set_done(self):
        # directly call callbacks,  loop less,
        self._done = True
        if self._callbacks:
            for cb in self._callbacks:
                cb(self)
            self._callbacks = None

    def set_result(self, result, update=False):
        """Sets the result of a ``Future``, trigger all ``done callbacks`` if ``update`` is ``False``.
        and the default action is ``initial`` with ``True``, after initial set_result(val), sometimes, 
        we need to update the Future's ``self._result`` value without trigger the ``done callbacks`` again.
            Note: since update the inner ``self._result`` may cause different view.
                so the best way to do ``set_result(new_val, update=True)`` is in the first added ``done callback``.

        example: 
            >>> future =Future()
            >>> def initial_done_callback(future_resolved):
                    initial_result = future_resolved.result()
                    ... DO SOME WORK, AND GENERATE ``new_result`` ...
                    future_resolved.set_result(new_result, update=True)
                    return future_resolved
            >>> future.add_done_callback(initial_done_callback)
            >>> #### HERE `initial_done_callback` is first added. so it's safe to update inner ``self._result``
                #### and all post added ``done_callback`` will only see the inner ``self._result`` with ``new_result``

        It is undefined to call any of the ``set`` methods more than once on the same object.
        """
        self._result = result
        if update is False:
            self._set_done()

    def callbacks(self):
        return self._callbacks

    def add_done_callback(self, fn):
        if self._done:
            fn(self)
        else:
            self._callbacks.append(fn)

    def cancel(self):
        if self._done:
            raise ValueError('Future is not cancelable since it already done.')
        exc = RuntimeError('Future was canceled by user.')
        self.set_result(exc)
        self.set_exc_info((ValueError, ValueError('Future is not cancelable since it already done.'), None))


class ContextManagerFuture(Future):
    """A Future that can be used with the "with" statement.

    When a coroutine yields this Future, the return value is a context manager
    that can be used like:

        >>> with (yield future) as result:
                pass

    At the end of the block, the Future's exit callback is run.

    This class is stolen from "toro" source:
    https://github.com/ajdavis/toro/blob/master/toro/__init__.py

    Original credits to jesse@mongodb.com
    Modified to be able to return the future result

    Attributes:
        _exit_callback (callable): the exit callback to call at the end of
            the block
        _wrapped (Future): the wrapped future
    """

    def __init__(self, wrapped, exit_callback):
        """Constructor.

        Args:
            wrapped (Future): the original Future object (to wrap)
            exit_callback: the exit callback to call at the end of
                the block
        """
        Future.__init__(self)
        wrapped.add_done_callback(self._done_callback)
        self._exit_callback = exit_callback
        self._wrapped = wrapped

    def _done_callback(self, wrapped):
        """Internal "done callback" to set the result of the object.

        The result of the object if forced by the wrapped future. So this
        internal callback must be called when the wrapped future is ready.

        Args:
            wrapped (Future): the wrapped Future object
        """
        if wrapped.exception():
            self.set_exception(wrapped.exception())
        else:
            self.set_result(wrapped.result())

    def result(self):
        """The result method which returns a context manager

        Returns:
            ContextManager: The corresponding context manager
        """
        if self.exception():
            raise self.exception()

        # Otherwise return a context manager that cleans up after the block.

        @contextlib.contextmanager
        def f():
            try:
                yield self._wrapped.result()
            finally:
                self._exit_callback()

        return f()


class Event(object):
    """An event blocks coroutines until its internal flag is set to True.

    Similar to `threading.Event`.

    A coroutine can wait for an event to be set. Once it is set, calls to
    ``yield event.wait()`` will not block unless the event has been cleared:

    .. testcode::

        from tornado import gen
        from tornado.ioloop import IOLoop
        from tornado.locks import Event

        event = Event()

        async def waiter():
            print("Waiting for event")
            await event.wait()
            print("Not waiting this time")
            await event.wait()
            print("Done")

        async def setter():
            print("About to set the event")
            event.set()

        async def runner():
            await gen.multi([waiter(), setter()])

        IOLoop.current().run_sync(runner)

    .. testoutput::

        Waiting for event
        About to set the event
        Not waiting this time
        Done
    """

    def __init__(self):
        self.value = None
        self._waiters = set()

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.value)

    def notify_all(self, value=None):
        for fut in self._waiters:
            if not fut.done():
                fut.set_result(value if value is not None else self.value)

    def notify_one(self, value=None):
        for fut in self._waiters:
            if not fut.done():
                fut.set_result(value if value is not None else self.value)
                return

    def is_set(self):
        """Return ``True`` if the internal flag is true."""
        return self.value

    def set(self, value):
        """Set the internal flag to ``True``. All waiters are awakened.
        Calling `.wait` once the flag is set will not block.
        """
        self.value = value
        self.notify_all(value)

    def clear(self):
        """Reset the internal flag to ``False``.

        Calls to `.wait` will block until `.set` is called.
        """
        self.value = None

    def wait(self, timeout=None):
        """Block until the internal flag is true.

        Returns a Future, which raises `tornado.util.TimeoutError` after a timeout.
        """
        fut = gen.Future()
        if self._value is not None:
            fut.set_result(self.value)
            return fut

        self._waiters.add(fut)
        fut.add_done_callback(lambda fut: self._waiters.remove(fut))

        if timeout is None:
            return fut
        timeout_fut = gen.with_timeout(timeout, fut, quiet_exceptions=(CancelledError,))
        timeout_fut.add_done_callback(lambda tf: fut.cancel() if not fut.done() else None)
        return timeout_fut


class CachedOverrideResolver(OverrideResolver):
    def initialize(self, resolver=None, mapping=None):
        self.resolver = resolver or DefaultExecutorResolver()
        self.mapping = mapping or {}
        self.io_loop = IOLoop.current()

    def close(self):
        self.resolver.close()
        self.io_loop = None

    def resolve_addr(self, host, port, family=socket.AF_UNSPEC):
        # the addresses we return should still be usable with SOCK_DGRAM.
        addrinfo = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
        return [(family, address) for family, socktype, proto, canonname, address in addrinfo]

    @coroutine
    def resolve(self, host, port, family=socket.AF_UNSPEC, *args, **kwargs):
        # TCPClient().connect('dev.abael.com', '/www/workspace/abaelcom/log/ctrl.unixsock', socket.AF_UNSPEC)
        if (host, port, family) in self.mapping:
            host, port = self.mapping[(host, port, family)]
        elif (host, port) in self.mapping:
            host, port = self.mapping[(host, port)]
        elif host in self.mapping:
            host = self.mapping[host]

        if not isinstance(port, int):
            assert os.path.exists(
                port), 'Here ONLY support UNIX socket file exists !'  # eg.: port = '/www/ctrl.unixsock'
            raise Return([(socket.AF_UNIX, port)])

        result = self.resolve_addr(host, port, family)
        #        result = yield self.io_loop.run_in_executor(None, self.resolve_addr, host, port, family)
        raise Return(result)


# This configure the default `Resolver` to `tornado.tcpclient.TCPClient`
Resolver.configure(CachedOverrideResolver)


def sock_options(sock, blocking=False, keepalive=True, conn_timeout=None, rcvtimeo=None, sndtimeo=None):
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1 if keepalive else 0)
    sock.setblocking(blocking)

    def setopt(s, op, val):
        try:
            s.setsockopt(socket.SOL_SOCKET, op, struct.pack('i', val))
        except:
            return 0
        real_opt = s.getsockopt(socket.SOL_SOCKET, op)
        if real_opt == val:
            return val

    unit = 1024 * 1024 # 1M
    tries = list(i*unit for i in range(16, 0, -1))
    s = reduce(lambda x, y: x or setopt(sock, socket.SO_SNDBUF, y), tries, 0)
    r = reduce(lambda x, y: x or setopt(sock, socket.SO_RCVBUF, y), tries, 0)

    if conn_timeout is not None:
        sock.settimeout(conn_timeout)
    if rcvtimeo is not None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVTIMEO, struct.pack('i', rcvtimeo))
    if sndtimeo is not None:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, struct.pack('i', sndtimeo))

    return r


def stream_options(stream, blocking=False, keepalive=True, conn_timeout=None, rcvtimeo=None, sndtimeo=None):
    future_chunk_size = sock_options(stream.socket, blocking=blocking, keepalive=keepalive, conn_timeout=conn_timeout,
                                     rcvtimeo=rcvtimeo, sndtimeo=sndtimeo)
    stream.set_nodelay(True)
    stream.future_chunk_size = future_chunk_size
    return future_chunk_size


def initial_future_from_kwargs_callback(kwargs=None, future=None):
    future =Future() if future is None else future
    future_callback = kwargs.pop('callback', None) if isinstance(kwargs, dict) else None
    if future_callback is not None and callable(future_callback):
        future.add_done_callback(future_callback)
    return future


class FutureConnection(object):
    def _future_start(self):
        self._future_connecting = False
        self._future_closed = False
        self._future_periodic_connect.start()
        self._future_io_loop = self._future_periodic_connect.io_loop
        self._future_thread_id = thread.get_ident()

    @coroutine
    def _future_stream_close(self, shutdown=False):
        if shutdown: # positive close issued by user.
            self._future_closed = True

        self._future_connecting = False

        LOG.warn('Closing: threadId:%s!', thread.get_ident())
        if self.future_connected():
            for callback in self._future_close_callbacks:
                if inspect.isgeneratorfunction(callback):
                    raise SyntaxError('wrap generator function with ``@tornado.gen.coroutine`` first :%s ' % callback)
                LOG.debug('calling close_callback: %s', callback)
                self.future_stream.io_loop.add_callback(callback, self)

            def _future_stream_close_callback(conn):
                conn.future_stream.close()
                conn.future_stream = None
                conn._future_packet_bulk = []
                conn._future_callback_queue.clear()  # registry

                if conn.future_stream is None:
                    if conn._future_closed is False:
                        conn._future_periodic_connect.start()
                    return

            self.future_stream.io_loop.add_callback(_future_stream_close_callback, self)

    @coroutine
    def _future_stream_connect(self, host=None, port=None, af=socket.AF_UNSPEC, ssl_options=None,
                                   max_buffer_size=None, source_ip=None, source_port=None, timeout=None):
        assert self._future_thread_id == thread.get_ident(), 'this should run in IO thread:%s' % self._future_thread_id
        if self.future_connected():
            if self._future_periodic_connect.is_running():
                self._future_periodic_connect.stop()
            raise Return(self)

        if self._future_connecting is True:
            return
        else:
            self._future_connecting = True

        host = host or self.host
        port = port or self.port
        LOG.warn('Connecting %s:%s, threadId:%s, id:%s' % (host, port, thread.get_ident(), id(self)))
        try:
            self.future_stream = yield self._future_connector.connect(host, port, timeout=timeout,
                                                                     max_buffer_size=max_buffer_size, af=af,
                                                                     ssl_options=ssl_options,
                                                                     source_ip=source_ip, source_port=source_port)
            # Note: we first register Async Close|Read|Write callback here, since handshake may require Async READ|WRITE
        except Exception as e:
            self._future_connecting = False
            LOG.exception('Error connect to %s:%s, retry.', host, port)
            if self.future_stream:
                self.future_stream.close()
            if not self._future_periodic_connect.is_running():
                self._future_periodic_connect.start()
            return

        self._future_callback_queue.clear()  # registry
        self._future_packet_bulk = []
        self._future_connecting = False
        self._future_chunk_size = stream_options(self.future_stream, blocking=False, keepalive=True)
        self._future_reader_buffer = bytearray(self._future_chunk_size)
        self.future_stream.set_close_callback(self._future_stream_close)
        self.future_stream.read_into(self._future_reader_buffer, callback=self._future_socket_read, partial=True)
        LOG.warn('Handshaking: %s:%s, threadId:%s, id:%s', host, port, thread.get_ident(), id(self))
        try:
            for callback in self._future_handshake_callbacks:
                if not is_coroutine_function(callback):
                    raise SyntaxError('wrap generator function with ``@tornado.gen.coroutine`` first :%s ' % callback)
                LOG.debug('calling handshake_callback: %s', callback)
                r = yield callback(self)

            waiters = tuple(self._future_waiters)
            for fut in waiters:
                if not fut.done():
                    fut.set_result(self)
        except Exception as e:
            LOG.exception('Error Handshaking to %s:%s, retry.', host, port)
            self.future_stream.close()
            if not self._future_periodic_connect.is_running():
                self._future_periodic_connect.start()
            return
        finally:
            self._future_connecting = False

        LOG.warn('Connected: %s:%s, threadId:%s, id:%s', host, port, thread.get_ident(), id(self))
        try:
            for callback in self._future_initial_callbacks:
                if inspect.isgeneratorfunction(callback):
                    raise SyntaxError('wrap generator function with ``@tornado.gen.coroutine`` first :%s ' % callback)
                LOG.debug('calling initial_callback: %s', callback)
                r = yield callback(self)
        except Exception as e:
            LOG.exception('Error connect to %s:%s, retry.', host, port)
            self.future_stream.close()
            if not self._future_periodic_connect.is_running():
                self._future_periodic_connect.start()
            return

        for callback in self._future_close_callbacks:
            if inspect.isgeneratorfunction(callback):
                raise SyntaxError('wrap generator function with ``@tornado.gen.coroutine`` first :%s ' % callback)

        LOG.warn('Initialized: %s:%s, threadId:%s, id:%s!', host, port, thread.get_ident(), id(self))


    ###########################################################################
    def future_initialize(self):
        self.host = self.host if hasattr(self, 'host') else '127.0.0.1'
        self.port = self.port if hasattr(self, 'port') else '/tmp/redis.sock'

        self.future_stream = None
        self._future_thread_id = None
        self._future_io_loop = None
        self._future_connecting = None
        self._future_closed = None

        self._future_reader_buffer = None
        self._future_chunk_size = None

        self._future_periodic_connect = PeriodicCallback(self._future_stream_connect, 1000)
        self._future_connector = TCPClient(resolver=CachedOverrideResolver())

        self._future_handshake_callbacks = []  # connected, handshaking
        self._future_initial_callbacks = []  # connected, shandshake passed, initializing
        self._future_waiters = []  # connected, shandshake passed, initialization passed, wake up all waiters.
        self._future_close_callbacks = []

        self._future_packet_bulk = []
        self._future_callback_queue = collections.deque()  # registry

    def future_connected(self):
        return self._future_connecting is False and self.future_stream is not None and not self.future_stream.closed()

    def future_disconnect(self):
        if not self.future_connected():
            return

        self._future_connecting = False

        def safe_shutdown_stream_read(stream):
            if stream.socket is None:
                return

            while len(self._future_close_callbacks) > 0:
                callback = self._future_close_callbacks.pop(0)
                stream.io_loop.add_callback(callback, self)

            if stream.reading() is False:
                stream.io_loop.update_handler(stream.socket.fileno(), IOLoop.WRITE | IOLoop.ERROR)

                if stream.writing() is False:
                    stream.io_loop.remove_handler(stream.socket.fileno())
                    stream.close()
                    return

            stream.io_loop.add_callback(safe_shutdown_stream_read, stream)

        self.future_stream.io_loop.add_callback(safe_shutdown_stream_read, self.future_stream)

    def future_connect(self):
        connected_future = Future()
        if self.future_connected():
            if self._future_periodic_connect.is_running():
                self._future_periodic_connect.stop()

            connected_future.set_result(self)
            return connected_future

        # most time, Transport should be in connected state.
        connected_future.add_done_callback(lambda cf: self._future_waiters.remove(cf))

        for callback in self._future_initial_callbacks:
            connected_future.add_done_callback(callback)

        if connected_future not in self._future_waiters:
            self._future_waiters.append(connected_future)

        if not self._future_periodic_connect.is_running():
            if self._future_io_loop is None:
                self._future_start()
            else:
                self._future_io_loop.add_callback(self._future_periodic_connect.start)

        return connected_future

    ###########################################################################
    def add_future(self, future, future_callback):
        self.future_stream.io_loop.add_future(future, future_callback)

    def add_callback(self, callback, *args, **kwargs):
        self.future_stream.io_loop.add_callback(callback, *args, **kwargs)

    def spawn_callback(self, callback, *args, **kwargs):
        self.future_stream.io_loop.spawn_callback(callback, *args, **kwargs)

    def add_handshake_callback(self, callback):
        self._future_handshake_callbacks.append(callback)

    def add_initial_callback(self, callback):
        self._future_initial_callbacks.append(callback)

    def add_close_callback(self, callback):
        self._future_close_callbacks.append(callback)

    def clear_connect_callbacks(self):
        self._future_close_callbacks = []

    def set_future_chunk_size(self, size=None):
        if size is None:
            size = self._future_chunk_size
        if size < 1:
            return
        cur_size = len(self._future_reader_buffer)
        if size > cur_size:
            self._future_reader_buffer.extend(bytearray(size - cur_size))

        elif size < cur_size:
            del self._future_reader_buffer[size:]

    ###########################################################################
    def _future_socket_read(self, num_bytes=0):
        if num_bytes < 1:
            return

        data = memoryview(self._future_reader_buffer)[:num_bytes] if num_bytes > 0 else None
        self.future_packet_read(data)
        # this 'read_into() DO MUST call after self.future_packet_read(data)! '
        self.future_stream.read_into(self._future_reader_buffer, callback=self._future_socket_read, partial=True)

    def _future_packet_aggregator(self, packet_num, aggregator_callback, packet):
        self._future_packet_bulk.append(packet)
        if len(self._future_packet_bulk) == packet_num:
            future_packet_bulk = self._future_packet_bulk
            self._future_packet_bulk = []
            aggregator_callback(future_packet_bulk)

    def _future_socket_send(self, buffer_or_partial, packet_callbacks, send_future=None, connected_future=None):
        if packet_callbacks is None:
            packet_callbacks = []
        elif callable(packet_callbacks):
            packet_callbacks = [packet_callbacks]

        if callable(buffer_or_partial):
            buffer_or_partial = buffer_or_partial()

        if isinstance(buffer_or_partial, (tuple, list)):
            buffer_or_partial = BYTES_EMPTY.join(buffer_or_partial)

        if send_future is None:
            send_future = self.future_stream.write(buffer_or_partial)
        else:
            self.future_stream.write(buffer_or_partial).add_done_callback(lambda _: send_future.set_result(_.result()))

        self._future_callback_queue.extend(packet_callbacks)

        return send_future

    def future_packet_send(self, packets, callbacks=None):
        packet_send_future = Future()
        send_partial = partial(self._future_socket_send, packets, callbacks, packet_send_future)
        if self.future_connected():
            return send_partial()
        else:
            self.future_connect().add_done_callback(send_partial)
        return packet_send_future

    def future_packet_read(self, data=None):
        raise NotImplementedError('Implement this method first !')


REDIS_SUB_COMMANDS = set(['SUBSCRIBE', 'UNSUBSCRIBE', 'PSUBSCRIBE', 'PUNSUBSCRIBE'])

class FutureCommander:
    ###########################################################################
    # single command with single reply
    def _command_call(self, *args, **kwargs):
        command = args[0]
        assert command not in REDIS_SUB_COMMANDS, 'PUBSUB using `Pydis` subscribe|psubscribe|unsubscribe|punsubscribe !'

        call_future = initial_future_from_kwargs_callback(kwargs)
        # ensure `self._command_pack(*args, **kwargs)` after CONNECTED. 
        packet_partial = partial(self._command_pack, *args, **kwargs)
        send_partial = partial(self._future_socket_send, packet_partial, call_future.set_result)
        if self.future_connected():
            send_partial()
        else:
            self.future_connect().add_done_callback(send_partial)
        return call_future

    # single command with multi replies.
    def _command_call_with_multiple_replies(self, packet_num, *args, **kwargs):
        call_future = initial_future_from_kwargs_callback(kwargs)
        assert args[0] not in REDIS_SUB_COMMANDS, 'PUBSUB using `Pydis` subscribe|psubscribe|unsubscribe|punsubscribe !'

        bulk_callback = packet_num * [partial(self._future_packet_aggregator, packet_num, call_future.set_result)]
        send_partial = partial(self._future_socket_send, partial(self._command_pack, *args, **kwargs), bulk_callback)

        if self.future_connected():
            send_partial()
        else:
            self.future_connect().add_done_callback(send_partial)
        return call_future

    # multi( assume N ) commands and multi replies( N )
    def _command_call_stacked(self, stacked_commands, **kwargs):
        call_future = initial_future_from_kwargs_callback(kwargs)

        stack_len = len(stacked_commands)
        delta_callback = partial(self._future_packet_aggregator_, stack_len, call_future.set_result)
        bulk_callback = [delta_callback] * stack_len
        send_partial = partial(self._future_socket_send, partial(self._command_packs, stacked_commands), bulk_callback)

        if self.future_connected():
            send_partial()
        else:
            self.future_connect().add_done_callback(send_partial)
        return call_future

    def _command_packs(self, commands):
        "Pack multiple commands into the Redis protocol"
        output = reduce(lambda x, y: x + y, (self._command_pack(*cmd, **options) for cmd, options in commands))
        return BYTES_EMPTY.join(output)

    def _command_pack(self, *args, **options):
        raise NotImplementedError('Implement this method first !')
        return []

    call = _command_call

###########################################################################################################
## Sample: FutureRedis
###########################################################################################################
