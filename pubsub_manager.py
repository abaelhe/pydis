#!/usr/bin/env python
# coding:utf-8

"""
@Author: Abael He<abaelhe@icloud.com>
@file: parser.py
@time: 21/11/2018 12:37
@license: All Rights Reserved, Abael.com
"""


import itertools, logging, uuid, json, pickle, six
from six import iterkeys, iteritems
from functools import partial
from .tornado_redis import parse_url, Pydis

""" 用来管理 实时业务，连接用户, 相互间消息； self.rooms [ namespace ] [ room ] [ [sid, active] ]
    
    用户层: sid  代表 服务器 | 用户 参与.

    资源层: room 代表资源(实体资源/数字资源/甚至是 虚拟资源( UGC 用户兴趣, ... ),), 只要 多用户参与的 就有价值.  
                例如, 金融领域的: 货币, 外汇, 信用, 财团, 公司,  ... 
                     不动产领域: 汽车, 房地产, 资产, 矿产,  ...
                     媒体领域的: 版权, 视频, 音频, 游戏, 图片, 书籍, 网络小说, ... 

    抽象层: namespace 代表业务, 在 各种 资源 `room` 之上，建立各种业务,
                例如,  组合方式: 各种资源, 例如 一个 namespace 可以组合 房地产，资产，银行， 车，这些 建立多个多用户参与的room;
                     金融领域的: 在货币之上, 建立借|贷|储|信贷 业务...,  在 外汇 建立购售|跨境支付,..., 财团 建立担保|信用评估 ..., 各种业务 
                     不动产领域: 在汽车之上, 建立购车|售后市场|租车 ...., 在 房地产 建立 购|售|租|寓|酒店,  资产 建立资产管理 | 物业, 矿产,  ...
                     媒体领域的: 版权, 视频, 音频, 游戏, 图片, 书籍, 网络小说, ...
    
    自动响应层： self.callbacks[ sid ] [ namespace ] 对每个用户，在每个领域，注册的 callbacks 

"""


default_logger = logging.getLogger('socketio')


class BaseManager(object):
    """
    管理用户连接基类: (数据全存内存里面) Manage client connections;
    
    消息接口:
        emit(event, data, namespace, room=None, skip_sid=None, callback=None, **kwargs)
    
        接口:
            消息 callback 触发的自增 id 生成器,  self.callbacks[sid][namespace] = {0: itertools.count(1)}

            trigger_callback(self, sid, namespace, id, data)
                查找 callback = self.callbacks[sid][namespace][id]  AND  callback(data)
            

    ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    业务 namespace 视图 namespace { room0, room1, ... } 里面包括结构规划合理的多个`room`,
            get_namespaces()
            get_participants(self, namespace, room);    
            connect( sid, namespace ): 用户(sid) 连接 分支公司(namespace), room: `None`(全namespace), `sid`(peer)
                    内有一些特殊的`room`: 
                    `None`: 整个namespace, 每个用户 初始连接 某个 namespace 时都会加入None的room. 
                    `sid`: 用户自己的room,  每个用户 初始连接 某个 namespace 时都会加入自己的room.
            is_connected(sid, namespace): self.rooms{namespace}{None}{sid}=True/False (前提: sid 不在断开连接池里 )
            pre_disconnect(sid, namespace): 预断开连接: self.pending_disconnect.setdafault(namespace, []).append(sid)
            disconnect(self, sid, namespace): 断开用户和 namespace: 
                                                1. 在 self.rooms[namespace]找用户加入过的 rooms, 在每个 room 里面发布 leave_room 消息
                                                2. 在 self.callbacks{sid}{namespace}{id} 删这个 namespace 里面所有的 id 对应的 callbacks.  
            close_room(self, room, namespace)
            
            

    用户 sid 试图 
            enter_room(self, sid, namespace, room)
            leave_room(self, sid, namespace, room)
            get_rooms(self, sid, namespace)
            
    
    对应一个业务(应用), 集团关注 用户(sid),  每个 room 抽象成 一群人 / 一个业务指标 / ... ;
    每个业务可以量化成多个  用户群(room), 每个room 里可有多个用户(sid),
            Biz2Group

    用户视图, 每个 room 代表一个兴趣组,  每个 room 兴趣组属于一个领域 namespace;  每个用户可以选择订阅某个 namespace 领域 (None, sid) 接着进行:
        Peer2peer, Peer2Group, Peer2Biz,

    self.rooms{namespace0, namespace1, ... }
    

    self.the client is in the process of being disconnected
    
    
    This class keeps track of all the clients and the rooms they are in, to
    support the broadcasting of messages. The data used by this class is
    stored in a memory structure, making it appropriate only for single process
    services. More sophisticated storage backends can be implemented by
    subclasses.
    """
    def __init__(self):
        self.logger = None
        self.server = None
        self.rooms = {} #  集团时图 (`namespace` &`room`):  内部结构: self.rooms [namespace] [room][[sid, active]]
        self.callbacks = {} # 用户视图(`sid`) 的 callbacks 注册器, 内部结构: self.callbacks [sid] [namespace] [id]
        self.pending_disconnect = {}

    def set_server(self, server):
        self.server = server

    def initialize(self):
        """Invoked before the first request is received. Subclasses can add
        their initialization code here.
        """
        pass

    def get_namespaces(self):
        """Return an iterable with the active namespace names."""
        return iterkeys(self.rooms)

    def get_participants(self, namespace, room):
        """Return an iterable with the active participants in a room."""
        for sid, active in iteritems(self.rooms[namespace][room].copy()):
            yield sid

    def connect(self, sid, namespace):
        """Register a client connection to a namespace."""
        self.enter_room(sid, namespace, None)
        self.enter_room(sid, namespace, sid)

    def is_connected(self, sid, namespace):
        if namespace in self.pending_disconnect and sid in self.pending_disconnect[namespace]:
            # the client is in the process of being disconnected
            return False
        try:
            return self.rooms[namespace][None][sid]
        except KeyError:
            pass

    def pre_disconnect(self, sid, namespace):
        """Put the client in the to-be-disconnected list.

        This allows the client data structures to be present while the
        disconnect handler is invoked, but still recognize the fact that the
        client is soon going away.
        """
        if namespace not in self.pending_disconnect:
            self.pending_disconnect[namespace] = []
        self.pending_disconnect[namespace].append(sid)

    def disconnect(self, sid, namespace):
        """Register a client disconnect from a namespace."""
        if namespace not in self.rooms:
            return
        rooms = []
        for room_name, room in six.iteritems(self.rooms[namespace].copy()):
            if sid in room:
                rooms.append(room_name)
        for room in rooms:
            self.leave_room(sid, namespace, room)

        if sid in self.callbacks and namespace in self.callbacks[sid]:
            del self.callbacks[sid][namespace]
            if len(self.callbacks[sid]) == 0:
                del self.callbacks[sid]

        if namespace in self.pending_disconnect and sid in self.pending_disconnect[namespace]:
            self.pending_disconnect[namespace].remove(sid)
            if len(self.pending_disconnect[namespace]) == 0:
                del self.pending_disconnect[namespace]

    def enter_room(self, sid, namespace, room):
        """Add a client to a room."""
        if namespace not in self.rooms:
            self.rooms[namespace] = {}
        if room not in self.rooms[namespace]:
            self.rooms[namespace][room] = {}
        self.rooms[namespace][room][sid] = True

    def leave_room(self, sid, namespace, room):
        """Remove a client from a room."""
        try:
            del self.rooms[namespace][room][sid]
            if len(self.rooms[namespace][room]) == 0:
                del self.rooms[namespace][room]
                if len(self.rooms[namespace]) == 0:
                    del self.rooms[namespace]
        except KeyError:
            pass

    def close_room(self, room, namespace):
        """Remove all participants from a room."""
        try:
            for sid in self.get_participants(namespace, room):
                self.leave_room(sid, namespace, room)
        except KeyError:
            pass

    def get_rooms(self, sid, namespace):
        """Return the rooms a client is in."""
        r = []
        try:
            for room_name, room in six.iteritems(self.rooms[namespace]):
                if room_name is not None and sid in room and room[sid]:
                    r.append(room_name)
        except KeyError:
            pass
        return r

    def emit(self, event, data, namespace, room=None, skip_sid=None, callback=None, **kwargs):
        """Emit a message to a single client, a room, or all the clients connected to the namespace.
            向指定的用户 | room | namespace 发送消息， 可指定 消息 return 时自动调用 callback。
        """
        if namespace not in self.rooms or room not in self.rooms[namespace]:
            return
        for sid in self.get_participants(namespace, room):# 向 namespace:room 里每一个参与者emit消息
            if sid != skip_sid:
                if callback is not None:
                    id = self._generate_ack_id(sid, namespace, callback)#生成响应ID
                else:
                    id = None
                self.server._emit_internal(sid, event, data, namespace, id)

    def trigger_callback(self, sid, namespace, id, data):
        """Invoke an application callback.
            消息发送给用户，根据用户 ACK 消息里的 callback 唯一ID，自动找到 callback, 执行调用. 
        """
        callback = None
        try:
            callback = self.callbacks[sid][namespace][id]
        except KeyError:
            # if we get an unknown callback we just ignore it
            self._get_logger().warning('Unknown callback received, ignoring.')
        else:
            del self.callbacks[sid][namespace][id]
        if callback is not None:
            callback(*data)

    def _generate_ack_id(self, sid, namespace, callback):
        """Generate a unique identifier for an ACK packet."""
        namespace = namespace or '/'
        if sid not in self.callbacks:
            self.callbacks[sid] = {}
        if namespace not in self.callbacks[sid]:
            self.callbacks[sid][namespace] = {0: itertools.count(1)}
        id = six.next(self.callbacks[sid][namespace][0])
        self.callbacks[sid][namespace][id] = callback # : 加超时信息! 和调用频率, 可能内存
        return id

    def _get_logger(self):
        """Get the appropriate logger

        Prevents uninitialized servers in write-only mode from failing.
        """

        if self.logger:
            return self.logger
        elif self.server:
            return self.server.logger
        else:
            return default_logger


class BasePubsubManager(BaseManager):
    """Manage a client list attached to a pub/sub backend.

    This is a base class that enables multiple servers to share the list of
    clients, with the servers communicating events through a pub/sub backend.
    The use of a pub/sub backend also allows any client connected to the
    backend to emit events addressed to Socket.IO clients.

    The actual backends must be implemented by subclasses, this class only
    provides a pub/sub generic framework.

    :param channel: The channel name on which the server sends and receives
                    notifications.
    """
    name = 'pubsub'

    def _publish(self, data):
        """Publish a message on the Socket.IO channel.

        This method needs to be implemented by the different subclasses that
        support pub/sub backends.
        """
        raise NotImplementedError('This method must be implemented in a subclass.')  # pragma: no cover

    def _listen(self):
        """ 
        >>>data = { #format: dict | PICKLE | JSON
                'method': 'emit'|'callback'|'close_room',
                'callback': 

                'host_id': 
                'namespace':
                'sid':
                'id':
                'args':
        }
        Return the next message published on the Socket.IO channel,
        blocking until a message is available.

        This method needs to be implemented by the different subclasses that
        support pub/sub backends.
        """
        raise NotImplementedError('This method must be implemented in a subclass.')  # pragma: no cover

    def __init__(self, channel='sys', write_only=False, logger=None):
        super(BasePubsubManager, self).__init__()
        self.channel = channel
        self.write_only = write_only
        self.host_id = uuid.uuid4().hex
        self.logger = logger

    def initialize(self):
        super(BasePubsubManager, self).initialize()
        if not self.write_only:
            self.thread = self.server.start_background_task(self._thread)
        self._get_logger().info(self.name + ' backend initialized.')

    def emit(self, event, data, namespace=None, room=None, skip_sid=None, callback=None, **kwargs):
        """Emit a message to a single client, a room, or all the clients
        connected to the namespace.

        This method takes care or propagating the message to all the servers
        that are connected through the message queue.

        The parameters are the same as in :meth:`.Server.emit`.
        """
        if kwargs.get('ignore_queue'):
            return super(BasePubsubManager, self).emit(event, data, namespace=namespace, room=room, skip_sid=skip_sid,
                                                   callback=callback)

        namespace = namespace or '/'

        if callback is not None:
            if self.server is None:
                raise RuntimeError('Callbacks can only be issued from the context of a server.')

            if room is None:
                raise ValueError('Cannot use callback without a room set.')

            id = self._generate_ack_id(room, namespace, callback)
            callback = (room, namespace, id)
        else:
            callback = None
        self._publish({'method': 'emit', 'event': event, 'data': data, 'callback': callback,
                       'namespace': namespace, 'room': room, 'host_id': self.host_id, 'skip_sid': skip_sid})

    def close_room(self, room, namespace=None):
        self._publish({'method': 'close_room', 'room': room, 'namespace': namespace or '/'})

    def _handle_emit(self, message):
        # Events with callbacks are very tricky to handle across hosts
        # Here in the receiving end we set up a local callback that preserves
        # the callback host and id from the sender
        remote_callback = message.get('callback')
        remote_host_id = message.get('host_id')
        if remote_callback is not None and len(remote_callback) == 3:
            callback = partial(self._return_callback, remote_host_id, *remote_callback)
        else:
            callback = None
        super(BasePubsubManager, self).emit(message['event'], message['data'],
                                        namespace=message.get('namespace'),
                                        room=message.get('room'),
                                        skip_sid=message.get('skip_sid'),
                                        callback=callback)

    def _handle_callback(self, message):
        if self.host_id == message.get('host_id'):
            try:
                sid = message['sid']
                namespace = message['namespace']
                id = message['id']
                args = message['args']
            except KeyError:
                return
            self.trigger_callback(sid, namespace, id, args)

    def _return_callback(self, host_id, sid, namespace, callback_id, *args):
        # When an event callback is received, the callback is returned back
        # the sender, which is identified by the host_id
        self._publish({'method': 'callback', 'host_id': host_id, 'sid': sid, 'namespace': namespace,
                'id': callback_id, 'args': args})

    def _handle_close_room(self, message):
        super(BasePubsubManager, self).close_room(room=message.get('room'), namespace=message.get('namespace'))

    def _thread(self):
        for message in self._listen():
            data = None
            if isinstance(message, dict):
                data = message
            else:
                if isinstance(message, six.binary_type):  # pragma: no cover
                    try:
                        data = pickle.loads(message)
                    except:
                        pass
                if data is None:
                    try:
                        data = json.loads(message)
                    except:
                        pass
            if data and 'method' in data:
                if data['method'] == 'emit':
                    self._handle_emit(data)
                elif data['method'] == 'callback':
                    self._handle_callback(data)
                elif data['method'] == 'close_room':
                    self._handle_close_room(data)


class PubsubManager(BasePubsubManager):  # pragma: no cover
    """Redis based client manager.

    This class implements a Redis backend for event sharing across multiple
    processes. Only kept here as one more example of how to build a custom
    backend, since the kombu backend is perfectly adequate to support a Redis
    message queue.

    To use a Redis backend, initialize the :class:`Server` instance as
    follows::

        url = 'redis://hostname:port/0'
        server = socketio.Server(client_manager=socketio.RedisManager(url))

    :param url: The connection URL for the Redis server. For a default Redis
                store running on the same host, use ``redis://``.
    :param channel: The channel name on which the server sends and receives
                    notifications. Must be the same in all the servers.
    :param write_only: If set ot ``True``, only initialize to emit events. The
                       default of ``False`` initializes the class for emitting
                       and receiving.
    """
    name = 'pydis'

    def __init__(self, url='redis://localhost:6379/0', channel='sys', write_only=False, logger=None):
        kwargs = parse_url(url)
        self.redis_url = url
        self.redis = Pydis()
        self._redis_connect()
        super(PubsubManager, self).__init__(channel=channel, write_only=write_only, logger=logger)

    def initialize(self):
        super(PubsubManager, self).initialize()

    def _publish(self, data):
        return self.redis.publish(self.channel, pickle.dumps(data))

    def _redis_listen_with_retries(self):
        self.pubsub.subscribe(self.channel)
        for message in self.pubsub.listen():
            yield message

    def _listen(self):
        channel = self.channel.encode('utf-8')
        self.pubsub.subscribe(self.channel)
        for message in self._redis_listen_with_retries():
            if message['channel'] == channel and \
                    message['type'] == 'message' and 'data' in message:
                yield message['data']
        self.pubsub.unsubscribe(self.channel)
