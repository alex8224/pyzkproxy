# -*- coding:utf-8 -*-
'''
author: alex8224@gmail.com
zookeeper proxy
1. forward req to real proxy
2. cache some node

'''

from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from functools import partial
from collections import OrderedDict
from Queue import Queue, LifoQueue
import threading
import sys
import json
import time
import struct
import traceback
import logging
from StringIO import StringIO

FORMAT = '%(asctime)-15s - %(threadName)s - %(message)s'
logging.basicConfig(file="zkclient.log", filemode="w", format=FORMAT)
logger = logging.getLogger('zkproxy')
logger.setLevel(logging.DEBUG)


class Node(object):
    def __init__(self, path):
        self.path = path
        self.child = []
        self.data = ""

    def add_child(self, node):
        self.child.append(node)


class ZkCache(object):
    def __init__(self):
        self.map = OrderedDict()
        self.lock = threading.Lock()

    def put(self, path, nodes):
        try:
            self.lock.acquire()
            self.map[path] = nodes
        finally:
            self.lock.release()

    def get(self, path):
        return self.map.get(path) or []

    def ensure_node(self, path):
        if path not in self.map:
            try:
                self.lock.acquire()
                self.map[path] = []
            finally:
                self.lock.release()

    def keys(self):
        return self.map.keys()

    def __str__(self):
        return str(self.map)

    def split(self, path):
        child_index = path.rfind("/")
        if child_index > -1:
            parent, child = path[:child_index], path[child_index + 1:]
            return parent, child
        else:
            return path, None

    def exists(self, ele):
        parent, child = self.split(ele)
        if child:
            child_list = self.map.get(parent) or []
            return child in child_list
        else:
            return ele in self.map


class OpCode(object):
    notification = 0
    create = 1
    delete = 2
    exists = 3
    getData = 4
    setData = 5
    getACL = 6
    setACL = 7
    getChildren = 8
    sync = 9
    ping = 11
    getChildren2 = 12
    auth = 100
    setWatches = 101
    createSession = -10
    closeSession = -11
    error = -1


class Perms(object):
    READ = 1 << 0
    WRITE = 1 << 1
    CREATE = 1 << 2
    DELETE = 1 << 3
    ADMIN = 1 << 4
    ALL = READ | WRITE | CREATE | DELETE | ADMIN


class ErrCode(object):
    Ok = 0
    SystemError = -1
    RuntimeInconsistency = -2
    DataInconsistency = -3
    ConnectionLoss = -4
    MarshallingError = -5
    Unimplemented = -6
    OperationTimeout = -7
    BadArguments = -8
    APIError = -100
    NoNode = -101
    NoAuth = -102
    BadVersion = -103
    NoChildrenForEphemerals = -108
    NodeExists = -110
    NotEmpty = -111
    SessionExpired = -112
    InvalidCallback = -113
    InvalidACL = -114
    AuthFailed = -115
    SessionMoved = -118


class EventType(object):
    NopEvt = -1
    NodeCreated = 1
    NodeDeleted = 2
    NodeDataChanged = 3
    NodeChildrenChanged = 4


class KeeperState(object):
    Unknown = -1
    Disconnected = 0
    NoSyncConnected = 1
    SyncConnected = 3
    AuthFailed = 4
    Expired = -112


class ReplyType(object):
    EVENT = -1
    PING = -2
    AUTH = -4


class Record(object):

    def __init__(self):
        pass

    def read_int(self, out):
        return struct.unpack(">i", out.read(4))[0]

    def read_byte(self, out):
        return struct.unpack("b", out.read(1))[0]

    def write_int(self, v, out):
        out.write(struct.pack(">i", v))

    def write_byte(self, v, out):
        out.write(struct.pack("b", v))

    def write_bool(self, v, out):
        val = 1 if v else 0
        self.write_byte(val, out)

    def read_bool(self, in_stream):
        val = self.read_byte(in_stream)
        return True if val == 1 else False

    def read_long(self, out):
        return struct.unpack(">q", out.read(8))[0]

    def write_long(self, v, out):
        out.write(struct.pack(">q", v))

    def read_str(self, in_stream):
        str_len = struct.unpack(">i", in_stream.read(4))[0]
        string = in_stream.read(str_len)
        return string

    def write_str(self, string, out):
        str_len = len(string)
        self.write_int(str_len, out)
        out.write(string)

    def write_buff(self, buff, out):
        if not buff:
            self.write_int(-1, out)
        else:
            self.write_int(len(buff), out)
            out.write(buff)

    def read_buff(self, out):
        buff_len = self.read_int(out)
        if buff_len == -1:
            return None
        else:
            buff = out.read(buff_len)
            return buff

    def read_str_list(self, out):
        list_len = self.read_int(out)
        vals = []
        for _ in range(list_len):
            vals.append(self.read_str(out))
        return vals

    def write_str_list(self, str_list, out):
        self.write_int(len(str_list), out)
        for string in str_list:
            self.write_str(string, out)

    def write_record(self, record, out):
        if record:
            record.serialize(out)

    def read_record(self, record, in_stream):
        return record.deserialize(in_stream)

    def write_record_list(self, record_list, out):
        self.write_int(len(record_list), out)
        for rec in record_list:
            self.write_record(rec, out)

    def read_record_list(self, rec_type, in_stream):
        record_len = self.read_int(in_stream)
        record_list = []
        for _ in range(record_len):
            record = rec_type()
            record_list.append(record.deserialize(in_stream))
        return record_list

    def serialize(self, out):
        pass

    def deserialize(self):
        pass

    def get_bytes(self, read_len, in_stream):
        return in_stream.read(read_len)

    def __str__(self):
        pass


class Id(Record):
    def Id(self):
        super(Id, self).__init__()

    def build(self, schema, idstr):
        self.schema = schema
        self.id = idstr
        return self

    def serialize(self, out):
        self.write_str(self.schema, out)
        self.write_str(self.id, out)

    def deserialize(self, in_stream):
        self.schema = self.read_str(in_stream)
        self.id = self.read_str(in_stream)
        return self

    def __str__(self):
        return "id:{schema=%s,id=%s}" % (self.schema, self.id)


class Ids(object):
    ANYONE_ID_UNSAFE = Id().build("world", "anyone")


class Acl(Record):
    def __init__(self):
        self.perms = None
        self.id = None
        super(Acl, self).__init__()

    def build(self, perms, idstr):
        self.perms = perms
        self.id = idstr
        return self

    def serialize(self, out):
        self.write_int(self.perms, out)
        self.write_record(self.id, out)

    def deserialize(self, in_stream):
        self.perms = self.read_int(in_stream)
        id_record = Id()
        self.id = self.read_record(id_record, in_stream)
        return self

    def __str__(self):
        return "Acl{%d, %s}" % (self.perms, str(self.id))


class Stat(Record):
    def __init__(self):
        super(Stat, self).__init__()

    def build(self, czxid, mzxid, ctime, mtime, version, cversion, aversion, ephemeralOwner, dataLength, numChildren, pzxid):
        self.czxid            =  czxid
        self.mzxid            =  mzxid
        self.ctime            =  ctime
        self.mtime            =  mtime
        self.version          =  version
        self.cversion         =  cversion
        self.aversion         =  aversion
        self.ephemeralOwner   =  ephemeralOwner
        self.dataLength       =  dataLength
        self.numChildren      =  numChildren
        self.pzxid            =  pzxid
        return self

    def serialize(self, out):
        self.write_long(self.czxid, out)
        self.write_long(self.mzxid, out)
        self.write_long(self.ctime, out)
        self.write_long(self.mtime, out)
        self.write_int(self.version, out)
        self.write_int(self.cversion, out)
        self.write_int(self.aversion, out)
        self.write_long(self.ephemeralOwner, out)
        self.write_int(self.dataLength, out)
        self.write_int(self.numChildren, out)
        self.write_long(self.pzxid, out)

    def deserialize(self, in_stream):
        self.czxid          =    self.read_long(in_stream)
        self.mzxid          =    self.read_long(in_stream)
        self.ctime          =    self.read_long(in_stream)
        self.mtime          =    self.read_long(in_stream)
        self.version        =    self.read_int(in_stream)
        self.cversion       =    self.read_int(in_stream)
        self.aversion       =    self.read_int(in_stream)
        self.ephemeralOwner =    self.read_long(in_stream)
        self.dataLength     =    self.read_int(in_stream)
        self.numChildren    =    self.read_int(in_stream)
        self.pzxid          =    self.read_long(in_stream)
        return self

    def __str__(self):
        return "Stat{czxid=%s, mzxid=%s, ctime=%s, mtime=%s, version=%s, cversion=%s, aversion=%s, ephemeralOwner=%s, dataLength=%s, numChildren=%s, pzxid=%d}" % (self.czxid, \
                self.mzxid,self.ctime, self.mtime, self.version, self.cversion, self.aversion, self.ephemeralOwner, self.dataLength, self.numChildren, self.pzxid)


class RequestHeader(Record):

    def __init__(self, xid=0, typecode=0):
        self._xid = xid
        self._type = typecode
        super(RequestHeader, self).__init__()

    @property
    def xid(self):
        return self._xid

    @xid.setter
    def xid(self, x):
        self._xid = x

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, typecode):
        self._type = typecode

    def serialize(self, out):
        self.write_int(self._xid, out)
        self.write_int(self._type, out)

    def deserialize(self, in_stream):
        self._xid = self.read_int(in_stream)
        self._type = self.read_int(in_stream)
        return self

    def __str__(self):
        return "xid=%d, type=%d" % (self._xid, self._type)


class ReplyHeader(Record):
    def __init__(self):
        super(ReplyHeader, self).__init__()
        self._xid = None
        self._zxid = None
        self._err = None

    def build(self, xid, zxid, err):
        self._xid = xid
        self._zxid = zxid
        self._err = err
        return self

    @property
    def xid(self):
        return self._xid

    @property
    def zxid(self):
        return self._zxid

    @property
    def err(self):
        return self._err

    def serialize(self, out):
        self.write_int(self._xid, out)
        self.write_long(self._zxid, out)
        self.write_int(self._err, out)

    def deserialize(self, in_stream):
        self._xid = self.read_int(in_stream)
        self._zxid = self.read_long(in_stream)
        self._err = self.read_int(in_stream)
        return self

    def __str__(self):
        return "reply{xid=%d, zxid=%d, err=%d}" % (self._xid if self._xid else 0, self._zxid if self._zxid else 0, self._err if self._err else 0)


class CloseSessoinReq(RequestHeader):
    def __init__(self):
        super(CloseSessoinReq, self).__init__(1, OpCode.closeSession)


class Ping(RequestHeader):
    def __init__(self):
        super(Ping, self).__init__(-2, OpCode.ping)

    def __str__(self):
        return "Ping"


class Pong(ReplyHeader):
    def __init__(self):
        super(Pong, self).__init__()
        self._xid = -2
        self._zxid = -2
        self._err = 0

    def __str__(self):
        return "Pong"


class ConnectReq(Record):
    def __init__(self, ver, last_zxid, timeout, session, password):
        self.ver = ver
        self.last_zxid = last_zxid
        self.timeout = timeout
        self.session = session
        self.password = password
        super(ConnectReq, self).__init__()

    def serialize(self, out):
        self.write_int(self.ver, out)
        self.write_long(self.last_zxid, out)
        self.write_int(self.timeout, out)
        self.write_long(self.session, out)
        self.write_buff(self.password, out)

    def deserialize(self, in_stream):
        self.ver = self.read_int(in_stream)
        self.last_zxid = self.read_long(in_stream)
        self.timeout = self.read_int(in_stream)
        self.session = self.read_long(in_stream)
        self.password = self.read_buff(in_stream)
        return self

    def __str__(self):
        return "ConnectReq{ver=%d, last_zxid=%d, timeout=%d, session=%d, password=%s}" % (self.ver, self.last_zxid, self.timeout, self.session, self.password)


class ConnectResp(Record):
    def __init__(self):
        self.ver = None
        self.timeout = None
        self.session = None
        self.password = None
        super(ConnectResp, self).__init__()

    def build(self, ver, timeout, session, password):
        self.ver = ver
        self.timeout = timeout
        self.session = session
        self.password = password
        return self

    def serialize(self, out):
        self.write_int(self.ver, out)
        self.write_int(self.timeout, out)
        self.write_long(self.session, out)
        self.write_buff(self.password, out)

    def deserialize(self, in_stream):
        self.ver = self.read_int(in_stream)
        self.timeout = self.read_int(in_stream)
        self.session = self.read_long(in_stream)
        self.password = self.read_buff(in_stream)
        return self

    def __str__(self):
        return "ConnectResp{ver=%d, timeout=%d, session=0x%x, password=%d}" % (self.ver, self.timeout, self.session, 16)


class CreateReq(Record):
    def __init__(self):
        super(CreateReq, self).__init__()

    def build(self, path, data, acl_list, flag):
        self.path = path
        self.data = data
        self.acl_list = acl_list
        self.flag = flag
        return self

    def serialize(self, out):
        self.write_str(self.path, out)
        self.write_buff(self.data, out)
        self.write_record_list(self.acl_list, out)
        self.write_int(self.flag, out)

    def deserialize(self, in_stream):
        self.path = self.read_str(in_stream)
        self.data = self.read_buff(in_stream)
        self.acl_list = self.read_record_list(Acl, in_stream)
        self.flag = self.read_int(in_stream)
        return self

    def __str__(self):
        acl_buff = map(lambda x: str(x), self.acl_list)
        return "CreateReq{path=%s, data=%s, acl_list=%s, flag=%d}" % (self.path, self.data, acl_buff, self.flag)


class CreateResp(Record):
    def __init__(self):
        self.path = None

    def build(self, path):
        self.path = path
        return self

    def serialize(self, out):
        if self.path:
            self.write_str(self.path, out)

    def deserialize(self, in_stream):
        self.path = self.read_str(in_stream)
        return self

    def __str__(self):
        return "CreateResp{path=%s}" % (self.path, )


class GetChildReq(Record):

    def __init__(self):
        super(GetChildReq, self).__init__()

    def build(self, path, watch):
        self.path = path
        self.watch = watch
        return self

    def serialize(self, out):
        self.write_str(self.path, out)
        self.write_bool(self.watch, out)

    def deserialize(self, in_stream):
        self.path = self.read_str(in_stream)
        self.watch = self.read_bool(in_stream)
        return self

    def __str__(self):
        return "GetChlidReq{path=%s, watch=%s}" % (self.path, self.watch)


class GetChildResp(Record):
    def __init__(self):
        self.children = []
        super(GetChildResp, self).__init__()

    @property
    def childrens(self):
        return self.children

    @childrens.setter
    def childrens(self, childs):
        self.children = childs

    def serialize(self, out):
        self.write_str_list(self.children, out)

    def deserialize(self, in_stream):
        self.children = self.read_str_list(in_stream)
        return self

    def __str__(self):
        if self.children:
            nodelist = "\n".join(self.children)
            return "GetChildResp{children=%s}" % nodelist
        else:
            return "GetChildResp{}"


class ExistsReq(Record):
    def __init__(self):
        self.path = None
        self.watch = None
        super(ExistsReq, self).__init__()

    def build(self, path, watch):
        self.path = path
        self.watch = watch

    def serialize(self, out):
        self.write_str(self.path, out)
        self.write_bool(self.watch, out)

    def deserialize(self, in_stream):
        self.path = self.read_str(in_stream)
        self.watch = self.read_bool(in_stream)
        return self

    def __str__(self):
        return "ExistsReq{path=%s, watch=%s}" % (self.path, self.watch)


class ExistsResp(Record):
    def __init__(self):
        super(ExistsResp, self).__init__()
        self.stat = None

    def set_stat(self, stat):
        self.stat = stat

    def serialize(self, out):
        self.write_record(self.stat, out)

    def deserialize(self, in_stream):
        self.stat = self.read_record(Stat(), in_stream)
        return self

    def __str__(self):
        return "ExistsResp{stat=%s}" % self.stat


class SetDataReq(Record):
    def __init__(self):
        super(SetDataReq, self).__init__()
        self.path = ''
        self.data = ''
        self.ver = -1

    def build(self, path, data, ver):
        self.path = path
        self.data = data
        self.ver = ver
        return self

    def serialize(self, out):
        self.write_str(self.path, out)
        self.write_buff(self.data, out)
        self.write_int(self.ver, out)

    def deserialize(self, in_stream):
        self.path = self.read_str(in_stream)
        self.data = self.read_buff(in_stream)
        self.ver = self.read_int(in_stream)
        return self

    def __str__(self):
        return "SetDataReq{path=%s, data=anydata, ver=%d}" % (self.path, self.ver)


class SetDataResponse(Record):
    def __init__(self):
        super(SetDataResponse, self).__init__()
        self.stat = None

    def deserialize(self, in_stream):
        self.stat = self.read_record(Stat(), in_stream)
        return self

    def serialize(self, out):
        self.write_record(self.stat, out)

    def __str__(self):
        return "SetDataResp{stat=%s}" % str(self.stat)


class SetWatches(Record):
    def __init__(self):
        super(SetWatches, self).__init__()

    def build(self, relative_zxid, data_watches, exists_watches, child_watches):
        self.relative_zxid = relative_zxid
        self.data_watches = data_watches
        self.exists_watches = exists_watches
        self.child_watches = child_watches
        return self

    def serialize(self, out):
        self.write_long(self.relative_zxid, out)
        self.write_str_list(self.data_watches, out)
        self.write_str_list(self.exists_watches, out)
        self.write_str_list(self.child_watches, out)

    def deserialize(self, in_stream):
        self.relative_zxid = self.read_long(in_stream)
        self.data_watches = self.read_str_list(in_stream)
        self.exists_watches = self.read_str_list(in_stream)
        self.child_watches = self.read_str_list(in_stream)
        return self

    def __str__(self):
        return "SetWatches{relative_zxid=%d, data_watches=%s, exists_watches=%s, child_watchers%s}" % (self.relative_zxid, ",".join(self.data_watches), ",".join(self.exists_watches), ",".join(self.child_watches))


class SetWatchesResp(Record):
        def __init__(self):
            super(SetWatchesResp, self).__init__()

        def serialize(self, out):
            pass

        def deserialize(self, in_stream):
            return self


class NopResp(Record):
    def __init__(self):
        super(NopResp, self).__init__()

    def build(self, name):
        self.name = name
        return self

    def deserialize(self, in_stream):
        return self

    def serialize(self, out):
        pass

    def __str__(self):
        return "NopResp{name=}"


class NopReq(Record):
    def __init__(self):
        super(NopReq, self).__init__()

    def build(self, name):
        self.name = name
        return self

    def serialize(self, out):
        pass

    def deserialize(self, in_stream):
        return self

    def __str__(self):
        return "NopReq{name=}"


class XidCount(object):
    SET_WATCHE = -8

    def __init__(self):
        self.xid = 0 

    @property
    def last_zxid(self):
        return self._last_zxid

    @last_zxid.setter
    def last_zxid(self, last_zxid):
        self._last_zxid = last_zxid

    def next_xid(self):
        self.xid += 1
        return self.xid


Xid = XidCount()


class Packet(object):
    def __init__(self, header, request, reply_header, response, watcher=None, callback=None, buff=None):
        self.header = header
        self.request = request
        self.reply_header = reply_header
        self.response = response
        self.watcher = watcher
        self.callback = callback
        self.buff = buff
        self.proxyed = False
        self.acked = False

    def serialize(self):
        buff = StringIO()
        try:
            self.header.serialize(buff)
            if self.request:
                self.request.serialize(buff)
            req_len = struct.pack(">i", len(buff.getvalue()))
            return req_len + buff.getvalue()
        except:
            logger.error(traceback.format_exc())
        finally:
            buff.close()

    def serialize_forward(self):
        buff = StringIO()
        try:
            self.reply_header.serialize(buff)
            if self.response:
                self.response.serialize(buff)
            if self.watcher:
                self.watcher.serialize(buff)
            len_bytes = struct.pack(">i", len(buff.getvalue()))
            return len_bytes + buff.getvalue()
        except:
            logger.error(traceback.format_exc())
        finally:
            buff.close()

    def dispatch_event(self, in_stream):
        if not self.watcher:
            self.watcher = WatcherEvent()
        self.watcher.deserialize(in_stream)
        logger.debug("got event %s" % self.watcher)

    def deserialize(self, in_stream):
        try:
            # resp_len = struct.unpack(">i", in_stream.read(4))[0]
            # self.reply_header.deserialize(in_stream)
            Xid.last_zxid = self.reply_header.zxid
            if self.reply_header.err == 0:
                if self.reply_header.xid == -1:
                    self.dispatch_event(in_stream)
                elif self.reply_header.xid == -2:
                    pass
                else:
                    self.response.deserialize(in_stream)
                    if self.callback:
                        self.callback(self.response)
                    return self
        except:
            logger.error(traceback.format_exc())

    def __str__(self):
        try:
            return "Packet{header=%s, req=%s, reply=%s, resp=%s, acked=%s}" % (
                    str(self.header) if self.header else "",
                    str(self.request) if self.request else "",
                    str(self.reply_header) if self.reply_header else "",
                    str(self.response) if self.response else "",
                    str(self.acked))
        except:
            logger.error(traceback.format_exc())


class DeleteReq(Record):

    def __init__(self):
        super(DeleteReq, self).__init__()

    def build(self, path, ver):
        self.path = path
        self.ver = ver
        return self

    def serialize(self, out):
        self.write_str(self.path, out)
        self.write_int(self.ver, out)

    def deserialize(self, in_stream):
        self.path = self.read_str(in_stream)
        self.ver = self.read_int(in_stream)
        return self

    def __str__(self):
        return "DeleteReq{path=%s, ver=%d}" % (self.path, self.ver)


class GetDataReq(Record):
    def __init__(self):
        super(GetDataReq, self).__init__()

    def build(self, path, watch):
        self.path = path
        self.watch = watch
        return self

    def serialize(self, out):
        self.write_str(self.path, out)
        self.write_bool(self.watch, out)

    def deserialize(self, in_stream):
        self.path = self.read_str(in_stream)
        self.watch = self.read_bool(in_stream)
        return self

    def __str__(self):
        return "GetDataReq{path=%s, watch=%s}" % (self.path, 'true' if self.watch else 'false')


class GetDataResp(Record):
    def __init__(self):
        super(GetDataResp, self).__init__()
        self.data = None
        self.stat = None

    def build(self, data, stat):
        self.data = data
        self.stat = stat
        return self

    def serialize(self, out):
        self.write_buff(self.data, out)
        self.write_record(self.stat, out)

    def deserialize(self, in_stream):
        self.data = self.read_buff(in_stream)
        self.stat = self.read_record(Stat(), in_stream)
        return self

    def __str__(self):
        return "GetDataResq{data=anydata, stat=%s}" % (str(self.stat) if self.stat else "")


class WatcherEvent(Record):
    def __init__(self):
        super(WatcherEvent, self).__init__()
        self.type = None
        self.state = None
        self.path = None

    def build(self, type_code, state, path):
        self.type = type_code
        self.state = state
        self.path = path
        return self

    def deserialize(self, in_stream):
        self.type = self.read_int(in_stream)
        self.state = self.read_int(in_stream)
        self.path = self.read_str(in_stream)
        return self

    def serialize(self, out):
        self.write_int(self.type, out)
        self.write_int(self.state, out)
        self.write_str(self.path, out)

    def __str__(self):
        if self.type:
            return "WatcherEvent{type=%d, state=%d, path=%s}" % (self.type, self.state, self.path)
        else:
            return "WatcherEvent{}"


def send_packet(sock, packet):
    sock.write(packet.serialize())
    sock.flush()


def send_buff(sock, buff):
    sock.write(buff)
    sock.flush()


zkCache = ZkCache()


def iter_all_child(sock_pair, child_list, name):
    rfile, wfile = sock_pair
    while child_list:
        node = child_list.pop()
        if node:
            node_name = "%s/%s" % (name, node)
            logger.debug("\tget child for %s" % (node_name, ))
            get_child_req = GetChildReq().build(node_name, False)
            child_packet = Packet(RequestHeader(Xid.next_xid(), OpCode.getChildren), get_child_req, ReplyHeader(), GetChildResp())
            send_packet(wfile, child_packet)
            resp_len = struct.unpack(">i", rfile.read(4))[0] 
            child_packet.reply_header.deserialize(rfile)
            child_packet.deserialize(rfile)
            if child_packet.reply_header.err == 0 and child_packet.response.childrens:
                return child_packet.response.childrens
                #logger.debug(child_packet.response.childrens)
                #zkCache.put(node_name, child_packet.response.childrens)
                #iter_all_child(sock_pair, child_packet.response.childrens, node_name)



class NopCallback(object):
    def __call__(self, *args, **kwargs):
        pass
        # print("nop callback", args, kwargs)


NopDo = NopCallback()


class ExitSignal(object): pass



class BlockZkClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None

    def connect(self):
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.wfile = self.sock.makefile("wb")
        self.rfile = self.sock.makefile("rb")
        self.init()

    def init(self):
        '''send connect req and wait resp'''
        password = struct.pack(">QQ", 0, 0)
        conn_req = ConnectReq(0, 0, 60000000, 0, password)
        buff_io = StringIO()
        conn_req.serialize(buff_io)
        body_len = struct.pack(">i", len(buff_io.getvalue()))
        self.wfile.write(body_len)
        self.wfile.write(buff_io.getvalue())
        self.wfile.flush()

        resp_body_len = struct.unpack(">i", self.rfile.read(4))[0]
        conn_resp = ConnectResp()
        conn_resp.deserialize(self.rfile)
        self.timeout = int(conn_resp.timeout / 1000)
        logger.debug("connect to %s:%d Ok. connect response is %s" % (self.host, self.port, conn_resp))

    def close(self):
        if self.rfile:
            self.rfile.close()

        if self.wfile:
            self.wfile.close()


class ZkClient(object):
    ''''''
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.sock = None
        self.last = time.time()
        self.queue = Queue()
        self.pending_queue = Queue()
        self.event_listener = []
        self.wfile = None
        self.rfile = None
        self.running = True
        self.timeout = 5
        self.state = ProxyState.INIT

    def subscribe(self, listener):
        self.event_listener.append(listener)

    def send_packet(self, packet):
        pack, callback = packet
        self.queue.put(packet)

    def send_all(self, packet):
        self.wfile.write(packet.serialize())
        self.wfile.flush()

    def start(self):
        ''' start write thread and read thread '''
        ping_thread = threading.Thread(target=self.start_ping)
        ping_thread.setDaemon(True)
        ping_thread.start()

        send_thread = threading.Thread(target=self.start_write)
        send_thread.setDaemon(True)
        send_thread.start()

        read_thread = threading.Thread(target=self.start_read)
        read_thread.setDaemon(True)
        read_thread.start()
        self.running = True
        self.state = ProxyState.CONNECTED

    def start_write(self):
        while self.running:
            packet, callback = self.queue.get()
            self.last = time.time()
            try:
                if isinstance(packet, ExitSignal):
                    logger.error("got exit signal")
                    break
                self.send_all(packet)
                if packet.header.type != OpCode.ping:
                    self.pending_queue.put((packet, callback))
            except:
                logger.error("start_write %s resend %s futures" % (traceback.format_exc(), packet))
                self.state = ProxyState.BROKEN
                self.queue.put((packet, callback))
                self.finish()
                self.reconnect()
        logger.info("start write exit")

    def dispatch_event(self, reply_len, reply):
        watcher = WatcherEvent().deserialize(self.rfile)
        for listener in self.event_listener:
            listener((reply_len, reply, watcher))

    def start_read(self):
        try:
            while self.running:
                if self.state == ProxyState.DISCONNECTED:
                    break

                len_buff = self.rfile.read(4)
                if not len_buff or len(len_buff) < 4:
                    logger.error("peer close connection %s:%d" % self.sockname)
                    self.state = ProxyState.BROKEN
                    break
                resp_len = struct.unpack(">i", len_buff)[0]
                if resp_len == 0:
                    logger.debug("resp_len == 0 error")
                    continue
                reply = ReplyHeader().deserialize(self.rfile)
                self.last = time.time()
                if reply.xid == ReplyType.EVENT:
                    self.dispatch_event(resp_len, reply)
                elif reply.xid == ReplyType.PING:
                    logger.info("got pong")
                elif reply.xid == ReplyType.AUTH:
                    logger.debug("got auth failed. should exit ...")
                else:
                    pending_packet, callback = self.pending_queue.get()
                    pending_packet.reply_header = reply
                    pending_packet.deserialize(self.rfile)
                    logger.debug("got reply %s" % pending_packet)
                    if callback:
                        callback(pending_packet)
            logger.warn("start_read exit")
            self.finish()
            if self.state == ProxyState.BROKEN:
                self.reconnect()
        except:
            logger.error("zkclient read eof %s" % traceback.format_exc())

    def start_ping(self):
        while self.running:
            duration = time.time() - self.last
            if duration > self.timeout:
                self.send_ping()
                logger.info("send_ping")
            time.sleep(1)

        logger.warn("start_ping exit")

    def send_ping(self):
        self.last = time.time()
        ping_req = Packet(Ping(), NopReq(), ReplyHeader(), NopResp())
        self.send_packet((ping_req, NopDo))

    def connect(self):
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        self.sockname = self.sock.getpeername()
        self.wfile = self.sock.makefile("wb")
        self.rfile = self.sock.makefile("rb")
        self.init()

    def finish(self):
        self.running = False
        self.state = ProxyState.DISCONNECTED
        self.queue.put((ExitSignal(), NopDo))
        if self.rfile:
            self.rfile.close()
            self.rfile = None

        if self.wfile:
            self.wfile.close()
            self.wfile = None

        if self.sock:
            self.sock.close()
            self.sock = None

    def close(self):
        self.finish()
        
    def reconnect(self):
        self.connect()
        logger.warn("reconnect to %s:%d" % self.sock.getpeername())

    def init(self):
        '''send connect req and wait resp'''

        password = struct.pack(">QQ", 0, 0)
        conn_req = ConnectReq(0, 0, 60000, 0, password)
        buff_io = StringIO()
        conn_req.serialize(buff_io)
        body_len = struct.pack(">i", len(buff_io.getvalue()))
        self.wfile.write(body_len)
        self.wfile.write(buff_io.getvalue())
        self.wfile.flush()

        resp_body_len = struct.unpack(">i", self.rfile.read(4))[0]
        conn_resp = ConnectResp()
        conn_resp.deserialize(self.rfile)
        self.timeout = int(conn_resp.timeout / 1000) - 5
        logger.info("connect to %s:%d Ok. connect response is %s, %d" % (self.host, self.port, conn_resp, self.timeout))
        self.start()


class ProxyState(object):
    INIT = 1
    CONNECTED = 2
    DISCONNECTED = 3
    BROKEN = 4


class ZkSession(object):
    def __init__(self):
        self.session = 100000

    @property
    def session_id(self):
        return self.session

    @property
    def next_session_id(self):
        self.session = self.session + 1
        return self.session

ZkSess = ZkSession()


OpCodeMap = {
            OpCode.create: (CreateReq, CreateResp),
            OpCode.exists: (ExistsReq, SetDataResponse),
            OpCode.setWatches: (SetWatches, NopResp),
            OpCode.getChildren: (GetChildReq, GetChildResp),
            OpCode.getData: (GetDataReq, GetDataResp),
            OpCode.delete: (DeleteReq, NopResp),
            OpCode.ping: (NopReq, NopResp),
            OpCode.setData: (SetDataReq, SetDataResponse),
}

class StringBuilder(object):
    def __init__(self):
        self.io_buff = StringIO()

    def appendRN(self, line):
        self.io_buff.write(line + "\r\n");
        return self

    def append(self, string):
        self.io_buff.write(string)
        return self

    def __str__(self):
        try:
            return self.io_buff.getvalue()
        finally:
            self.io_buff.close()

class ClientHandler(object):
    def __init__(self, sock, zkhost, zkport, ignore_path = None):
        self.zkclient = ZkClient(zkhost, zkport)
        self.client = sock
        self.wfile = sock.makefile("wb")
        self.rfile = sock.makefile("rb")
        self.state = ProxyState.INIT
        self.client_write_queue = Queue()
        self.xid = -100
        self.running = True
        self.ignore_path = ["/Cache_default", "/TaskLock"]
        self.heartbeat = 3600000*24
        self.un_acked = OrderedDict()
        self.lock = threading.Lock()

    def ack_connect(self):
        req_len = struct.unpack(">i", self.rfile.read(4))[0]
        io_buff = StringIO()
        io_buff.write(self.rfile.read(req_len))
        conn_req = ConnectReq(0, 0, 0, 0, "")
        io_buff.seek(0)
        conn_req.deserialize(io_buff)
        io_buff.close()

        conn_resp = ConnectResp().build(0, self.heartbeat, ZkSess.next_session_id, "1234567890123456")
        buff_io = StringIO()
        conn_resp.serialize(buff_io)
        resp_len = struct.pack(">i", len(buff_io.getvalue()))
        connect_packet = Packet(None, None, None, None, buff=resp_len + buff_io.getvalue())
        self.client_write_queue.put(connect_packet)
        buff_io.close()
        self.state = ProxyState.CONNECTED
        logger.debug("proxy connection started... %s" % conn_resp)

    def not_in_ignore(self, path):
        if not self.ignore_path:
            return True
        else:
            for ipath in self.ignore_path:
                if path.startswith(ipath):
                    return False
            return True

    def in_ignore(self, path):
        return not not_in_ignore(path)

    def process_event(self, event):
        #TODO update cache via event
        reply_len, reply_header, watcher = event
        logger.warn("got event %s %s" % (reply_header, watcher))
        io_buff = StringIO()
        try:
            record = Record()
            record.write_int(reply_len, io_buff)
            record.write_record(reply_header, io_buff)
            record.write_record(watcher, io_buff)
            event_packet = Packet(None, None, None, None, buff=io_buff.getvalue())
            self.client_write_queue.put(event_packet)
        finally:
            io_buff.close()

    def callback(self, packet):
        if packet.proxyed and self.not_in_ignore(packet.request.path):
            #packet from real zk server,ignore packet because clienthandler already return a cache packet instead it.
            logger.debug("ignore from real zk server %s" % packet)
            return

        try:
            self.lock.acquire()
            xid = packet.header.xid    

            if xid in self.un_acked and (not packet.acked):
                logger.debug("ack %s" % packet)
                packet.acked = True

            self.un_acked[xid] = packet
            ordered_packet_list = sorted(self.un_acked.items())
            un_acked_list = filter(lambda p: p[1].acked == False, ordered_packet_list)

            if len(un_acked_list) > 0:
                str_buff = StringBuilder().append("[\n")
                for _, un_acked_packet in un_acked_list:
                    str_buff.append(str(un_acked_packet)).append(",\n")
                str_buff.append("]\n")    
                logger.error("still has some packet unacked %s" % str_buff);
                return
            else:
                for _, acked_packet in ordered_packet_list:
                    self.client_write_queue.put(acked_packet)
                self.un_acked.clear()
        except:
            logger.error("got proxyed packet err %s" % traceback.format_exc())
        finally:
            self.lock.release()

    def handle_read(self):
        try:
            while self.running:
                if self.state == ProxyState.INIT:
                    self.ack_connect()
                if self.state == ProxyState.DISCONNECTED:
                    break
                elif self.state == ProxyState.CONNECTED:
                    req_len = struct.unpack(">i", self.rfile.read(4))[0]
                    req_header = RequestHeader(0, 0)
                    req_header.deserialize(self.rfile)
                    self.xid = req_header.xid
                    #if req_header.type == OpCode.closeSession:
                    #    self.finish()
                    if req_header.type == OpCode.ping:
                        self.send_pong()
                        continue
                    if req_header.type not in OpCodeMap:
                        logger.debug("no such req type %d" % req_header.type)
                    else:
                        #forward req to real zk server
                        reqcls, respcls = OpCodeMap[req_header.type]
                        req = reqcls().deserialize(self.rfile)
                        req_packet = Packet(req_header, req, ReplyHeader(), respcls())
                        logger.debug("req %s " % (req_packet, ))
                        self.un_acked[req_header.xid] = req_packet
                        if not self.do_filter_handler(req_packet):
                            self.zkclient.send_packet((req_packet, self.callback))
                            logger.debug("forward %s to real server" % req_packet)
            logger.warn("handle_read exit")                
        except:
            logger.error("handle_read error %s " % (traceback.format_exc(), ))
            self.running = False
            self.state = ProxyState.DISCONNECTED
        finally:
            self.finish()


    def init_filter_chain(self):
        self.chain = {CreateReq: self.intecept_create, GetChildReq: self.intecept_getchild, ExistsReq: self.intecept_exists}

    def do_filter_handler(self, packet):
        handler = self.chain.get(packet.request.__class__)
        if handler:
            path = packet.request.path
            if not self.not_in_ignore(path):
                logger.debug("ignore path %s" % path)
                return False
            else:
                return handler(packet)
        else:
            return False

    def intecept_create(self, create_req):
        if zkCache.exists(create_req.request.path):
            reply_header = create_req.reply_header
            reply_header.build(create_req.header.xid, 0, ErrCode.NodeExists)
            create_req.acked = True
            self.callback(create_req)
            logger.debug("return cache for %s" % create_req)
            return True
        else:
            path = create_req.request.path
            parent, child = zkCache.split(path)
            child_list = zkCache.get(parent)
            child_list.append(child)
            zkCache.put(parent, child_list)
            reply_header = create_req.reply_header
            reply_header.build(create_req.header.xid, 0, 0)
            create_req.response.path = path
            create_req.acked = True
            self.callback(create_req)
            create_req.proxyed = True
            logger.debug("try add %s to cache path %s " % (child, parent))
            return False #forward create_req to real zk server

    def intecept_exists(self, exists_req):
        if zkCache.exists(exists_req.request.path):
            reply_header = exists_req.reply_header
            reply_header.build(exists_req.header.xid, 0, 0)
            response = exists_req.response
            stat = Stat()
            response.stat = Stat().build(0,0,0,0,0,0,0,0,0,0,00)
            exists_req.acked = True
            self.callback(exists_req)
            logger.debug("return cache exists for %s" % exists_req)
            return True
        else:
            return False


    def intecept_getchild(self, getchild_req):
        childrens = zkCache.get(getchild_req.request.path)
        reply_header = getchild_req.reply_header
        reply_header.build(getchild_req.header.xid, 0, 0)
        response = getchild_req.response
        response.childrens = childrens
        getchild_req.acked = True
        self.callback(getchild_req)
        logger.warn("return cache path's %s child in cache %s" % (getchild_req.request.path, childrens))
        return True

    def send_pong(self):
        pong_packet = Packet(None, None, Pong(), None)
        self.client_write_queue.put(pong_packet)

    def finish(self):
        self.client_write_queue.put(ExitSignal())
        self.state = ProxyState.DISCONNECTED
        self.zkclient.close()

    def handle_write(self):
        '''forward zk server response to client'''
        while True:
            packet = self.client_write_queue.get()
            if not packet:
                continue
            else:
                if isinstance(packet, ExitSignal):
                    break
                if packet.buff:
                    send_buff(self.wfile, packet.buff)
                else:
                    buff = packet.serialize_forward()
                    send_buff(self.wfile, buff)
                    if not isinstance(packet.reply_header, Pong):
                        logger.debug("write zk response %s to client" % packet)
        logger.warn("handle_write exit")

    def start(self):
        self.init_filter_chain()
        self.zkclient.connect()
        self.zkclient.subscribe(self.process_event)
        reader = threading.Thread(target=self.handle_read)
        reader.setDaemon(True)
        reader.start()

        writer = threading.Thread(target=self.handle_write)
        writer.setDaemon(True)
        writer.start()


class ZkProxy(object):
    def __init__(self, host, port, zkhost, zkport, backlog=128):
        self.host = host
        self.port = port
        self.backlog = backlog
        self.zkhost = zkhost
        self.zkport = zkport

    def serve_forever(self):
        server_sock = socket(AF_INET, SOCK_STREAM)
        server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(self.backlog)
        self.server_sock = server_sock
        while True:
            client_sock, address = self.server_sock.accept()
            self.process_request(client_sock)

    def process_request(self, client_sock):
        ClientHandler(client_sock, self.zkhost, self.zkport).start()


class Worker(object):
    def __init__(self, master, zkhost, zkport):
        self.queue = master.queue
        self.running = False
        self.client = ZkClient(zkhost, zkport)
        self.master = master

    def submit(self, parent, node):
        self.queue.put((parent, node))

    def notify(self, parent, packet):
        self.master.notify((parent, packet))

    def run(self):
        self.running = True
        self.client.connect()
        logger.debug("Worker Thread started")
        while self.running:
            parent, node = self.queue.get()
            nodename = "%s/%s" % (parent, node)
            logger.debug("getChild for %s" % nodename)
            get_child_req = GetChildReq().build(nodename, False)
            child_packet = Packet(RequestHeader(Xid.next_xid(), OpCode.getChildren), get_child_req, ReplyHeader(), GetChildResp())
            callback = partial(self.notify, parent)
            self.client.send_packet((child_packet, callback))
        logger.debug("worker exit")    

    def close(self):
        self.client.close()

class ControlMaster(object):
    def __init__(self, bootstrap_nodes, pool_size, zkhost, zkport):
        self.tasks = []
        self.zkcache = None
        self.pool_size = pool_size
        self.zkhost = zkhost
        self.zkport = zkport
        self.queue = Queue()
        self.bootstrap_nodes = bootstrap_nodes
        self.last = time.time()
        self.await = Queue()

    def on_idle(self):
        for worker in self.tasks:
            worker.close()

        self.await.put(ExitSignal)

    def detect_idle(self):
        while True:
            if time.time() - self.last > 5:
                self.on_idle()
                break
            time.sleep(1)

    def initworker(self):
        '''
        create worker thread
        '''
        for _ in range(self.pool_size):
            worker = Worker(self, self.zkhost, self.zkport)
            thread = threading.Thread(target=worker.run)
            thread.setDaemon(True)
            thread.start()
            self.tasks.append(worker)

    def notify(self, nodes):
        '''
        1. receive nodelist
        2. add nodelist to map
        3. split nodelist to single node set
        4. put single to tasker
        5. wait notify
        node: {"/dubbo":"com.supercom.eif"}
        '''
        self.last = time.time()
        parent, packet = nodes
        path = packet.request.path
        if packet.response.childrens:
            zkCache.put(path, packet.response.childrens)
        for child in packet.response.childrens:
            nodename = "%s/%s" % (path, child)
            self.queue.put((path, child))

    def start(self):
        self.initworker()
        threading.Thread(target=self.detect_idle).start()
        zkCache.put("/dubbo", self.bootstrap_nodes)
        for node in self.bootstrap_nodes:
            self.queue.put(("/dubbo", node))

    def wait(self):
        self.await.get()


LEVEL = {"debug": logging.DEBUG , "info": logging.INFO, "warn":logging.WARN, "error": logging.ERROR}

def set_loglevel():
    if len(sys.argv) == 6:
        level = sys.argv[5]
    else:
        level = "debug"

    logger.setLevel(LEVEL.get(level))
    print("set logger level to %s" % level)


class TelnetServer(threading.Thread):
    '''
    1. receive cmd param \r\n
    2. execute cmd with param and write response
    3. if cmd is quit, then close socket
    '''

    def __init__(self, host, port):
        super(TelnetServer, self).__init__()
        self.setDaemon(True)
        self.host = host
        self.port = port
        self.zkCache = zkCache
        self.server_sock = None

    def run(self):
        self.listen()
        print("simple telnet server started listen on %s:%d" % (self.host, self.port))
        while True:
            client, addr = self.server_sock.accept()
            print("accept client %s:%d" % addr)
            self.handle_request(client)

    def listen(self):
        self.server_sock = socket(AF_INET, SOCK_STREAM)
        self.server_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.server_sock.bind((self.host, self.port))
        self.server_sock.listen(128)

    def writeline(self, val):
        self.wfile.write(val + "\r\n")
        self.wfile.flush()

    def process_cmd(self, cmd):
        cmd_args = cmd.split()
        if len(cmd_args) == 0:
            return
        cmd = cmd_args[0]
        if cmd in ["get", "len", "find", "split", "map.get"] and len(cmd_args) < 2:
            self.writeline("cmd %s need args" % cmd)
            return
        if cmd == "get":
            args = cmd_args[1]
            val = self.zkCache.get(args)
            valstr = json.dumps(val, True)
            self.writeline(valstr)
        elif cmd == "len":
            args = cmd_args[1]
            val = self.zkCache.get(args)
            if val:
                val_len = len(val)
                self.writeline(str(val_len))
            else:
                self.writeline("%s not found" % args)
        elif cmd =="keys":
            val = self.zkCache.keys()
            self.writeline(json.dumps(val, True))
        elif cmd == "find":
            args = cmd_args[1]
            vals = []
            for k, v in self.zkCache.map.iteritems():
                if k.find(args) > -1:
                    vals.append(k)
            self.writeline(json.dumps(vals, True))
        elif cmd == "split":
            args = cmd_args[1]
            parent, child = self.zkCache.split(args)
            self.writeline("parent=%s, child=%s" % (parent, child))
        elif cmd =="map.get":
            args = cmd_args[1]
            val = self.zkCache.map.get(args)
            self.writeline(json.dumps(val, True))
        else:
            self.writeline("unsupported cmd %s" % cmd_args[0])

    def close(self, client):
        self.rfile.close()
        self.wfile.close()
        client.close()

    def handle_request(self, client):
        self.rfile = client.makefile("r")
        self.wfile = client.makefile("w")
        while True:
            cmdline = self.rfile.readline().strip()
            if cmdline == "quit":
                self.close(client)
                break
            else:
                self.process_cmd(cmdline)


if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("usage: zkclient zkhost, zkport ,listenhost, listenport [debug|info|warn|error]")
    else:
        zkhost, zkport, listenhost, listenport = sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4])
        set_loglevel()
        client = BlockZkClient(zkhost, zkport)
        client.connect()
        start = time.time()
        try:
            bootstrap = iter_all_child((client.rfile, client.wfile), ["dubbo"], "")
            master = ControlMaster(bootstrap, 10, zkhost, zkport)
            master.start()
            master.wait()
            client.close()
            
            TelnetServer("127.0.0.1", 2222).start()
            print("zkproxy started")
            # start zk proxy and start serve
            zkproxy = ZkProxy(listenhost, listenport, zkhost, zkport)
            zkproxy.serve_forever()
        except:
            logger.error(traceback.format_exc())

        print("total node is %d" % len(zkCache.map))
        print("time %f" % (time.time() - start))


