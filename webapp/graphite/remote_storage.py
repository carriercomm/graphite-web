import socket
import time
import httplib
from urllib import urlencode
from django.core.cache import cache
from django.conf import settings
from graphite.render.hashing import compactHash
import pycurl

from graphite.logger import log

try:
  import cPickle as pickle
except ImportError:
  import pickle

try:
  import cStringIO as StringIO
except ImportError:
  import StringIO


class RemoteOperation:
  def __init__(self):
    self.buf = None
    self.curl_handle = None

  def _set_store(self, store):
    self.store = store

  def _write(self, buf):
    self.buf.write(buf)

  def timeout(self):
    self.buf = StringIO.StringIO()
    url = self._build_url()
    log.info('curl timed out fetching %s' % url)

  def _build_url(self):
    pass

  def get_handle(self):
    return self.curl_handle

  def start(self):
    self.curl_handle = pycurl.Curl()
    url = self._build_url()
    log.info('curl is fetching %s' % url)
    self.buf = StringIO.StringIO()
    self.curl_handle.setopt(self.curl_handle.URL, url)
    self.curl_handle.setopt(self.curl_handle.WRITEFUNCTION, self._write)

  def finish(self):
    bufstring = ''
    if self.buf:
      bufstring = self.buf.getvalue()
    if bufstring != '':
      try:
        self.load_data(pickle.loads(bufstring))
      except pickle.UnpicklingError:
        log.exception('error: %s returned unpicklable: %s' % (self._build_url(), bufstring))
        raise
    else:
      self.store.fail()
      if not self.suppressErrors:
        raise
      else:
        self.load_data([])

  def load_data(self, data):
    pass

class RemoteCoordinator:
  def __init__(self, operations=None):
    self.operations = operations or []
    self.multi = None

  def add_operation(self, operation):
    self.operations.append(operation)

  def start(self):
    if not self.operations:
      return

    self.multi = pycurl.CurlMulti()
    for o in self.operations:
      o.start()
      self.multi.add_handle(o.get_handle())

    while 1:
      ret, num_handles = self.multi.perform()
      if ret != pycurl.E_CALL_MULTI_PERFORM:
        break

  def finish(self, timeout=settings.REMOTE_STORE_FIND_TIMEOUT):
    if not self.multi:
      return
    num_handles = len(self.operations)

    SELECT_TIMEOUT = 1.0
    # Stir the state machine into action
    timeout_count = 0

    # Keep going until all the connections have terminated
    while num_handles != 0:
      if timeout_count > timeout:
        for o in self.operations:
          o.timeout()
        break
      # The select method uses fdset internally to determine which file descriptors
      # to check.
      if self.multi.select(SELECT_TIMEOUT) == -1:
        timeout_count += 1

      while 1:
        ret, num_handles = self.multi.perform()
        if ret != pycurl.E_CALL_MULTI_PERFORM:
          break
    for o in self.operations:
      self.multi.remove_handle(o.get_handle())
      o.finish()
      o.get_handle().close()

    self.multi.close()
    self.multi = None


class RemoteStore(object):
  lastFailure = 0.0
  retryDelay = settings.REMOTE_STORE_RETRY_DELAY
  available = property(lambda self: time.time() - self.lastFailure > self.retryDelay)

  def __init__(self, host):
    self.host = host


  def find(self, query):
    return FindRequest(self, query)

  def fail(self):
    self.lastFailure = time.time()


class FindRequest(RemoteOperation):
  suppressErrors = True

  def __init__(self, store, query):
    self.store = store
    self.query = query
    self.connection = None
    self.cacheKey = compactHash('find:%s:%s' % (self.store.host, query))
    self.cachedResults = None
    RemoteOperation.__init__(self)

  def _build_url(self):
    query_params = [
      ('local', '1'),
      ('format', 'pickle'),
      ('query', self.query),
    ]
    query_string = urlencode(query_params)
    return  'http://%s/metrics/find/?%s' % (self.store.host, query_string)

  def load_data(self, results):
    fetcher = RemoteFetch(self.store, self.query)
    resultNodes = [ RemoteNode(self.store, fetcher, node['metric_path'], node['isLeaf']) for node in results ]
    cache.set(self.cacheKey, resultNodes, settings.REMOTE_FIND_CACHE_DURATION)
    self.cachedResults = resultNodes

  def get_results(self):
    return self.cachedResults


class RemoteFetch(RemoteOperation):
  suppressErrors = True
  def __init__(self, store, query):
    self.store = store
    self.query = query
    self.data = {}
    self.info = {}
    self.curl_handle = None
    RemoteOperation.__init__(self)

  def _build_url(self):
    query_params = [
      ('target', self.query),
      ('format', 'pickle'),
      ('local', '1'),
      ('from', str( int(self.startTime) )),
      ('until', str( int(self.endTime) ))
    ]
    query_string = urlencode(query_params)
    return  'http://%s/render/?%s' % (self.store.host, query_string)

  def fetch(self, startTime, endTime):
    self.startTime = startTime
    self.endTime = endTime

  def load_data(self, seriesList):
    for series in seriesList:
      timeInfo = (series['start'], series['end'], series['step'])
      self.data[series['name']] = (timeInfo, series['values'])
      self.info[series['name']] = series['info']


class RemoteNode:
  context = {}

  def __init__(self, store, fetcher, metric_path, isLeaf):
    self.store = store
    self.fs_path = None
    self.metric_path = metric_path
    self.real_metric = metric_path
    self.name = metric_path.split('.')[-1]
    self.__isLeaf = isLeaf
    self.is_remote = True
    self.fetcher = fetcher


  def fetch(self, startTime, endTime):
    if not self.__isLeaf:
      return []

    try:
      return self.fetcher.data[self.metric_path]
    except KeyError:
      log.info('%s not in %s' % (self.metric_path, repr(self.fetcher.data)))
      raise


  def isLeaf(self):
    return self.__isLeaf

  def getInfo(self):
    return self.fetcher.info[self.metric_path]


# This is a hack to put a timeout in the connect() of an HTTP request.
# Python 2.6 supports this already, but many Graphite installations
# are not on 2.6 yet.

class HTTPConnectionWithTimeout(httplib.HTTPConnection):
  timeout = 30

  def connect(self):
    msg = "getaddrinfo returns an empty list"
    for res in socket.getaddrinfo(self.host, self.port, 0, socket.SOCK_STREAM):
      af, socktype, proto, canonname, sa = res
      try:
        self.sock = socket.socket(af, socktype, proto)
        try:
          self.sock.settimeout( float(self.timeout) ) # default self.timeout is an object() in 2.6
        except:
          pass
        self.sock.connect(sa)
        self.sock.settimeout(None)
      except socket.error, msg:
        if self.sock:
          self.sock.close()
          self.sock = None
          continue
      break
    if not self.sock:
      raise socket.error, msg
