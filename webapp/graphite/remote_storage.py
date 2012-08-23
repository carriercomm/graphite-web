import socket
import time
import httplib
from urllib import urlencode
from django.core.cache import cache
from django.conf import settings
from graphite.logger import log
from graphite.render.hashing import compactHash

try:
  import cPickle as pickle
except ImportError:
  import pickle

try:
  import urllib3
  HAS_CONN_POOLING = True
except:
  HAS_CONN_POOLING = False

class RemoteStore(object):
  lastFailure = 0.0
  retryDelay = settings.REMOTE_STORE_RETRY_DELAY
  available = property(lambda self: time.time() - self.lastFailure > self.retryDelay)

  def __init__(self, host):
    self.host = host
    if HAS_CONN_POOLING:
      self.pool = urllib3.HTTPConnectionPool(host, maxsize=10, timeout=30, block=False)
    else:
      self.pool = None


  def find(self, query):
    request = FindRequest(self, query)
    request.send()
    return request


  def fail(self):
    self.lastFailure = time.time()



class FindRequest:
  suppressErrors = True

  def __init__(self, store, query):
    self.store = store
    self.query = query
    self.connection = None
    self.cacheKey = compactHash('find:%s:%s' % (self.store.host, query))
    self.cachedResults = None


  def send(self):
    self.cachedResults = cache.get(self.cacheKey)

    if self.cachedResults:
      return

    if not HAS_CONN_POOLING:
      self.connection = HTTPConnectionWithTimeout(self.store.host)
      self.connection.timeout = settings.REMOTE_STORE_FIND_TIMEOUT

    query_params = [
      ('local', '1'),
      ('format', 'pickle'),
      ('query', self.query),
    ]
    query_string = urlencode(query_params)

    try:
      if HAS_CONN_POOLING:
        self.connection = self.store.pool.request('GET', '/metrics/find/?' + query_string)
      else:
        self.connection.request('GET', '/metrics/find/?' + query_string)
    except:
      log.exception()
      self.store.fail()
      if not self.suppressErrors:
        raise


  def get_results(self):
    if self.cachedResults:
      return self.cachedResults

    if not self.connection:
      self.send()

    try:
      if HAS_CONN_POOLING:
        response = self.connection
      else:
        response = self.connection.getresponse()
      assert response.status == 200, "received error response %s - %s" % (response.status, response.reason)
      if HAS_CONN_POOLING:
        result_data = response.data
      else:
        result_data = response.read()
      results = pickle.loads(result_data)

    except:
      log.exception()
      self.store.fail()
      if not self.suppressErrors:
        raise
      else:
        results = []

    resultNodes = [ RemoteNode(self.store, node['metric_path'], node['isLeaf']) for node in results ]
    cache.set(self.cacheKey, resultNodes, settings.REMOTE_FIND_CACHE_DURATION)
    self.cachedResults = resultNodes
    return resultNodes



class RemoteNode:
  context = {}

  def __init__(self, store, metric_path, isLeaf):
    self.store = store
    self.fs_path = None
    self.metric_path = metric_path
    self.real_metric = metric_path
    self.name = metric_path.split('.')[-1]
    self.__isLeaf = isLeaf
    self.is_remote = True


  def fetch(self, startTime, endTime):
    if not self.__isLeaf:
      return []

    query_params = [
      ('target', self.metric_path),
      ('format', 'pickle'),
      ('local', '1'),
      ('from', str( int(startTime) )),
      ('until', str( int(endTime) ))
    ]
    query_string = urlencode(query_params)

    if HAS_CONN_POOLING:
      connection = self.store.pool
    else:
      connection = HTTPConnectionWithTimeout(self.store.host)
      connection.timeout = settings.REMOTE_STORE_FETCH_TIMEOUT
    response = connection.request('GET', '/render/?' + query_string)
    if not HAS_CONN_POOLING:
      response = connection.getresponse()
    assert response.status == 200, "Failed to retrieve remote data: %d %s" % (response.status, response.reason)
    if HAS_CONN_POOLING:
      rawData = response.data
    else:
      rawData = response.read()

    seriesList = pickle.loads(rawData)
    assert len(seriesList) == 1, "Invalid result: seriesList=%s" % str(seriesList)
    series = seriesList[0]

    timeInfo = (series['start'], series['end'], series['step'])
    return (timeInfo, series['values'])


  def isLeaf(self):
    return self.__isLeaf



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
