import time
import subprocess
import os.path
import threading
from django.conf import settings
from graphite.logger import log
from graphite.storage import is_pattern, match_entries


class IndexSearcher:
  def __init__(self, index_path):
    self.index_path = index_path
    self.lock = threading.Lock()
    if not os.path.exists(index_path):
      open(index_path, 'w').close() # touch the file to prevent re-entry down this code path
      build_index_path = os.path.join(settings.GRAPHITE_ROOT, "bin/build-index.sh")
      retcode = subprocess.call(build_index_path)
      if retcode != 0:
        log.exception("Couldn't build index file %s" % index_path)
        raise RuntimeError("Couldn't build index file %s" % index_path)
    self.last_mtime = 0
    self._tree = (set(), {}) # (data, children)
    log.info("[IndexSearcher] performing initial index load")
    self.reload()

  @property
  def tree(self):
    self.lock.acquire()
    try:
      current_mtime = os.path.getmtime(self.index_path)
      if current_mtime > self.last_mtime:
        log.info("[IndexSearcher] reloading stale index %s, current_mtime=%s last_mtime=%s" %
                 (self.index_path, time.ctime(current_mtime), time.ctime(self.last_mtime)))
        self.reload()
    finally:
      self.lock.release()

    return self._tree

  def reload(self):
    log.info("[IndexSearcher] reading index data from %s" % self.index_path)
    t = time.time()
    total_entries = 0
    tree = self._tree #(set(), {}) # (leaves, branches)

    for line in open(self.index_path):
      line = line.strip()
      if not line:
        continue

      branches = line.split('.')
      leaf = branches.pop()
      parent = None
      cursor = tree
      for branch in branches:
        if branch not in cursor[1]:
          cursor[1][branch] = (set(), {}) # (leaves, branches)
        cursor = cursor[1][branch]

      cursor[0].add(leaf)
      total_entries += 1

    self._tree = tree
    self.last_mtime = os.path.getmtime(self.index_path)
    log.info("[IndexSearcher] index reload took %.6f seconds (%d entries)" % (time.time() - t, total_entries))

  # compare to: graphite.storage._find
  def _find(self, prefix, patterns):
    for match in self.__find([], self.tree, patterns):
      path = prefix
      for part in match[0]:
        path = os.path.join(path, part)
      if match[1]:
        path = path + '.wsp'
      yield path


  def __find(self, path, current, patterns):
    pattern = patterns[0]
    patterns = patterns[1:]

    matching_subdirs = match_entries(current[1], pattern)

    if patterns:
      for subdir in matching_subdirs:
        new_path = path[:]
        new_path.append(subdir)
        for match in self.__find(new_path, current[1][subdir], patterns):
          yield match

    else:
      matching_files = match_entries(current[0], pattern)

      for subdir in matching_subdirs:
        new_path = path[:]
        new_path.append(subdir)
        yield (new_path, False)

      for matching_file in matching_files:
        new_path = path[:]
        new_path.append(matching_file)
        yield (new_path, True)



  def search(self, query, max_results=None, keep_query_pattern=False):
    query_parts = query.split('.')
    metrics_found = set()
    for result in self.subtree_query(self.tree, query_parts):
      # Overlay the query pattern on the resulting paths
      if keep_query_pattern:
        path_parts = result['path'].split('.')
        result['path'] = '.'.join(query_parts) + result['path'][len(query_parts):]

      if result['path'] in metrics_found:
        continue
      yield result

      metrics_found.add(result['path'])
      if max_results is not None and len(metrics_found) >= max_results:
        return

  def subtree_query(self, root, query_parts):
    for match in self.__find([], root, query_parts):
      yield {
        'path': '.'.join(match[0]),
        'is_leaf' : match[1]
      }

class SearchIndexCorrupt(StandardError):
  pass


searcher = IndexSearcher(settings.INDEX_FILE)
