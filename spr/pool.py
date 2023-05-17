from select import select
import time
from typing import List, Optional
from .worker import Worker
from spr.common.lru_table import LRUTable


class WorkerPool:
  def __init__(self):
    self._workers: List[Worker] = []
    self._ready_workers: List[Worker] = []
    self._heartbeat_monitor = LRUTable()

  def submit(self, task):
    if self._ready_workers:
      worker = self._ready_workers.pop(-1)
    else:
      worker = self._create_worker()
    worker.submit_task(task)
    return True

  def check_worker_messages(self):
    readables, _, _ = select(self._workers, [], [], 0.001)
    for worker in readables:
      result = worker.refresh()
      if result is None:
        continue
      event_type, event_detail = result
      yield from getattr(self, f"_on_worker_{event_type}")(worker, event_detail)

  def check_worker_health(self, max_interval: float) -> Optional[Worker]:
    node = self._heartbeat_monitor.get_lru_node()
    if node is None:
      return None
    if time.time() - node.timestamp > max_interval:
      return node.value

  def close(self):
    for worker in self._workers:
      worker.exit()

  def _create_worker(self):
    worker = Worker()
    self._workers.append(worker)
    return worker

  def _on_worker_finish(self, worker, event_detail):
    self._ready_workers.append(worker)
    yield event_detail

  def _on_worker_heartbeat(self, worker, event_detail):
    self._heartbeat_monitor.update(worker, time.time())
    yield from []

  def _on_worker_exit(self, worker, event_detail):
    if worker in self._ready_workers:
      self._ready_workers.remove(worker)
    self._workers.remove(worker)
    yield from []

