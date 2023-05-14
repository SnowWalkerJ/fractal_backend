from select import select
from typing import List
from .worker import Worker


class WorkerPool:
  def __init__(self):
    self._workers: List[Worker] = []
    self._ready_workers: List[Worker] = []

  def submit(self, task):
    if self._ready_workers:
      worker = self._ready_workers.pop(-1)
    else:
      worker = self._create_worker()
    worker.submit_task(task)
    return True

  def check(self):
    readables, _, _ = select(self._workers, [], [], 0.001)
    for worker in readables:
      result = worker.refresh()
      if result is None:
        continue
      event_type, event_detail = result
      yield from getattr(self, f"_on_worker_{event_type}")(worker, event_detail)

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
    yield from []

  def _on_worker_exit(self, worker, event_detail):
    if worker in self._ready_workers:
      self._ready_workers.remove(worker)
    self._workers.remove(worker)
    yield from []
