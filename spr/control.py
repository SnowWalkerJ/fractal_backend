from collections import defaultdict, deque
import multiprocessing as mp
import psutil
from typing import Dict, Set, Union
from .task import Task
from .pool import WorkerPool
from .pipe import Pipe


class Queue:
  def __init__(self):
    self.q = deque()

  def push(self, item):
    self.q.append(item)

  def front(self):
    return self.q[0]

  def pop(self):
    return self.q.popleft()

  def __len__(self):
    return len(self.q)

  def __bool__(self):
    return bool(self.q)


class ControlBackend(mp.Process):
  def __init__(self, pipe, max_resource: Dict[str, int]):
    super().__init__()
    self.pipe = pipe
    self.pool = WorkerPool()
    self.resource = max_resource.copy()
    self.available_tasks = Queue()
    self.task_forward_relationship: Dict[int, Set[int]] = defaultdict(set)
    self.task_unresolved_dependencies: Dict[int, Set[int]] = defaultdict(set)
    self.tasks: Dict[int, Task] = {}
    self.finished_tasks: Set[int] = set()
    self.waiting_tasks: Set[int] = set()

  def run(self) -> None:
    while True:
      while self.pipe.poll(0.0):
        cmd, detail = self.pipe.recv()
        self.pipe.send(getattr(self, f"on_cmd_{cmd}")(detail))
      for task, result in self.pool.check():
        self.on_task_finish(task, result)

  def on_task_finish(self, tid, result):
    task = self.tasks[tid]
    self.return_resource(task)
    if isinstance(result, Exception):
      raise result
    self.waiting_tasks.remove(tid)
    for forward_tid in self.task_forward_relationship.pop(tid, set()):
      dependency = self.task_unresolved_dependencies[forward_tid]
      dependency.remove(tid)
      if not dependency:
        self.task_unresolved_dependencies.pop(forward_tid)
        self.available_tasks.push(forward_tid)
    self.finished_tasks.add(tid)
    self.try_to_run_task()

  def try_to_run_task(self):
    while self.available_tasks:
      tid = self.available_tasks.front()
      task = self.tasks[tid]
      if self.resource_available(task.resources):
        self.run_task(task)
        self.available_tasks.pop()
        self.waiting_tasks.add(tid)
      else:
        break

  def resource_available(self, resource: Dict[str, int]) -> bool:
    for name, amount in resource.items():
      if self.resource[name] < amount:
        return False
    return True

  def run_task(self, task):
    for name, amount in task.resources.items():
      self.resource[name] -= amount
    self.pool.submit(task)

  def return_resource(self, task):
    for name, amount in task.resources.items():
      self.resource[name] += amount

  def on_cmd_submit(self, task):
    task_id = task.tid
    self.tasks[task_id] = task
    unresolved_dependencies = set()
    for dep_tid in task.dependency:
      if dep_tid not in self.tasks:
        return ValueError("unrecognized tid")
      if dep_tid not in self.finished_tasks:
        self.task_forward_relationship[dep_tid].add(task_id)
        unresolved_dependencies.add(dep_tid)
    if unresolved_dependencies:
      self.task_unresolved_dependencies[task_id] = unresolved_dependencies
    else:
      self.available_tasks.push(task_id)
      self.try_to_run_task()

  def on_cmd_exit(self, _):
    self.pool.close()
    raise SystemExit()

  def on_cmd_check_task(self, tasks: Set[int]) -> Set[int]:
    return tasks & self.finished_tasks


class ControlCenter:
  def __init__(self, resources=None):
    if resources is None:
      resources = {}
    resources.setdefault("num_cpus", mp.cpu_count())
    resources.setdefault("memory", psutil.virtual_memory().total)
    self._pipe, pipe = Pipe()
    self._backend = ControlBackend(pipe, resources)
    self._backend.start()

  def wait_any(self, tids: Union[int, Set[int]]):
    if isinstance(tids, int):
      tids = {tids}
    else:
      tids = set(tids)
    self._pipe.send("check_task", tids)
    finished = self._pipe.recv()
    return finished

  def wait_all(self, tids: Union[int, Set[int]]):
    for _ in self.wait_iter(tids):
      pass

  def wait_iter(self, tids: Union[int, Set[int]]):
    if isinstance(tids, int):
      tids = {tids}
    else:
      tids = set(tids)
    while tids:
      self._pipe.send(("check_task", tids))
      finished = self._pipe.recv()
      yield from finished
      tids -= finished

  def submit(self, task):
    self._pipe.send(("submit", task))
    result = self._pipe.recv()
    if isinstance(result, Exception):
      raise result

  def __del__(self):
    self.close()

  def is_alive(self):
    return self._backend is not None

  def close(self):
    if self.is_alive():
      print("closing")
      self._pipe.send(("exit", None))
      self._backend.join()
      self._backend.close()
      self._backend = None
