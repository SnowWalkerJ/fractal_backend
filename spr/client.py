import atexit
import random
from typing import Set, Union, Iterator
import weakref
from .task import Task
from .control import ControlCenter


class Client:
  def __init__(self, resources=None):
    if resources is None:
      resources = {}
    self.control = ControlCenter(resources)
    atexit.register(Client._close_left_client, weakref.ref(self))

  def submit(self, func, args=(), kwargs={}, dependency=None, resources=None) -> int:
    if resources is None:
      resources = {}
    resources.setdefault("num_cpus", 1)
    resources.setdefault("memory", 0)
    if dependency is None:
      dependency = {}
    task_id = random.randrange(0xffffffffffffffff)
    task = Task(func, args, kwargs, task_id, dependency, resources)
    self.control.submit(task)
    return task_id

  def wait_all(self, task_ids: Union[int, Set[int]]) -> None:
    return self.control.wait_all(task_ids)

  def wait_any(self, task_ids: Union[int, Set[int]]) -> Set[int]:
    return self.control.wait_any(task_ids)

  def wait_iter(self, task_ids: Union[int, Set[int]]) -> Iterator[int]:
    return self.control.wait_iter(task_ids)

  def close(self):
    self.control.close()

  def __del__(self):
    self.close()

  @staticmethod
  def _close_left_client(ref):
    if ref:
      ref().close()
