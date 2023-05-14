from typing import Any, Dict, Set, Union


class Task:
  def __init__(self, func, args, kwargs, tid: int, dependency: Set[int], resources: Dict[str, Union[int, float]]):
    self.func = func
    self.args = args
    self.kwargs = kwargs
    self.dependency = dependency
    self.resources = resources
    self.tid = tid

  def run(self) -> Any:
    return self.func(*self.args, **self.kwargs)

  def __hash__(self):
    return self.tid
