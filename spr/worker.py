import multiprocessing as mp
import threading
import time

from spr.common.pipe import Pipe
from .task import Task


class ExitTask(Task):
  def __init__(self):
    pass

  def run(self):
    raise SystemExit


class WorkerBackend(mp.Process):
  def __init__(self, pipe):
    self.pipe = pipe
    super().__init__(daemon=True)

  def run(self):
    heartbeat = threading.Thread(target=self.send_heartbeat)
    heartbeat.start()
    while True:
      has_content = self.pipe.poll(60)
      if not has_content:
        # 如果一分钟没有收到消息，可能主进程已经退出了，或者最近没有新任务需要执行。所以Worker可以退出
        self.send(("exit", None))
        return
      task = self.recv()
      try:
        result = task.run()
      except Exception as e:
        result = e
      except SystemExit:
        self.send(("exit", None))
        return
      self.send(("finish", (task.tid, result)))

  def send(self, item):
    try:
      self.pipe.send(item)
    except BrokenPipeError:
      raise SystemExit

  def recv(self):
    try:
      return self.pipe.recv()
    except BrokenPipeError:
      raise SystemExit

  def send_heartbeat(self):
    while True:
      self.send(("heartbeat", None))
      time.sleep(5)


class Worker:
  def __init__(self):
    self._pipe, pipe = Pipe()
    self._backend = WorkerBackend(pipe)
    self._backend.start()
    self._ready = True

  def ready(self) -> bool:
    return self._ready

  def refresh(self):
    if self._pipe.poll(0.0):
      event_type, detail = self._pipe.recv()
      if event_type == "finish":
        self._ready = True
      return event_type, detail
    else:
      return None

  def submit_task(self, task: Task):
    self._pipe.send(task)
    self._ready = False

  def exit(self):
    self._pipe.send(ExitTask())

  def fileno(self) -> int:
    return self._pipe.fileno()

  def is_alive(self) -> bool:
    return self._backend.is_alive()
