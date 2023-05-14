import functools
import multiprocessing as mp
import cloudpickle as pickle


class Connection:
  def __init__(self, conn):
    self.conn = conn

  def fileno(self):
    return self.conn.fileno()

  def send(self, obj):
    return self.send_bytes(pickle.dumps(obj))

  def send_bytes(self, bytes):
    return self.conn.send_bytes(bytes)

  def recv(self):
    return pickle.loads(self.recv_bytes())

  def recv_bytes(self):
    return self.conn.recv_bytes()

  def poll(self, timeout=0.0):
    return self.conn.poll(timeout)

  def close(self):
    return self.conn.close()

  @property
  def closed(self):
    return self.conn.closed

  @property
  def readable(self):
    return self.conn.readable

  @property
  def writable(self):
    return self.conn.writable


@functools.wraps(mp.Pipe)
def Pipe(*args, **kwargs):
  a, b = mp.Pipe(*args, **kwargs)
  return Connection(a), Connection(b)
