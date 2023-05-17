from collections import deque


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
