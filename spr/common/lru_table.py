from typing import Any, Dict, Optional


class LRUNode:
  def __init__(self, value, timestamp, left=None, right=None):
    self.value = value
    self.timestamp = timestamp
    self.left = left
    self.right = right


class LRUTable:
  def __init__(self):
    self._head: Optional[LRUNode] = None
    self._tail: Optional[LRUNode] = None
    self._index: Dict[Any, LRUNode] = {}

  def get_lru_node(self) -> LRUNode:
    return self._tail

  def update(self, obj, timestamp):
    if obj not in self._index:
      self._insert(obj, timestamp)
    else:
      node = self._index[obj]
      node.timestamp = timestamp
      left, right = node.left, node.right
      if left is not None:
        left.right = right
      if right is not None:
        right.left = left
      node.left, node.right = None, self._head
      self._head.left = node
      self._head = node
      if left.right is None:
        self._tail = left

  def _insert(self, obj, timestamp):
    node = LRUNode(obj, timestamp, left=None, right=self._head)
    if self._head is not None:
      self._head.left = node
    self._head = node
