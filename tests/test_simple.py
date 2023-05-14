from spr import Client
import time


def a():
  print("a")
  time.sleep(1)


def b():
  print("b")


if __name__ == "__main__":
  executor = Client()
  aid = executor.submit(a)
  bid = executor.submit(b, dependency={aid})
  executor.wait_all({aid, bid})
  print("finished")
  executor.close()
