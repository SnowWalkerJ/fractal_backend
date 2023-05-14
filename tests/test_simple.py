from spr import Client
import time


def a():
  print("a")
  time.sleep(1)


def b():
  print("b")


if __name__ == "__main__":
  client = Client()
  aid = client.submit(a)
  bid = client.submit(b, dependency={aid})
  client.wait_all({aid, bid})
