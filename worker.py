import redis
from rq import Worker, Queue
from multiprocessing import set_start_method
from utils import REDIS_URL

set_start_method("spawn", force=True)

listen = ['default']
conn = redis.from_url(REDIS_URL)

if __name__ == "__main__":
    queues = [Queue(name, connection=conn) for name in listen]
    worker = Worker(queues, connection=conn)
    worker.work()
