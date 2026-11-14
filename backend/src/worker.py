import logging
import time
from pathlib import Path
import redis
from src.config.settings import REDIS_URL
from src.services.run import QUEUE_KEY, execute, requeue_stale
from src.utils.repository import init_db

logging.basicConfig(level=logging.INFO)


def main():
    init_db()
    client = redis.Redis.from_url(REDIS_URL)
    # 启动接管：worker 曾在出队后被强杀的运行会永久卡在 running，
    # 这里重新投回队列，由 claim 的 fencing 保证不会与迟到的旧执行冲突。
    while True:
        try:
            recovered = requeue_stale()
            if recovered:
                logging.info("已重新排队 %s 个中断运行", recovered)
            break
        except Exception:
            logging.exception("中断恢复扫描失败，稍后重试")
            time.sleep(2)
    while True:
        Path("/tmp/worker-heartbeat").write_text(str(time.time()))
        try:
            item = client.blpop(QUEUE_KEY, timeout=3)
            if item:
                execute(item[1].decode())
            else:
                # 空闲周期也扫描一次，覆盖本 worker 运行期间其他实例中断的情况。
                requeue_stale()
        except Exception:
            logging.exception("worker failure")
            time.sleep(2)


if __name__ == "__main__":
    main()
