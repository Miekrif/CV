import json
import logging
import random
import time
from threading import Thread
from Postgres import start_connection


logging.basicConfig(
    level="INFO",
    format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
)
logger = logging.getLogger(__name__)


def new_format_json(msg):
    try:
        while True:
            for level in range(50):
                (
                    msg[f"bid_{str(level).zfill(2)}"],
                    msg[f"ask_{str(level).zfill(2)}"],
                ) = (
                    random.randrange(1, 100),
                    random.randrange(100, 200),
                )
            msg["stats"] = {
                "sum_bid": sum(v for k, v in msg.items() if "bid" in k),
                "sum_ask": sum(v for k, v in msg.items() if "ask" in k),
            }
            # logger.info(f"{json.dumps(msg)}")

            if msg.get('ask_01', 0) + msg.get('bid_01', 0) < 105:
                send_to_postgres(msg, json.dumps(msg))
    except Exception as e:
        print(e)


def send_to_postgres(msg, jd):
    try:
        print((jd))
        start_connection(msg)
        # thread = Thread(target=start_connection, args=[msg])
        # thread.start()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    msg = dict()
    new_format_json(msg)
    print(msg)