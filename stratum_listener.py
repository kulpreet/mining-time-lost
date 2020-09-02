import asyncio
import configparser
from datetime import datetime
import json
import signal
import sys
import time
from urllib.parse import urlparse

RETRIES = 10


async def send_auth(reader, writer, user, password):
    auth = {"params": [user, password], "id": 2, "method": "mining.authorize"}
    print(f'send: {auth}')
    writer.write(f'{json.dumps(auth)}\n'.encode())

    line = await reader.readline()
    print(f'auth response: {line}')


def send_subscribe(writer):
    subscribe = {"id": 1, "method": "mining.subscribe", "params": []}
    writer.write(f'{json.dumps(subscribe)}\n'.encode())


async def pool_listener(poolname, url, user, password):
    parsed = urlparse(url)
    timestamp = datetime.now().isoformat(timespec='minutes')
    with open(f'./data/{poolname}-{timestamp}.json', 'wt') as file:
        for i in range(RETRIES):
            try:
                reader, writer = await asyncio.open_connection(
                    parsed.hostname, parsed.port)

                send_subscribe(writer)
                await send_auth(reader, writer, user, password)

                while True:
                    line = await reader.readline()
                    msg = json.loads(line.decode())
                    msg["r"] = datetime.now().isoformat()
                    file.write(json.dumps(msg)+'\n')
                    file.flush()
            except:
                if i < RETRIES:
                    time.sleep(5)
                    print(f'Reconnecting {poolname}')
                    continue
                else:
                    raise


async def run_with(config):
    tasks = []
    for pool in config.sections():
        if not config[pool].getboolean("enabled"):
            continue
        print(f'Starting {pool}')
        task = asyncio.create_task(
            pool_listener(pool, config[pool]["url"],
                          config[pool]["user"], config[pool]["password"]))
        tasks.append(task)

    def signal_handler(sig, frame):
        print('Stopping..')
        for task in tasks:
            task.cancel()
        print('Stopped.')
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    for task in tasks:
        await task


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("pools.ini")
    asyncio.run(run_with(config))

