import asyncio
import configparser
import json
from datetime import datetime
from urllib.parse import urlparse


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
    file = open(f'./data/{poolname}-{timestamp}.json', 'wt')

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
        if not line:
            break

    file.close()


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
    
    for task in tasks:
        await task


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("pools.ini")
    asyncio.run(run_with(config))
