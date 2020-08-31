import configparser
import json
import socket
from datetime import datetime
from urllib.parse import urlparse


def run(poolname, url, user, password):
    parsed = urlparse(url)
    timestamp = datetime.now().isoformat(timespec='minutes')
    with open(f'{poolname}-{timestamp}.json', 'wb') as file:  
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.connect((parsed.hostname, parsed.port))
            auth = {"params": [user, password], "id": 0, "method": "mining.authorize"}
            client.send(f'{json.dumps(auth)}\n'.encode())
            data = client.recv(1024)

            subscribe = {"id": 1, "method": "mining.subscribe", "params": []}
            client.send(f'{json.dumps(subscribe)}\n'.encode())

            while True:
                data = client.recv(1024*1024)
                # for msg in json.loads(data.decode()).split('\n'):
                #     msg["r"] = datetime.now().isoformat()
                file.write(data)
                file.flush()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("pools.ini")

    for pool in config.sections():
        run(pool, config[pool]["url"], config[pool]["user"], config[pool]["password"])
