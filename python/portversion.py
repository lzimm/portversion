import argparse
import asyncio
import aioredis
import signal
import time
import logging
import sys

file_handler = logging.FileHandler(filename='portversion.log')
stdout_handler = logging.StreamHandler(sys.stdout)
logging.basicConfig(level=logging.INFO, 
                    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
                    handlers=[file_handler, stdout_handler])

portversions = {}
port = None

async def reaper():
    global portversions
    global port

    while True:
        ts = time.time()
        portverions = {p: (v, t) for (p, (v, t)) in portversions.items() if ts - t < 30}
        p = next((p for (p, (v, t)) in sorted(portversions.items(), key=lambda p: p[1], reverse=True)), None)
        if p != port:
            logging.info("Updating port: %s" % p)
            port = p
        await asyncio.sleep(10)

async def subscriber(channel):
    logging.info("Setting up channel: %s" % channel)

    redis = aioredis.Redis.from_url("redis://localhost")
    psub = redis.pubsub()

    async def reader(ch):
        global portversions

        while True:
            message = await ch.get_message(ignore_subscribe_messages=True)
            if message:
                bs = message.get('data')
                if len(bs) == 32:
                    port = int(bs[0:16], 16)
                    version = int(bs[16:], 16)
                    if port not in portversions:
                        logging.info("Setting port: %s version: %s" % (port, version))
                    portversions[port] = (version, time.time())
            await asyncio.sleep(1)

    async with psub as p:
        logging.info("Subscribing to channel: %s" % channel)

        await p.subscribe(channel)
        await reader(p)
        await p.unsubscribe(channel)

    await psub.close()

async def pipe(reader, writer):
    try:
        while not reader.at_eof():
            writer.write(await reader.read(2048))
    finally:
        writer.close()

async def handler(local_reader, local_writer):
    try:
        global port

        if not port:
            local_writer.close()
            return

        remote_reader, remote_writer = await asyncio.open_connection('127.0.0.1', port)
        pipe1 = pipe(local_reader, remote_writer)
        pipe2 = pipe(remote_reader, local_writer)
        await asyncio.gather(pipe1, pipe2)
    finally:
        local_writer.close()

def server(port, channel):
    logging.info("Starting server on: %s" % port)

    loop = asyncio.get_event_loop()
    subscriber_task = loop.create_task(subscriber(channel))
    reaper_task = loop.create_task(reaper())

    handler_server = asyncio.start_server(handler, '0.0.0.0', port)
    server = loop.run_until_complete(handler_server)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    for task in asyncio.all_tasks():
        task.cancel()

    server.close()
    loop.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser('portversion')
    parser.add_argument('-P', '--port', required=False, type=int, default=8024)
    parser.add_argument('-C', '--channel', required=False, type=str, default='portversion')
    args = parser.parse_args()
    server(args.port, args.channel)
