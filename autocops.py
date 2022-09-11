"""AutoCops is a module to automatically copy file all over the place."""
from doctest import UnexpectedException
import os
import sys
import logging
import asyncio
import json

from threading import Event, Lock
from dataclasses import dataclass
from argparse import ArgumentParser
from hashlib import sha512

from fabric import Connection
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent,\
                            FileMovedEvent, FileDeletedEvent,\
                            DirCreatedEvent, DirDeletedEvent, DirMovedEvent


LOG_FORMAT = '%(asctime)-15s [%(funcName)s] %(message)s'
ACTION_HANDLERS = {}
OBSERVERS = []
#MAIN_LOOP = asyncio.new_event_loop()
MAIN_LOOP = asyncio.get_event_loop()

Q_LOCK = Lock()
Q_SIG = Event()
EVENTS = []
CONNS = {}


@dataclass
class Destination:
    """Simple destination dataclass."""
    # conn: Connection
    host: str
    path: str
    sep: str = '/'


class RemoteActionHandler():
    """Remote action handler"""

    def __init__(self, host) -> None:
        self.events = []
        self.conn = Connection(host)
        self.lock = asyncio.Lock()
        self.sig = asyncio.Event()
        self.sig.clear()

    async def enqueue(self, action):
        """Enqueue a remote action."""
        async with self.lock:
            self.events.append(action)
        logging.info(f'Setting {self.sig}')
        self.sig.set()
        logging.info(f'Set {self.sig}')

    async def handle_actions(self):
        """Process all actions in an endless loop"""
        while True:
            logging.info(f'Waiting %s...', self.sig)
            x = await self.sig.wait()
            logging.info('Done waiting')
            # async with self.lock:
            event = self.events.pop(0)
            if len(self.events) == 0:
                self.sig.clear()

            if isinstance(event, str):
                logging.info('%s "%s"', self.conn.host, event)
                try:
                    self.conn.run(event)
                except UnexpectedException as err:
                    logging.error('Unable to execute remote command: %s', err)
                except Exception as err:
                    logging.error('Unable to execute reomte command: %s', err)
            elif isinstance(event, tuple):
                src_path, remote = event
                r_host = self.conn.host
                l_hash = sha512(open(src_path, 'rb').read()).hexdigest()
                hash_cmd = f'if [ -e {remote} ]; then sha512sum {remote}; fi'
                hash_out = self.conn.run(hash_cmd).stdout
                r_hash = hash_out.split()[0] if len(hash_out) > 0 else ''
                if l_hash.lower() == r_hash.lower():
                    logging.info('identical hash for %s:%s (%s)', r_host,
                                 remote, l_hash)
                else:
                    logging.info('%s => %s:%s', src_path, r_host, remote)
                    self.conn.put(src_path, remote)


class AutoCopsHandler(FileSystemEventHandler):
    """AutoCops Event Handler"""

    def __init__(self, source, destinations) -> None:
        self.source = source
        self.dests = destinations
        super().__init__()

    def on_moved(self, event):
        # loop = asyncio.get_running_loop()
        # MAIN_LOOP.create_task(process_event_two(event, self.source, self.dests))
        # asyncio.run_coroutine_threadsafe(process_event_two(event, self.source, self.dests), MAIN_LOOP)
        # asyncio.run(process_event_two(event, self.source, self.dests))
        return super().on_moved(event)

    def on_any_event(self, event):
        coro = process_event_two(event, self.source, self.dests)
        fut = asyncio.run_coroutine_threadsafe(coro, MAIN_LOOP)
        logging.info('Running Coroutine')
        x = fut.result()
        logging.info('Future result is %s', x)
        # loop = asyncio.get_running_loop()
        # loop = asyncio.get_event_loop()
        # MAIN_LOOP.create_task(process_event_two(event, self.source, self.dests))
        # asyncio.run_coroutine_threadsafe(process_event_two(event, self.source, self.dests), MAIN_LOOP)
        # asyncio.run(process_event_two(event, self.source, self.dests))
        return super().on_any_event(event)


def load_config(config_filename):
    """
    Load configuration
    @param config_filename the configurati on filename
    """
    try:
        with open(config_filename, 'rb') as file:
            result = json.load(file)
    except FileNotFoundError as err:
        logging.error('Unable to open config file: "%s"', err)
        sys.exit(1)
    except json.JSONDecodeError as err:
        logging.error('Unable to decode JSON configuration: "%s"', err)
        sys.exit(1)

    for item in result:
        if 'source' not in item:
            logging.error('No "source" in config "%s": %s',
                          config_filename, item)
            sys.exit(1)

        if 'dest' not in item:
            logging.error('No "dest" in config "%s": %s',
                          config_filename, item)
            sys.exit(1)

    return result


def get_destinations(config):
    """
    Create connections for each destination.
    """
    results = []
    for item in config['dest']:
        host = item['host']
        path = item['path']
        dest = Destination(host=host, path=path)
        dest.sep = item['sep'] if 'sep' in item else dest.sep
        results.append(dest)
        if host not in ACTION_HANDLERS:
            ACTION_HANDLERS[host] = RemoteActionHandler(host)

    return results


async def enqueue_remote_action(action, dest):
    """
    Enqueue a remote action
    """
    await ACTION_HANDLERS[dest.host].enqueue(action)


async def process_event_two(event, source, destinations):
    """Enqueue remote things."""
    logging.info('IN PROCESS EVENT TOO')
    rel_path = event.src_path.partition(source)[2]
    remotes = {dest.sep.join([dest.path,
                              rel_path.replace(os.path.sep, dest.sep)]):
               dest for dest in destinations}

    for r_path, dest in remotes.items():
        cmd = None
        src_path = None
        # working form the theory that created and closed are irelevant
        # this may cause problems for created (ingored) then moved files
        if isinstance(event, DirCreatedEvent):
            cmd = f'mkdir -p {r_path}'
        elif isinstance(event, DirDeletedEvent):
            cmd = f'rmdir {r_path}'
        elif isinstance(event, DirMovedEvent):
            src_path = r_path
            dest_path = event.dest_path.partition(source)[2]
            dest_path = dest_path.replace(os.path.sep, dest.sep)
            dest_path = dest.sep.join([dest.path, dest_path])

            # for whatever reason, dir moves on windows include a sub path so
            # we should trim of the end. For example if we have ./a/b/c (c is a
            # file) and we rename a to z, the envet gives a source and
            # destination of ./a/b/ and ./a/b -- which is dumb, but whatever
            while src_path[-1] == dest_path[-1]:
                src_path = src_path[:-1]
                dest_path = dest_path[:-1]

            cmd = f'mv {src_path} {dest_path}'
        elif isinstance(event, FileModifiedEvent):
            src_path = event.src_path
        elif isinstance(event, FileDeletedEvent):
            cmd = f'rm {r_path}'
        elif isinstance(event, FileMovedEvent):
            src_path = r_path
            dest_path = event.dest_path.partition(source)[2]
            dest_path = dest_path.replace(os.path.sep, dest.sep)
            dest_path = dest.sep.join([dest.path, dest_path])
            cmd = f'mv {src_path} {dest_path}'

        if cmd:
            await enqueue_remote_action(cmd, dest)
        elif src_path:
            await enqueue_remote_action((src_path, r_path), dest)
        else:
            logging.error('Unhandled event "%s"', event)


def process_event(source, dests, ignored, event):
    """Process events."""
    rel_path = event.src_path.partition(source)[2]

    if any([ignore in rel_path for ignore in ignored]):
        logging.debug('Ignoring %s', rel_path)
        return

    # TODO here we also need to track the destination host so we can
    # enqueue with connections properly
    remotes = {dest.sep.join([dest.path,
                              rel_path.replace(os.path.sep, dest.sep)]):
               dest for dest in dests}

    for remote, dest in remotes.items():
        cmd = None
        src_path = None
        # working form the theory that created and closed are irelevant
        # this may cause problems for created (ingored) then moved files
        if isinstance(event, DirCreatedEvent):
            cmd = f'mkdir -p {remote}'
        elif isinstance(event, DirDeletedEvent):
            cmd = f'rmdir {remote}'
        elif isinstance(event, DirMovedEvent):
            src_path = remote
            dest_path = event.dest_path.partition(source)[2]
            dest_path = dest_path.replace(os.path.sep, dest.sep)
            dest_path = dest.sep.join([dest.path, dest_path])

            # for whatever reason, dir moves on windows include a sub path so
            # we should trim of the end. For example if we have ./a/b/c (c is a
            # file) and we rename a to z, the envet gives a source and
            # destination of ./a/b/ and ./a/b -- which is dumb, but whatever
            while src_path[-1] == dest_path[-1]:
                src_path = src_path[:-1]
                dest_path = dest_path[:-1]

            cmd = f'mv {src_path} {dest_path}'
        elif isinstance(event, FileModifiedEvent):
            src_path = event.src_path
        elif isinstance(event, FileDeletedEvent):
            cmd = f'rm {remote}'
        elif isinstance(event, FileMovedEvent):
            src_path = remote
            dest_path = event.dest_path.partition(source)[2]
            dest_path = dest_path.replace(os.path.sep, dest.sep)
            dest_path = dest.sep.join([dest.path, dest_path])
            cmd = f'mv {src_path} {dest_path}'

        if cmd:
            logging.info('%s "%s"', dest.conn.host, cmd)
            try:
                dest.conn.run(cmd)
            except UnexpectedException as err:
                logging.error('Unable to execute remote command: %s', err)
            except Exception as err:
                logging.error('Unable to execute reomte command: %s', err)
        elif src_path:
            r_host = dest.conn.host
            l_hash = sha512(open(src_path, 'rb').read()).hexdigest()
            hash_cmd = f'if [ -e {remote} ]; then sha512sum {remote}; fi'
            hash_out = dest.conn.run(hash_cmd).stdout
            r_hash = hash_out.split()[0] if len(hash_out) > 0 else ''
            if l_hash.lower() == r_hash.lower():
                logging.info('identical hash for %s:%s (%s)', r_host, remote,
                             l_hash)
            else:
                logging.info('%s => %s:%s', src_path, r_host, remote)
                dest.conn.put(src_path, remote)
        else:
            logging.error('Unhandled event "%s"', event)


async def full_sync(source, destinations, ignore):
    """
    Fully sync source with the destinations. It might be worthwhile adding a
    third lib (rsync) so speed this up.

    THIS METHOD REQUIRES EXCLUSIVE QUEUE ACCESS AND LOCKING SHOULD BE PERFORMED
    OUTSIDE. The reason being is that we want to hold off genuine file system
    events until we have ensured we create a full folder and file structure.
    """
    logging.info('Discovering folder structure...')
    for root, dirs, files in os.walk(source):

        # skip contents of this folder if ignored in path
        if any([ignored in root for ignored in ignore]):
            continue

        # ensure folders get created first
        for evt in [DirCreatedEvent(os.path.join(root, dir))
                    for dir in dirs if dir not in ignore]:
            await process_event_two(evt, source, destinations)

        # then get files copied over
        for evt in [FileModifiedEvent(os.path.join(root, file))
                    for file in files]:
            await process_event_two(evt, source, destinations)

    logging.info('Discovery complete.')


async def process_action_handlers():
    """Run the async action handlers"""
    tasks = []
    for _, handler in ACTION_HANDLERS.items():
        tasks.append(asyncio.create_task(handler.handle_actions()))

    await asyncio.gather(*tasks)


async def __main__():
    """
    The main method. Parses arguments and sets up logging before we start
    copying things around.
    """
    parser = ArgumentParser()
    parser.add_argument('-c', '--config', action='store', dest='config',
                        help='Config file location', default="config.json")
    parser.add_argument('-d', '--debug', action='store_true', dest='debug',
                        help='debug logging')
    parser.add_argument('-l', '--logfile', action='store', dest='logfile',
                        help='log file location')
    parser.add_argument('-o', '--output', action='store', dest='output',
                        help='output file location')
    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(format=LOG_FORMAT, level=log_level,
                        filename=args.logfile if args.logfile else None)

    config = load_config(args.config)
    destinations = []

    # lock berfore creating the handler so we can prefill the queue with the
    # base folder structure AND THEN once traversal is complete unlock so the
    # changes get populated in the queue.
    Q_LOCK.acquire()

    for item in config:
        source_path = item['source']
        if source_path[-1] != os.sep:
            source_path = f'{source_path}{os.sep}'

        ignored = item['ignore'] if 'ignore' in item else []

        destinations.extend(get_destinations(item))

        hndlr = AutoCopsHandler(source_path, destinations)
        obsrv = Observer()
        obsrv.schedule(hndlr, source_path, True)
        obsrv.start()
        OBSERVERS.append(obsrv)
        await full_sync(source_path, destinations, ignored)

    # in the unlikely event there are no files or folders to sync after startup
    # clear the signal so we don't start processing
    # if len(EVENTS) > 0:
    #     Q_SIG.set()

    # Q_LOCK.release()

    # while obsrv.is_alive():
    #     Q_LOCK.acquire()
    #     event = EVENTS.pop(0)
    #     if len(EVENTS) == 0:
    #         Q_SIG.clear()
    #     Q_LOCK.release()
    #     process_event(source_path, destinations, ignored, event)

    if not all([o.is_alive() for o in OBSERVERS]):
        logging.error('At least one Watchdog observer was not alive')
        sys.exit(1)

    await process_action_handlers()

    for obsrv in OBSERVERS:
        obsrv.stop()
        obsrv.join()


if __name__ == '__main__':
    MAIN_LOOP.run_until_complete(__main__())
