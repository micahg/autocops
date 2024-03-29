"""AutoCops is a module to automatically copy file all over the place."""
from doctest import UnexpectedException
import os
import sys
import logging
import asyncio
import json

from dataclasses import dataclass
from argparse import ArgumentParser
from hashlib import sha512

from fabric import Connection
from invoke import UnexpectedExit
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent,\
                            FileMovedEvent, FileDeletedEvent,\
                            DirCreatedEvent, DirDeletedEvent, DirMovedEvent


LOG_FORMAT = '%(asctime)-15s [%(funcName)s] %(message)s'
ACTION_HANDLERS = {}
OBSERVERS = []
MAIN_LOOP = asyncio.get_event_loop()


@dataclass
class Destination:
    """Simple destination dataclass."""
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

    def run_sync(self, cmd):
        """
        Synchronously run a command. Careful not to use this while looping over
        asynchronous actions. ONLY DO IT BEFORE THE EVENT LOOP KICKS OFF
        """
        try:
            res = self.conn.run(cmd).stdout
        except Exception as err:
            logging.error('Error running on %s: %s', self.conn.host, err)
            raise err
        return res

    async def enqueue(self, action):
        """Enqueue a remote action."""
        async with self.lock:
            self.events.append(action)
            self.sig.set()

    async def handle_actions(self):
        """Process all actions in an endless loop"""
        while await self.sig.wait():
            async with self.lock:
                event = self.events.pop(0)
                if len(self.events) == 0:
                    self.sig.clear()

            if isinstance(event, str):
                logging.info('%s "%s"', self.conn.host, event)
                try:
                    self.conn.run(event)
                except UnexpectedException as err:
                    logging.error('Unable to execute remote command: %s', err)
                except UnexpectedExit as err:
                    logging.error('Unable to execute command: %s', err)
            elif isinstance(event, tuple):
                src_path, remote, r_hash = event
                r_host = self.conn.host
                try:
                    l_hash = sha512(open(src_path, 'rb').read()).hexdigest()
                except FileNotFoundError:
                    logging.error('Not hashing disappeared file: %s', src_path)
                    continue
                except PermissionError:
                    logging.error('Not hashing inaccessible file %s', src_path)
                    continue
                if not r_hash:
                    hash_cmd = f'if [ -e {remote} ]; then sha512sum {remote}; fi'
                    hash_out = self.conn.run(hash_cmd).stdout
                    r_hash = hash_out.split()[0] if len(hash_out) > 0 else ''
                else:
                    pass
                if l_hash.lower() == r_hash.lower():
                    logging.info('identical hash for %s:%s', r_host,
                                 remote)
                    logging.debug(l_hash)
                    continue
                logging.info('%s => %s:%s', src_path, r_host, remote)
                try:
                    self.conn.put(src_path, remote)
                except FileNotFoundError:
                    logging.error('Not copying disappeared file: %s', src_path)
                    continue


class AutoCopsHandler(FileSystemEventHandler):
    """AutoCops Event Handler"""

    def __init__(self, source, destinations, ignored) -> None:
        self.source = source
        self.dests = destinations
        self.ignored = ignored
        super().__init__()

    def on_any_event(self, event):
        # skip contents of this folder if ignored in path
        if any([ignored in event.src_path for ignored in self.ignored]):
            return super().on_any_event(event)

        coro = process_event(event, self.source, self.dests)
        fut = asyncio.run_coroutine_threadsafe(coro, MAIN_LOOP)
        try:
            fut.result()
        except TimeoutError as err:
            logging.error('Coroutine failed on timeout: %s', err)
        except asyncio.CancelledError as err:
            logging.error('Coroutine failed on cancellation: %s', err)
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


async def process_event(event, source, destinations, dhash=None):
    """Enqueue remote things."""
    rel_path = event.src_path.partition(source)[2]

    for dest in destinations:
        r_path = rel_path.replace(os.path.sep, dest.sep)
        r_path = dest.sep.join([dest.path, r_path])
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
            pre_hash = None
            if dhash and dest.host in dhash and r_path in dhash[dest.host] and dhash[dest.host][r_path]:
                pre_hash = dhash[dest.host][r_path]
            await enqueue_remote_action((src_path, r_path, pre_hash), dest)
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
    dest_hash = {}
    for d in destinations:
        logging.info('destination is %s:%s', d.host, d.path)
        output = ACTION_HANDLERS[d.host].run_sync(f'find {d.path} -type f | xargs -d \'\\n\' sha512sum')
        logging.info('output is %s', output)
        pre_hash = {}
        for l in output.splitlines():
            (hsh, path) = l.split()[0:2]
            pre_hash[path] = hsh
    dest_hash[d.host] = pre_hash

    logging.info('Discovering folder structure...')
    for root, dirs, files in os.walk(source):

        # skip contents of this folder if ignored in path
        if any([ignored in root for ignored in ignore]):
            continue

        # ensure folders get created first
        for evt in [DirCreatedEvent(os.path.join(root, dir))
                    for dir in dirs if dir not in ignore]:
            await process_event(evt, source, destinations)

        # then get files copied over
        for evt in [FileModifiedEvent(os.path.join(root, file))
                    for file in files if file not in ignore]:
            await process_event(evt, source, destinations, dest_hash)

    logging.info('Discovery complete.')


async def process_action_handlers():
    """Run the async action handlers"""
    tasks = []
    for _, handler in ACTION_HANDLERS.items():
        tasks.append(asyncio.create_task(handler.handle_actions()))

    await asyncio.gather(*tasks)


async def keep_forward_open(handler: RemoteActionHandler, local, remote,
                            evt: asyncio.Event):
    """
    Open a tunnel and wait forever so it stays open.
    """
    with handler.conn.forward_local(local, remote):
        logging.info(f'Tunnling on {handler} ({local}:{remote})')
        while await evt.wait():
            return


def add_forward(fwd, evt: asyncio.Event):
    """
    Add forwarded ports. Hosts will be in action hanlders that can be used
    later.
    """
    if 'host' not in fwd:
        logging.error('No host in forward %s', fwd)
        return None
    if 'remote' not in fwd:
        logging.error('No remote in %s', fwd)
        return None
    if 'local' not in fwd:
        logging.error('No local in %s', fwd)
        return None

    host = fwd['host']
    local = fwd['local']
    remote = fwd['remote']
    handler = RemoteActionHandler(host)
    ACTION_HANDLERS[host] = handler
    return keep_forward_open(handler, local, remote, evt)


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
    parser.add_argument('-n', '--no-sync', action='store_true', dest='nosync',
                        help='Skip sync on startup')
    parser.add_argument('-p', '--config-path', action='store', dest='config_path', nargs='?',
                        help='Specify the config path (eg: specify a separate configuration section)')
    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(format=LOG_FORMAT, level=log_level,
                        filename=args.logfile if args.logfile else None)

    config = load_config(args.config)
    destinations = []
    logging.info('Config keys are: %s', ', '.join(config.keys()))

    fwd_evt = asyncio.Event()
    fwd_evt.clear()
    if 'forwards' in config:
        for fwd in config['forwards']:
            coro = add_forward(fwd, fwd_evt)
            logging.info(coro)
            asyncio.create_task(coro)
    
    if args.config_path is None:
        keys = ', '.join(config.keys())
        logging.info('Available config paths: %s', keys)
        logging.info('Please specify a config path with the -p parameter')
        sys.exit(0)

    config_path = args.config_path if args.config_path else 'paths'
    logging.info('Using config key "%s"', config_path)
    for item in config[config_path]:
        source_path = item['source']
        if source_path[-1] != os.sep:
            source_path = f'{source_path}{os.sep}'

        ignored = item['ignore'] if 'ignore' in item else []

        destinations.extend(get_destinations(item))

        hndlr = AutoCopsHandler(source_path, destinations, ignored)
        obsrv = Observer()
        obsrv.schedule(hndlr, source_path, True)
        obsrv.start()
        OBSERVERS.append(obsrv)
        if not args.nosync:
            await full_sync(source_path, destinations, ignored)

    if not all([o.is_alive() for o in OBSERVERS]):
        logging.error('At least one Watchdog observer was not alive')
        sys.exit(1)

    await process_action_handlers()

    for obsrv in OBSERVERS:
        obsrv.stop()
        obsrv.join()


if __name__ == '__main__':
    MAIN_LOOP.run_until_complete(__main__())
