"""AutoCops is a module to automatically copy file all over the place."""
from doctest import UnexpectedException
import os
import sys
import logging
import json
from threading import Event, Lock

from dataclasses import dataclass
from argparse import ArgumentParser

from fabric import Connection
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileModifiedEvent,\
                            FileMovedEvent, FileDeletedEvent,\
                            DirCreatedEvent, DirDeletedEvent


LOG_FORMAT = '%(asctime)-15s [%(funcName)s] %(message)s'
Q_LOCK = Lock()
Q_SIG = Event()
EVENTS = []


@dataclass
class Destination:
    """Simple destination dataclass."""
    conn: Connection
    path: str
    sep: str = '/'


class AutoCopsHandler(FileSystemEventHandler):
    """AutoCops Event Handler"""

    def on_moved(self, event):
        Q_LOCK.acquire()
        EVENTS.append(event)
        Q_LOCK.release()
        Q_SIG.set()

    def on_any_event(self, event):
        Q_LOCK.acquire()
        EVENTS.append(event)
        Q_LOCK.release()
        Q_SIG.set()

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

    if 'source' not in result:
        logging.error('No "source" in config "%s"', config_filename)
        sys.exit(1)

    if 'dest' not in result:
        logging.error('No "dest" in config "%s"', config_filename)
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
        con = Connection(host)
        dest = Destination(con, path)
        dest.sep = item['sep'] if 'sep' in item else dest.sep
        results.append(dest)
    return results


def process_event(source, dests, ignored, event):
    """Process events."""
    rel_path = event.src_path.partition(source)[2]
    logging.info(event)

    if any([ignore in rel_path for ignore in ignored]):
        logging.debug('Ignoring %s', rel_path)
        return

    remotes = {dest.sep.join([dest.path, rel_path]): dest for dest in dests}

    for remote, dest in remotes.items():
        cmd = None
        src_path = None
        # working form the theory that created and closed are irelevant
        # this may cause problems for created (ingored) then moved files
        if isinstance(event, DirCreatedEvent):
            cmd = f'mkdir -p {remote}'
        elif isinstance(event, DirDeletedEvent):
            cmd = f'rmdir {remote}'
        elif isinstance(event, FileModifiedEvent):
            src_path = event.src_path
        elif isinstance(event, FileDeletedEvent):
            cmd = f'rm {remote}'
        elif isinstance(event, FileMovedEvent):
            src_path = remote
            dest_path = dest.sep.join([dest.path,
                                       event.dest_path.partition(source)[2]])
            cmd = f'mv {src_path} {dest_path}'

        if cmd:
            logging.info('%s "%s"', dest.conn.host, cmd)
            try:
                dest.conn.run(cmd)
            except UnexpectedException as err:
                logging.error('Unable to execute remote command: %s', err)
        elif src_path:
            logging.info('%s => %s:%s', src_path, dest.conn.host, remote)
            dest.conn.put(src_path, remote)
        else:
            logging.error('Unhandled event "%s"', event)


def full_sync(source, destinations, ignore):
    """
    Fully sync source with the destinations. It might be worthwhile adding a
    third lib (rsync) so speed this up.

    TODO fabric rsync
    """
    logging.info('Syncing folder...')
    for root, dirs, files in os.walk(source):

        # skill contents of this folder if ignored in path
        if any([ignored in root for ignored in ignore]):
            continue

        # get the relative path
        rel_path = root.partition(source)[2]

        # ensure all folder exist on the remote
        for dir in dirs:

            # skip ignored directories
            if dir in ignore:
                continue

            for dest in destinations:
                full_dest = dest.sep.join([dest.path, rel_path, dir])
                cmd = f'mkdir -p {full_dest}'
                dest.conn.run(cmd)

        for file in files:
            full_source = os.path.join(root, file)

            for dest in destinations:
                full_dest = dest.sep.join([dest.path, rel_path, file])
                logging.info('%s => %s', full_source, full_dest)
                dest.conn.put(full_source, full_dest)
    logging.info('All folders synced')


def watch_folder(source, destinations, ignored):
    """Watch a folder for changes."""
    obsrv = Observer()
    hndlr = AutoCopsHandler()

    obsrv.schedule(hndlr, source, True)
    obsrv.start()

    while obsrv.is_alive() and Q_SIG.wait():
        Q_LOCK.acquire()
        event = EVENTS.pop()
        Q_SIG.clear()
        Q_LOCK.release()
        process_event(source, destinations, ignored, event)

    obsrv.stop()
    obsrv.join()


def __main__():
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

    source_path = config['source']
    if source_path[-1] != os.sep:
        source_path = f'{source_path}{os.sep}'

    ignored = config['ignore'] if 'ignore' in config else []

    # ensure we end up waiting for a signal
    Q_SIG.clear()

    destinations = get_destinations(config)

    watch_folder(source_path, destinations, ignored)

    # full_sync(source_path, destinations, ignored)


if __name__ == '__main__':
    __main__()
