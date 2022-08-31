"""AutoCops is a module to automatically copy file all over the place."""
import os
import sys
import logging
import json

from dataclasses import dataclass
from argparse import ArgumentParser

from fabric import Connection
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent,\
                            FileModifiedEvent, FileClosedEvent, FileMovedEvent,\
                            FileDeletedEvent

LOG_FORMAT = '%(asctime)-15s [%(funcName)s] %(message)s'


@dataclass
class Destination:
    conn: Connection
    path: str

class AutoCopsHandler(FileSystemEventHandler):
    """AutoCops Event Handler"""

    def on_any_event(self, event):
        logging.info(event)

        if isinstance(event, FileCreatedEvent):
            pass
        elif isinstance(event, FileModifiedEvent):
            pass
        elif isinstance(event, FileClosedEvent):
            pass
        elif isinstance(event, FileMovedEvent):
            pass
        elif isinstance(event, FileDeletedEvent):
            pass

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
        results.append(dest)
    return results


def full_sync(source, destinations):
    """
    Fully sync source with the destinations. It might be worthwhile adding a
    third lib (rsync) so speed this up.

    TODO fabric rsync
    """
    logging.info('Syncing folder...')
    for root, dirs, files in os.walk(source):

        # get the relative path
        rel_path = root.partition(source)[2]

        # ensure all folder exist on the remote
        for dir in dirs:
            for destination in destinations:
                full_dest = os.path.join(destination.path, rel_path, dir)
                cmd = f'mkdir -p {full_dest}'
                destination.conn.run(cmd)

        for file in files:
            full_source = os.path.join(root, file)

            for destination in destinations:
                full_dest = os.path.join(destination.path, rel_path, file)
                logging.info(f'{full_source} => {full_dest}')
                destination.conn.put(full_source, full_dest)
    logging.info('All folders synced')


def watch_folder():
    """Watch a folder for changes."""
    obsrv = Observer()
    hndlr = AutoCopsHandler()

    obsrv.schedule(hndlr, '/home/micah/dev/autocops', True)

    obsrv.start()

    try:
        while obsrv.is_alive():
            obsrv.join()
    finally:
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

    destinations = get_destinations(config)
    full_sync(source_path, destinations)


if __name__ == '__main__':
    __main__()
