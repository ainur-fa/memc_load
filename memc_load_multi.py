#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
from optparse import OptionParser
from multiprocessing import Process, Queue, Array
from itertools import islice
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
READ_LINES = 100000
PROCESS_VALUE = 4
QUEUE_MAXSIZE = 4
MEMC_SOCKET_TIMEOUT = 5
MEMC_DEAD_RETRY = 2
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def serialize_data(appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    return ua, key, packed


def parse_appsinstalled(logger, line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logger.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logger.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


def get_stat(logger, log_archives, processed_counter, errors_counter, files_index):
    for log_archive in log_archives:
        index = files_index[log_archive]
        if processed_counter[index]:
            err_rate = float(errors_counter[index]) / processed_counter[index]
            logger.info(f'File {log_archive} statistics:')
            if err_rate < NORMAL_ERR_RATE:
                logger.info(f"Acceptable error rate ({err_rate}). Successfull load")
            else:
                logger.error(f"High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")


def handle_data(log_settings, data_queue, options, processed_counter, errors_counter, files_index):
    log = log_settings['log']
    logger = logging.getLogger(__name__)
    logger.setLevel(log_settings['level'])
    handler = logging.FileHandler(log) if log else logging.StreamHandler()
    handler.setFormatter(log_settings['formatter'])
    logger.addHandler(handler)

    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    counter = {}

    while True:
        buffer = {}
        file, data = data_queue.get()
        if not data:
            logger.debug("Done")
            break
        if not counter.get(file):
            counter[file] = {'processed': 0, 'errors': 0}

        for line in data:
            appsinstalled = parse_appsinstalled(logger, line)
            if not appsinstalled:
                counter[file]['errors'] += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                counter[file]['errors'] += 1
                logger.error("Unknow device type: %s" % appsinstalled.dev_type)
                continue

            ua, key, packed = serialize_data(appsinstalled)

            try:
                if options.dry:
                    logger.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
                    counter[file]['processed'] += 1
                else:
                    if not buffer.get(memc_addr):
                        buffer[memc_addr] = {}
                    buffer[memc_addr].update({key: packed})
            except Exception as e:
                logger.exception("Cannot write to memc %s: %s" % (memc_addr, e))
                counter[file]['errors'] += 1

        for addr, data in buffer.items():
            memc = memcache.Client(servers=[addr], socket_timeout=MEMC_SOCKET_TIMEOUT, dead_retry=MEMC_DEAD_RETRY)
            notstored = len(memc.set_multi(data))
            counter[file]['errors'] += notstored
            counter[file]['processed'] += len(data) - notstored

    with processed_counter.get_lock():
        for key, value in counter.items():
            index = files_index[key]
            processed_counter[index] += counter[key]['processed']
    with errors_counter.get_lock():
        for key, value in counter.items():
            index = files_index[key]
            errors_counter[index] += counter[key]['errors']


def main(options, logger):
    data_queue = Queue(QUEUE_MAXSIZE)
    log_archives = list(glob.iglob(options.pattern))
    files_count = len(log_archives)
    processed_counter = Array('i', files_count)
    errors_counter = Array('i', files_count)
    files_index = {file: _id for _id, file in enumerate(log_archives)}

    workers = []
    for _ in range(PROCESS_VALUE):
        worker = Process(target=handle_data, args=(log_settings, data_queue, options,
                                                   processed_counter, errors_counter, files_index))
        worker.start()
        workers.append(worker)

    for fn in log_archives:
        logger.info('Processing %s' % fn)
        fd = gzip.open(fn, 'rt')
        data = list(islice(fd, READ_LINES))
        while data:
            data_queue.put((fn, data))
            data = list(islice(fd, READ_LINES))
        fd.close()
        dot_rename(fn)

    [data_queue.put((None, None)) for _ in range(PROCESS_VALUE)]
    [worker.join() for worker in workers]

    if log_archives:
        get_stat(logger, log_archives, processed_counter, errors_counter, files_index)


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()

    logger = logging.getLogger()
    log_level = logging.INFO if not opts.dry else logging.DEBUG
    logger.setLevel(log_level)
    handler = logging.FileHandler(opts.log) if opts.log else logging.StreamHandler()
    formatter = logging.Formatter(fmt='[%(processName)s] [%(asctime)s] %(levelname).1s %(message)s',
                                  datefmt='%Y.%m.%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    log_settings = {'level': log_level, 'formatter': formatter, 'log': opts.log}

    if opts.test:
        prototest()
        sys.exit(0)

    logger.info("Memc loader started with options: %s" % opts)
    try:
        main(opts, logger)
    except Exception as e:
        logger.exception("Unexpected error: %s" % e)
        sys.exit(1)
