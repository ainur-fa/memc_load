#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
from logging.handlers import QueueListener, QueueHandler
import collections
from optparse import OptionParser
from multiprocessing import Process, Queue, Manager, Barrier
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
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(logger, memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logger.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception as e:
        logger.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


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


def get_stat(logger, counter, file):
    if counter[file]['processed']:
        err_rate = float(counter[file]['errors']) / counter[file]['processed']
        if err_rate < NORMAL_ERR_RATE:
            logger.info("Acceptable error rate (%s). Successfull load" % err_rate)
        else:
            logger.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
    dot_rename(file)


def update_stat(counter, file, processed, errors):
    part_result = counter[file]
    part_result['errors'] += errors
    part_result['processed'] += processed
    counter[file] = part_result


def handle_data(log_settings, data_queue, options, counter, synchronization):
    logger = logging.getLogger(__name__)
    logger.addHandler(QueueHandler(log_settings['queue']))
    logger.setLevel(log_settings['level'])

    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    while True:
        file, data = data_queue.get()
        if not data:
            logger.debug("DONE")
            return
        if data == 'END':
            synchronization.wait()  # чтоб get_stat выполнялось после завершения обработки всех кусков файла
            continue
        if data == 'CALC':
            get_stat(logger, counter, file)
            continue

        processed = errors = 0
        for line in data:
            appsinstalled = parse_appsinstalled(logger, line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logger.error("Unknow device type: %s" % appsinstalled.dev_type)
                continue
            ok = insert_appsinstalled(logger, memc_addr, appsinstalled, options.dry)
            if ok:
                processed += 1
            else:
                errors += 1

        update_stat(counter, file, processed, errors)


def main(options):
    data_queue = Queue(QUEUE_MAXSIZE)
    log_archives = list(glob.iglob(options.pattern))
    counter = Manager().dict({i: {'processed': 0, 'errors': 0} for i in log_archives})
    synchronization = Barrier(PROCESS_VALUE)

    workers = []
    for _ in range(PROCESS_VALUE):
        worker = Process(target=handle_data, args=(log_settings, data_queue, options, counter, synchronization))
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
        [data_queue.put((fn, 'END')) for _ in range(PROCESS_VALUE)]  # синхронизация при завершении файла
        data_queue.put((fn, 'CALC'))  # для подсчета статистики
    [data_queue.put((None, None)) for _ in range(PROCESS_VALUE)]  # признак для завершения работы процесса

    [worker.join() for worker in workers]


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
    logger_queue = Queue(-1)
    queue_listener = QueueListener(logger_queue, handler)
    queue_listener.start()
    log_settings = {'queue': logger_queue,
                    'level': log_level}

    if opts.test:
        prototest()
        sys.exit(0)

    logger.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logger.exception("Unexpected error: %s" % e)
        sys.exit(1)
    finally:
        queue_listener.stop()
