# coding: utf-8
from __future__ import print_function
from __future__ import division
import os
import os.path
import re
from argparse import ArgumentParser
from datetime import datetime, timedelta
import json
import itertools
from multiprocessing import Pool
from glob import glob
import numpy as np


def outpath(filename, dest):
    basename = os.path.basename(filename)[:-4]
    return '{}/{}.sql'.format(dest, basename)


def month_hours(year, month):
    d = datetime(year, month, 1, 0, 0)
    while d.month == month:
        yield d
        d += timedelta(hours=1)


def process(args):
    filename, shapes, dest, ids = args
    cells = ids['cells']
    simulations = ids['simulations']
    datetimes = ids['datetimes']
    basename = os.path.basename(filename)[:-4]
    print(basename)

    pattern = r'surf_(.+)_(m[0-9]{3})_([0-9]{4})([0-9]{2}).grib'
    model, ensemble, year, month = re.match(pattern, basename).groups()
    year = int(year)
    month = int(month)

    data = np.load(filename)
    ts = list(month_hours(year, month))

    rows = []
    for shape in shapes:
        indices = shape['point_indices']
        if not indices['lat']:
            continue
        values = data[:, indices['lat'], indices['lon']]
        count = len(values[0])
        result_min = np.min(values, axis=1)
        result_max = np.max(values, axis=1)
        result_sum = np.sum(values, axis=1)
        result_sumsq = np.sum(values[:] ** 2, axis=1)
        rows.extend([(
            cells[shape['code']],
            simulations['d4PDF_RCM/{}/{}'.format(model, ensemble)],
            datetimes[str(t)],
            count,
            result_min[i],
            result_max[i],
            result_sum[i],
            result_sumsq[i],
        ) for i, t in enumerate(ts)])
    f = open('{}/{}.sql'.format(dest, basename), 'w')
    f.write('''
    INSERT INTO sd_rain
        (cellid, simulationid, datetimeid, cntx, minx, maxx, sumx, sumx2)
    VALUES {};'''.format(
        ','.join('({})'.format(','.join(str(v) for v in row)) for row in rows)
    ))


def main():
    parser = ArgumentParser()
    parser.add_argument('--dest', default='result',
                        help='destination directory')
    parser.add_argument('--threads', default=1, type=int,
                        help='number of parallel threads')
    parser.add_argument('--target', nargs=1,
                        help='target geometry file')
    parser.add_argument('--ids', nargs=1,
                        help='ids file')
    parser.add_argument('--force', action='store_true',
                        help='ignore existing files')
    parser.add_argument('files', nargs='+',
                        help='.npy files')
    args = parser.parse_args()

    ids = json.load(open(args.ids[0]))
    if not os.path.exists(args.dest):
        os.makedirs(args.dest)

    shapes = [s for s in json.load(open(args.target[0]))]
    filenames = [(f, shapes, args.dest, ids) for f
                 in itertools.chain(*[glob(p) for p in args.files])
                 if args.force or not os.path.exists(outpath(f, args.dest))]

    pool = Pool(args.threads)
    pool.map(process, filenames)


if __name__ == '__main__':
    main()
