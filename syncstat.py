"""
syncstat
~~~~~~~~

Synchronization script.

Examples:

* Synchronize files if they was modified in last two days

  python2 syncstat.py -s3 bucket/backup

* Synchronize all files

  python2 syncstat.py --ignore-mtime -s3 bucket/backup

* Pass dry run flag to the s3cmd

  python2 syncstat.py -s3 bucket/backup -- --dry-run

Throughput:

* Process 10 files per second

  python2 syncstat.py --throughput 10 -s3 bucket/backup

  # "--per second" option is intended but can be passed explicitly.

* Process 10 files per minute

  python2 syncstat.py --throughput 10 --per minute -s3 bucket/backup

* Process 10 files per hour

  python2 syncstat.py --throughput 10 --per hour -s3 bucket/backup

Batch processing:

* Synchronize first 500 of modified files

  python2 syncstat.py --limit 500 --marker-dir /tmp/ -s3 bucket/backup

* Synchronize first 500 of all files

  python2 syncstat.py --ignore-mtime --limit 500 --marker-dir /tmp/ -s3 bucket/backup
"""

import argparse
import commands
import datetime
import functools
import os
import sys
import time

MARKER_FILE = ".syncstat.lock"
OLD_MARKER_FILE = ".syncstat.lock.old"
CWD = os.getcwd()
SYNC_TIME = datetime.timedelta(days=2).total_seconds()
TEST_TIME = int(time.time() - SYNC_TIME)
THROUGHPUT_TABLE = {
    "second": 1,
    "minute": datetime.timedelta(minutes=1).total_seconds(),
    "hour": datetime.timedelta(hours=1).total_seconds(),
    "day": datetime.timedelta(days=1).total_seconds(),
}


def main(
    directory, do_mtime, s3, debug, jitter, limit, cmd_put_template, marker, stats, **kw
):

    jit_time = time.time()
    jit_counter = 0

    with open(marker, "w") as f:
        for root, dirs, files in os.walk(directory):
            for name in files:
                filename = os.path.join(root, name)
                stats["count_total"] += 1
                f.write(filename + "\n")

                if do_mtime:
                    try:
                        stat = os.stat(filename)
                    except OSError as e:
                        sys.stderr.write("{}\n".format(e))
                        stats["count_errors"] += 1
                        continue
                    if stat.st_mtime <= TEST_TIME:
                        continue

                    stats["count_modified"] += 1

                if limit and stats["count_uploaded"] + stats["count_errors"] >= limit:
                    continue

                print(filename)

                cmd = cmd_put_template.format(filename, s3(filename))

                debug(cmd)

                status, output = get_status_output(cmd)
                if status:
                    sys.stderr.write("{}\n".format(output))
                    stats["count_errors"] += 1
                else:
                    stats["count_uploaded"] += 1

                jit_counter += 1
                jit_counter, jit_time = jitter(jit_counter, jit_time)


def get_status_output(*args, **kwargs):
    try:
        # Python 2.6+
        import commands

        return commands.getstatusoutput(*args)
    except ImportError:
        # Python 3+
        import subprocess

        return subprocess.getstatusoutput(*args)

    ### https://stackoverflow.com/questions/11344557/replacement-for-getstatusoutput-in-python-3
    # p = subprocess.Popen(*args, **kwargs)
    # stdout, stderr = p.communicate()
    # return p.returncode, stdout, stderr


def prepare_markers(marker, old_marker, **kw):
    if os.path.exists(marker):
        os.rename(marker, old_marker)
    if not os.path.exists(old_marker):
        open(old_marker, "a").close()


def process_removed(
    marker, old_marker, cmd_del_template, debug, jitter, stats, s3, **kw
):
    sort_cmd = " ".join(["sort", "-o", marker, marker])
    debug(sort_cmd)
    status, _ = get_status_output(sort_cmd)
    assert not status, "sort marker file failed"

    diff_cmd = " ".join(
        [
            "diff",
            "--new-line-format=''",
            "--unchanged-line-format=''",
            old_marker,
            marker,
        ]
    )
    debug(diff_cmd)
    status, output = get_status_output(diff_cmd)
    removed_files = output.strip().split()
    jit_time = time.time()
    jit_counter = 0
    for removed_file in removed_files:
        print(removed_file)
        stats["count_total"] += 1
        cmd = cmd_del_template.format(s3(removed_file))
        debug(cmd)
        status, output = get_status_output(cmd)
        if status:
            sys.stderr.write("{}\n".format(output))
            stats["count_errors"] += 1
        else:
            stats["count_deleted"] += 1
        jit_counter += 1
        jit_counter, jit_time = jitter(jit_counter, jit_time)


def report(stats, **kw):
    print(
        "Total: {count_total}. "
        "Modified: {count_modified}. "
        "Uploaded: {count_uploaded}. "
        "Deleted: {count_deleted}. "
        "Errors: {count_errors}.".format(**stats)
    )


def cli():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-p", "--path", default=CWD, type=str, help="path to target directory"
    )

    parser.add_argument(
        "-s3", "--s3path", type=str, help="path to S3, format: BUCKET[/PREFIX]"
    )

    parser.add_argument(
        "-d", "--debug", default=False, action="store_true", help="print debug information",
    )

    parser.add_argument(
        "-i", "--ignore-mtime", default=False, action="store_true", help="process files regardless of modification time",
    )

    # Throughput.
    parser.add_argument(
        "-t", "--throughput", type=int, help="number of files processed at time"
    )

    parser.add_argument(
        "-u", "--per", default="second", type=str, help="granularity of throughput"
    )

    # Batches.
    parser.add_argument(
        "-l", "--limit", type=int, help="maximum number of files to process"
    )

    parser.add_argument(
        "-m", "--marker-dir", default=CWD, type=str, help="directory to store marker files",
    )

    # S3cmd arguments.
    parser.add_argument(
        "attrs", nargs="*", type=str, help="custom attributes passed to the s3cmd directry",
    )

    return parser.parse_args()


def parse(args):

    assert (
        os.path.exists(args.path) and os.path.isdir(args.path)
    ), "--path should be existing directory"
    assert (
        os.path.exists(args.marker_dir) and os.path.isdir(args.marker_dir)
    ), "--marker-dir should be existing directory"
    assert args.s3path, "s3path can not be empty"
    assert args.per in [
        "second", "minute", "hour", "day"
    ], "per should be one of second/minute/hour/day"

    ns = {
        "directory": args.path,
        "do_mtime": not args.ignore_mtime,
        "s3": functools.partial(s3path, args.s3path, args.path),
        "debug": lambda *args: None,
        "jitter": lambda a, b: (a, b),
        "limit": args.limit,
        "cmd_put_template": s3cmd(args.attrs, "put", "{!r}", "{!r}"),
        "cmd_del_template": s3cmd(args.attrs, "del", "{!r}"),
        "marker": os.path.join(args.marker_dir, MARKER_FILE),
        "old_marker": os.path.join(args.marker_dir, OLD_MARKER_FILE),
        "stats": {
            "count_total": 0,
            "count_modified": 0,
            "count_uploaded": 0,
            "count_deleted": 0,
            "count_errors": 0,
        },
    }

    if args.debug:
        ns["debug"] = do_debug

    if args.throughput:
        ns["jitter"] = functools.partial(
            jitter, args.throughput, THROUGHPUT_TABLE[args.per]
        )

    return ns


def do_debug(*args):
    print("DEBUG: {}".format(" ".join(args)))


def jitter(max_files, per_time, files_processed, started_time):
    if files_processed >= max_files:
        time.sleep(max(per_time - int(time.time() - started_time), 0))
        return 0, time.time()
    return files_processed, started_time


def s3cmd(args, *suffix):
    return " ".join(["s3cmd"] + [repr(i) for i in args] + list(suffix))


def s3path(s3buket, directory, filename):
    return "s3://" + os.path.join(s3buket, del_prefix(filename, directory))


def del_prefix(path, prefix):
    if not prefix.endswith("/"):
        prefix += "/"

    if path.startswith(prefix):
        return path[len(prefix) :]

    return path


if __name__ == "__main__":
    kw = parse(cli())
    prepare_markers(**kw)
    main(**kw)
    process_removed(**kw)
    report(**kw)
