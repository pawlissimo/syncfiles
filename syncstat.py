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
#import commands
import datetime
import functools
import os
import sys
import sqlite3
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
DB_PATH_SQLITE = 'files.sqlite'
FILE_STATUS_ADDED = 'ADDED'
FILE_STATUS_MODIFIED = 'MODIFIED'
FILE_STATUS_NOT_MODIFIED = 'NOT MODIFIED'
FILE_STATUS_REMOVED = 'REMOVED'
SYNC_STATUS_FAILED = 'FAILED'
SYNC_STATUS_OK = 'OK'


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


def init_db(**kw):
    try:
        conn = sqlite3.connect(DB_PATH_SQLITE)
        conn.row_factory = sqlite3.Row  # allows us to work with Rows instead of tuples
        cur = conn.cursor()

        cur.execute('''CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY,
                    file TEXT, 
                    modified_at REAL, 
                    checked INTEGER, 
                    file_status TEXT,
                    synced INTEGER, 
                    synced_at REAL, 
                    sync_status TEXT
                    )''')

    except sqlite3.Error as e:
        print(e)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        #raise
    else:
        conn.close()


def index_files(directory, stats, **kw):

    # Amount of collected files before writing to DB
    BATCH_SIZE = 1000

    def check_files(cur, file_names, files_info):
        """ Check file's modification time """
        # "Total: {count_total}. "
        # "Modified: {count_modified}. "
        # "Uploaded: {count_uploaded}. "
        # "Deleted: {count_deleted}. "
        # "Errors: {count_errors}.".format(**stats)

        try:
            qry = """SELECT * FROM files WHERE file in ('{}')""".format("', '".join(file_names))
            cur.execute(qry)
            existed_records = cur.fetchall()  # cur.fetchmany(size=BATCH_SIZE)

            for file_name in file_names:
                existed_record = None
                for rec in existed_records:
                    if file_name == rec['file']:
                        existed_record = rec
                        break

                try:
                    stat = os.stat(file_name)
                except OSError as e:
                    sys.stderr.write("{}\n".format(e))
                    stats["count_errors"] += 1
                    continue

                if existed_record:
                    f_status = FILE_STATUS_NOT_MODIFIED
                    if existed_record['modified_at'] < stat.st_mtime:
                        stats["count_modified"] += 1
                        f_status = FILE_STATUS_MODIFIED
                    else:
                        # we shouldn't process just 'added' files as 'not changed'
                        if existed_record['file_status'] == FILE_STATUS_ADDED:
                            f_status = FILE_STATUS_ADDED
                        # if file was modified before syncing, then we should leave that status until sync
                        elif existed_record['file_status'] == FILE_STATUS_MODIFIED:
                            f_status = FILE_STATUS_MODIFIED

                    files_info.append({
                        "id": existed_record['id'],
                        "file": file_name,
                        "modified_at": stat.st_mtime,
                        "checked": True,
                        "file_status": f_status,
                        "synced": existed_record['synced']
                    })

                    # remove existed_record from existed_records for optimization purposes
                    existed_records.remove(existed_record)
                # new record
                else:
                    files_info.append({
                        # "id": None,
                        "file": file_name,
                        "modified_at": stat.st_mtime,
                        "checked": True,
                        "file_status": FILE_STATUS_ADDED,
                        "synced": False
                    })

            return True
        except sqlite3.Error as e:
            print(e)
            return False


    def update_db(cur, files_info):
        # qry_strings = []
        new_files = []
        modified_files = []
        not_modified_files = []
        not_modified_added_files = []
        for info in files_info:
            if not info.get('id'):
                new_files.append( (info.get('file'), info.get('modified_at'), FILE_STATUS_ADDED ))
                # qry_strings.append("INSERT INTO files(file, modified_at, checked, file_status) VALUES (info.get('file'), info.get('modified_at'), 1, FILE_STATUS_ADDED)")
            else:
                if info.get('file_status') == FILE_STATUS_MODIFIED:
                    modified_files.append( (info.get('modified_at'), FILE_STATUS_MODIFIED, info.get('id')) )
                    # qry_strings.append("UPDATE files SET modified_at=info.get('modified_at'), checked=1, file_status=FILE_STATUS_MODIFIED WHERE id = info.get('id')")
                elif info.get('file_status') == FILE_STATUS_NOT_MODIFIED:
                    not_modified_files.append( info.get('id') )
                    # qry_strings.append("UPDATE files SET file_status=FILE_STATUS_NOT_MODIFIED WHERE id in (info.get('id'))")
                elif info.get('file_status') == FILE_STATUS_ADDED:
                    not_modified_added_files.append( info.get('id') )

        # add new files
        if new_files:
            cur.executemany("INSERT INTO files(file, modified_at, checked, file_status) VALUES (?, ?, 1, ?)", new_files)

        # update modified
        if modified_files:
            cur.executemany("UPDATE files SET modified_at=?, checked = 1, file_status=? WHERE id = ?", modified_files)

        # update not modified
        if not_modified_files:
            qry = "UPDATE files SET checked = 1, file_status=? WHERE id IN ({})".format(', '.join(str(x) for x in not_modified_files))
            cur.execute(qry, (FILE_STATUS_NOT_MODIFIED, ))
        if not_modified_added_files:
            qry = "UPDATE files SET checked = 1, file_status=? WHERE id IN ({})".format(', '.join(str(x) for x in not_modified_added_files))
            cur.execute(qry, (FILE_STATUS_ADDED,) )

        # cur.executescript("""""".join(qry)) # to surround final qry by """


    try:
        # 0. establish DB connection
        conn = sqlite3.connect(DB_PATH_SQLITE)
        conn.row_factory = sqlite3.Row  # allows us to work with Rows instead of tuples
        cur = conn.cursor()

        # 1. refresh current index
        cur.execute("UPDATE files SET checked = 0")

        # 2. scan for added/modified/not modified files
        # for optimization purposes we process batches
        files_processed = 0
        files_info = []     # array of dict (id=123, file='file_path', modified_at=12345, file_status='some_status')
        files_to_check = [] # array of filenames
        for root, dirs, files in os.walk(directory):
            for name in files:
                filename = os.path.join(root, name)
                files_to_check.append(filename)
                files_processed += 1
                stats["count_total"] += 1

                if files_processed >= BATCH_SIZE:
                    check_files(cur, files_to_check, files_info)
                    update_db(cur, files_info)
                    files_processed = 0
                    files_info = []
                    files_to_check = []
                    print("files processed: {}".format(stats["count_total"]))
        if files_processed > 0:
            check_files(cur, files_to_check, files_info)
            update_db(cur, files_info)
            files_processed = 0
            files_info = []
            files_to_check = []
            print("files processed: {}".format(stats["count_total"]))

        # 3. mark rest files as "REMOVED"
        cur.execute("UPDATE files SET checked = 1, file_status = ? WHERE checked = 0", (FILE_STATUS_REMOVED, ))

        # Save (commit) the changes
        conn.commit()

    except sqlite3.Error as e:
        print(e)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        #raise
    else:
        conn.close()


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
    """
    Executes s3cmd commands.
    https://s3tools.org/usage
    Examples:
        s3cmd put FILE [FILE...] s3://BUCKET[/PREFIX]
        s3cmd del s3://BUCKET/OBJECT
        s3cmd get s3://BUCKET/OBJECT LOCAL_FILE
    """
    return " ".join(["s3cmd"] + [repr(i) for i in args] + list(suffix))


def s3path(s3buket, directory, filename):
    """ Build s3 path by removing directory prefix from file path. I.e. using only file's name"""
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
    init_db(**kw)

    # start timer
    start_time = time.time()

    index_files(**kw)
    # main(**kw)
    # process_removed(**kw)
    report(**kw)

    # print execution time
    print("Executed in %s seconds" % (time.time() - start_time))
