import sys
import sqlite3

FILE_STATUS_ADDED = 'ADDED'
FILE_STATUS_MODIFIED = 'MODIFIED'
FILE_STATUS_NOT_CHANGED = 'NOT CHANGED'
FILE_STATUS_REMOVED = 'REMOVED'

SYNC_STATUS_FAILED = 'FAILED'
SYNC_STATUS_OK = 'OK'


class FileRecord:
    modified_at = ''

    def __init__(self, name):
        self.name = name


def init_db():
    try:
        conn = sqlite3.connect('files.sqlite')
        cur = conn.cursor()

        # Create tables
        cur.execute('''CREATE TABLE IF NOT EXISTS files (
                    id INTEGER PRIMARY KEY,
                    file TEXT, 
                    modified_at REAL, 
                    checked INTEGER, 
                    file_status TEXT,
                    synced_at REAL, 
                    sync_status TEXT
                    )''')

        # Insert a row of data
        # filesInfo = []
        # filesInfo.append( ('file_name', '2006-01-05', 0, '2006-01-05', 'OK') )
        # cur.executemany("INSERT INTO files VALUES ()", filesInfo)
        # cur.execute("INSERT INTO files VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
        # cur.execute("INSERT INTO files VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

        # Save (commit) the changes
        conn.commit()

        # We can also close the connection if we are done with it.
        # Just be sure any changes have been committed or they will be lost.
        conn.close()
    except sqlite3.Error as e:
        print(e)
    except:
        print "Unexpected error:", sys.exc_info()[0]
        raise

if __name__ == "__main__":
    # init_db()
    pass
