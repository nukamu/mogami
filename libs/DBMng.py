#! /usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import os, os.path
import Tips
import errno

class MogamiMetaDB(object):
    """Class for Mogami's DB to store metadata
    """
    def __init__(self, path):
        self.db_file = "mogami_meta.db"
        self.db_path = os.path.join(path, self.db_file)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_cur = self.db_conn.cursor()
    
        # Create files table
        self.db_cur.execute("""
        CREATE TABLE files (
        path TEXT PRIMARY KEY,
        mode INT,
        uid INT,
        gid INT,
        nlink INT,
        size INT,
        atime INT,
        mtime INT,
        ctime INT,
        dist TEXT,
        dist_path TEXT
        )
        """)

    def _set_file(self, path, st, dist, dist_path):
        """register metadata of files to DB

        @param path path name
        @param st same with the return value of os.lstat(path)
        @param dist ファイルを持っているメタデータサーバのIP
        @param dist_path メタデータサーバ上でのpath
        @return On success, zero is returned. On error, error number is returned.
        """
        r = self.db_cur.execute("SELECT * FROM files WHERE path = ?;", (path,)).fetchone()
        if r:
            return errno.EEXIST
        self.db_cur.execute("""
        INSERT INTO files (
        path, mode, uid, gid, nlink, size, atime, mtime, ctime, dist, dist_path) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path, st.st_mode, st.st_uid, st.st_gid, st.st_nlink, st.st_size,
              st.st_atime, st.st_mtime, st.st_ctime, dist, dist_path ))
        self.db_conn.commit()
        return 0

    def _rm_file(path):
        r = self.db_cur.execute("", (path))

    def _set_dir(self, path, st):
        """register metadata of a file to DB

        @param path path of the directory
        @param st return value of os.lstat(path)
        @return if success 0, error number with error
        """
        self.db_cur.execute("""
            INSERT INTO dirs (
            path, mode, uid, """)
        """TODO:
        """

    def dump_all(self, ):
        """print information of all files
        """
        self.db_cur.execute("""
        SELECT * FROM files""")
        l = self.db_cur.fetchall()
        for tmp in l:
            for tmp2 in tmp:
                print tmp2,
            print ""
    
    def return_st(self, path):
        """path からファイルのメタデータのリストを返す
        大体os.lstat の代わりに使える
        ファイルがなければ None を返す

        @param path file path
        """
        self.db_cur.execute("""
        SELECT mode, uid, gid, nlink, size, atime, mtime, ctime 
        FROM files WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            return None  # the file doesn't exist
        st_org = l[0]
        st = tips.MogamiStat()
        i = 0
        for attr in st.mogami_attrs:
            setattr(st, attr, st_org[i])
            i += 1
        return st

    def return_dist(self, path):
        """path からどのメタデータサーバがファイルを持っているかを返す

        @param path file path
        @return [データサーバIP, データサーバ上でのpath]
        """
        self.db_cur.execute("""
        SELECT dist, dist_path 
        FROM files WHERE path = ?""", (path, ))
        l = self.db_cur.fetchall()
        if len(l) != 1:
            return None
        ret = [str(l[0][0]), str(l[0][1])]
        return ret

    def access(self, path, mode):
        pass

    def getattr(self, path):
        pass

    def readdir(self, path):
        pass

    def mkdir(self, path):
        pass

    def rmdir(self, path):
        pass
    
    def unlink(self, path):
        pass
            
    def rename(self, oldpath, newpath):
        pass

    def chmod(self, path, mode):
        pass

    def chown(self, path, uid, gid):
        pass

    def truncate(self, path, uid, gid):
        pass

    def utime(self, path, times):
        pass

if __name__ == '__main__':
    try:
        os.mkdir("./meta")
    except Exception, e:
        pass
    db = MogamiMetaDB("./meta")
    i = db.set_row("test", os.lstat("/home/miki/svn/mogami/fs.py"), "testdist", "testpath")
    print i
    i = db.set_row("test", os.lstat("/home/miki/svn/mogami/data.py"), "dist", "path")
    print i
    st = db.return_st("test")
    print st
    dist = db.return_dist("test")
    print dist
