#! /usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import os.path
import sqlite3
import cPickle

PWD = "/home/miki/svn/workflows/apps/montage/solvers/gxp_make"
EXE_PWD = "/data/local2/mikity/mnt/montage/solvers/gxp_make"
MOGAMI_MOUNT = "/data/local2/mikity/mnt"


def file_from_feature(cmd, feature_dict):
    """
    @param cmd list of arguments
    @param feature like {(-1, -1, "", "-f", 5): load, (): load}
    >>> file_from_feature(['mProjectPP', '-f', 'file2', 'file3', 'file4', 'file5', 'file6'], {(-1, -1, '', '-f', 5): 1000})
    {'/montage/solvers/gxp_make/file2': 1000, '/montage/solvers/gxp_make/file5': 1000}
    """
    features = feature_dict.keys()

    ret_file_dict = {}

    for feature in features:
        size = feature_dict[feature]
        start = feature[0]
        end = feature[1]
        plus_str = feature[2]
        option = feature[3]
        count = feature[4]
        
        if plus_str == "":
            counter = 0
            if count == -1:
                filename = os.path.join(EXE_PWD, option)
                filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))
                if filename not in ret_file_dict:
                    ret_file_dict[filename] = size
                else:
                    ret_file_dict[filename] += size
                
            for arg in cmd:
                filename = os.path.join(EXE_PWD, arg)
                filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))
                if counter == count:
                    if arg not in ret_file_dict:
                        ret_file_dict[filename] = size
                    else:
                        ret_file_dict[filename] += size
                if option == arg:
                    filename = os.path.join(EXE_PWD, cmd[counter + 1])
                    filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))
                    if filename not in ret_file_dict:
                        ret_file_dict[filename] = size
                    else:
                        ret_file_dict[filename] += size
                counter += 1
        else:
            counter = 0
            for arg in cmd:
                if counter == count:
                    if len(arg) < start + end:
                        print start, end
                        continue
                    filename = arg[:start + 1] + plus_str + arg[-end:]
                    filename = os.path.join(EXE_PWD, filename)
                    filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))
                    if filename not in ret_file_dict:
                        ret_file_dict[filename] = size
                    else:
                        ret_file_dict[filename] += size
                elif option == arg:
                    if len(arg) < start + end:
                        print start, end
                        continue
                    filename = cmd[counter + 1][:start + 1] + plus_str + arg[counter + 1][-end:]
                    filename = os.path.join(EXE_PWD, filename)
                    filename = os.path.normpath(filename.replace(MOGAMI_MOUNT, ""))

                    if filename not in ret_file_dict:
                        ret_file_dict[filename] = size
                    else:
                        ret_file_dict[filename] += size
                counter += 1

    return ret_file_dict

class MogamiWorkflowIOAnalyzer(object):
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_conn = sqlite3.connect(self.db_path)
        self.db_cur = self.db_conn.cursor()

        self.command_dict = {}
        
    def analize(self, ):
        self.make_sets()
        

    def make_sets(self, ):
        """
        This function should be called in self.analize().
        """
        aplog_list = self._parse_data_from_db()
                
        for ap in aplog_list:
            cmd = ap[0]
            pid = ap[1]
            path = ap[2]
            created = ap[3]
            read_log = ap[4]
            write_log = ap[5]

            app = cmd[0]
            if app not in self.command_dict:
                self.command_dict[app] = ([{}, 0], [{}, 0])
            read_features = self.command_dict[app][0][0]
            write_features = self.command_dict[app][1][0]
            self.command_dict[app][0][1] += 1
            self.command_dict[app][1][1] += 1

            counter = 0
            former_arg = None

            read_feature = None
            read_size = 0
            write_feature = None
            write_size = 0

            for arg in cmd:
                cmd_file = os.path.normpath(os.path.join(PWD, arg))
                likely = self.my_str_find(cmd_file, path)

                if cmd_file == path:
                    option = ""
                    if former_arg[0] == '-':
                        option = former_arg
                    if len(read_log) > 0:
                        read_feature = (-1, -1, '', option, counter)
                        read_size = self.size_from_iolog(read_log)
                    if len(write_log) > 0:
                        write_feature = (-1, -1, '', option, counter)
                        write_size = self.size_from_iolog(write_log)
                    break
                elif likely != None:
                    option = ""
                    if former_arg[0] == '-':
                        option = former_arg

                    if len(read_log) > 0:
                        read_feature = (likely[0], likely[1], likely[2],
                                        option, counter)
                        read_size = self.size_from_iolog(read_log)
                    if len(write_log) > 0:
                        write_feature = (likely[0], likely[1], likely[2],
                                         option, counter)
                        write_size = self.size_from_iolog(write_log)

                counter += 1
                former_arg = arg

            rel_path = path.replace(PWD, '')
            if read_feature == None:
                if len(read_log) > 0:
                    read_feature = (-1, -1, "", rel_path, -1)
                    read_size = self.size_from_iolog(read_log)
            if write_feature == None:
                if len(write_log) > 0:
                    write_feature = (-1, -1, "", rel_path, -1)
                    write_size = self.size_from_iolog(write_log)

            if read_feature != None:
                if read_feature in read_features:
                    read_features[read_feature] += read_size
                else:
                    read_features[read_feature] = read_size
            if write_feature != None:
                if write_feature in write_features:
                    write_features[write_feature] += write_size
                else:
                    write_features[write_feature] = write_size

    def size_from_iolog(self, log_list):
        """
        @param log list like [(0, 1024), (2048, 1024)]
        """
        size = 0
        for log in log_list:
            size += log[1]
        return size

    def _parse_data_from_db(self, ):
        # select data
        self.db_cur.execute("""
        SELECT * FROM ap_log;
        """)

        count = 0
        ret_list = []
        for data in self.db_cur:
            cmd = eval(data[0])
            pid = data[1]
            path = data[2]
            created = data[3]
            read_log = eval(data[4])
            write_log = eval(data[5])
            ret_list.append((cmd, pid, path, created,
                               read_log, write_log))
        # close the connection to db
        self.db_conn.close()
        self.db_cur = None

        return ret_list

    def output(self, output_file):
        """Output result to the specified file.

        @param output_file path of output file
        """
        f = open(output_file, 'w+')
        f.write(cPickle.dumps(self.command_dict))
        f.close()

    def my_str_find(self, str1, str2):
        """
        """
        str1_base = os.path.basename(str1)
        str2_base = os.path.basename(str2)

        if len(str1_base) > len(str2_base):
            return None

        counter = 0
        while counter < len(str1_base):
            if str1_base[counter] == str2_base[counter]:
                counter += 1
            else:
                break

        from_left_offset = counter - 1

        counter = 0
        while counter < len(str1_base):
            if str1_base[-counter] == str2_base[-counter]:
                counter += 1
            else:
                break

        from_right_offset = counter - 1

        if from_left_offset == -1 and from_right_offset == -1:
            return None
        plus_str = str2_base[from_left_offset + 1: -from_right_offset]
        if plus_str == "":
            return None
        if (from_left_offset + from_right_offset + 1) != len(str1_base):
            return None

        #print str1_base, str2_base
        return (from_left_offset, from_right_offset, plus_str)

if __name__ == "__main__":
    import doctest
    doctest.testmod()
    if len(sys.argv) != 3:
        sys.exit("Usage: %s [db_path] [output_file]" % (sys.argv[0]))
    workflow_analyzer = MogamiWorkflowIOAnalyzer(sys.argv[1])
    workflow_analyzer.analize()
 
    #f = open("montage_data0.csv", 'r')
    #for i in range(int(sys.argv[3])):
    #    tmp = f.readline()
    #f.close()
    #next_cmd = eval(tmp.split("|")[0])
    #print next_cmd

    #print file_from_feature(next_cmd, workflow_analyzer.command_dict[next_cmd[0]][0][0])
    #print file_from_feature(next_cmd, workflow_analyzer.command_dict[next_cmd[0]][1][0])
    workflow_analyzer.output(sys.argv[2])
