'''
Created on Feb 29, 2016

@author: rereidy
'''

import re
import sys
import csv
import struct
import logging
import datetime
import cStringIO
from os.path import basename
from os.path import splitext

class empdwn(object):
    """
    empdwn - Module to parse EMPDWN table data

    Note:  This parser makes assumptions of the format of the data report.

        Report format:
                                                                                                   1         1         1         1
         1         2         3         4         5         6         7         8         9         0         1         2         3
....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v
000 Wells Fargo Dealer Services              AFS USERS REPORT                       EMPDWN  01/14/16  RUN: 01/14/16   PAGE:        1
OBJECT/RP3/EMPDWN                                                                                    TIME: 19:00:58
ASSN TELLER NO BRANCH DEPT PIN CHG  STATUS AUTH LVL EMPL NAME                        EMPL ID  LAST LOGIN
---- --------- ------ ---- -------- ------ -------- -------------------------------- -------- ----------
   1      4193    273 0000               8       20  JAMES DERMODY                   A472223
   2      4193    273 0000               8       20  JAMES DERMODY                   A472223
   1       428    605 0000 11/24/15      1       15 AARON BAUMANN                    A784663    01/14/16
   2       428    605 0000 11/24/15      1       15 AARON BAUMANN                    A784663    01/14/16
   1      7973    580 0000 01/08/16      1       20 AARON CALLWOOD                   U436184
   2      7973    580 0000 01/08/16      1       20 AARON CALLWOOD                   U436184    01/14/16
   1      2326    874 0000 12/31/15      1       29 AARON HOLTON                     A460502
   2      2326    874 0000 12/31/15      1       29 AARON HOLTON                     A460502
   1      5065    605 0000 12/28/15      1       20 AARON KENNEDY                    U302850    01/14/16
   2      5065    605 0000 12/28/15      1       20 AARON KENNEDY                    U302850    01/14/16
    """

    column_hdrs       = []
    TRSEC_RPT_HDR     = re.compile(r'^(000|OBJECT\/RP3\/EMPDWN)')
    TRSEC_COL_HDR     = re.compile(r"^(?P<assn>ASSN)\s+(?P<teller>TELLER NO)\s+(?P<branch>BRANCH)\s+(?P<dept>DEPT)\s+(?P<pin_chg>PIN CHG)\s+(?P<status>STATUS)\s+(?P<auth_lvl>AUTH LVL)\s+(?P<empl_name>EMPL NAME)\s+(?P<empl_id>EMPL ID)\s+(?P<last_login>LAST LOGIN)")
    TRSEC_LINE_LENGTH = 105
    
    #
    #    helper functions for input file field conversions
    #
    cnv_text           = lambda s: s.rstrip()
    cnv_int            = lambda s: int(s)

    fieldspecs = [
        # column_namne  start_pos  len, conversion_function (defined at the script level)
        ('ASSN',        0,         4,   cnv_int),
        ('TELLER NO',   6,         9,   cnv_int),
        ('BRANCH',      15,        6,   cnv_int),
        ('DEPT',        22,        4,   cnv_text),
        ('PIN CHG',     28,        8,   cnv_text),
        ('STATUS',      37,        6,   cnv_int),
        ('AUTH LVL',    44,        7,   cnv_int),
        ('EMPL NAME',   53,        31,  cnv_text),
        ('EMPL ID',     86,        7,   cnv_text),
        ('LAST LOGIN',  97,        8,   cnv_text),
    ]

    def __init__(self, fp, logger, debug=False):
        start_tm = datetime.datetime.now()
        logger.info("empdwn initialization started")
        
        if type(fp) is not file and fp.mode != 'r':
            raise TypeError("empdwn: first argument must be a file object opened for read")
        
        self.fp     = fp
        self.logger = logger
        self.debug  = debug
        self.buffers = []

        unpack_len = 0
        unpack_fmt = ""
        for fieldspec in self.fieldspecs:
            start = fieldspec[1] - 1
            end = start + fieldspec[2]
            if start > unpack_len:
                unpack_fmt += str(start - unpack_len) + "x"
            unpack_fmt += str(end - start) + "s"
            unpack_len = end

        self.field_indices = range(len(self.fieldspecs))

        if self.debug:
            self.logger.debug("unpack_len = {0} unpack_fmt = {1}".format(unpack_len, unpack_fmt))

        self.unpacker = struct.Struct(unpack_fmt).unpack_from
        end_tm = datetime.datetime.now()
        self.logger.info("empdwn initialization complete (elapsed time: {0})".format(end_tm - start_tm))

    def parse(self):
        start_tm = datetime.datetime.now()
        self.logger.info("starting parse")
        for line in self.fp.readlines():
            if line.startswith("000"):
                if not "AFS USERS REPORT" in line:
                    raise ValueError("invalid report type - \"AFS USERS REPORT\" not in '{0}'".format(line))
                 
            hdr_mtch = self.TRSEC_RPT_HDR.match(line.rstrip("\n"))
            if hdr_mtch is not None:
                continue
            
            if line.startswith("ASSN TELLER"):
                colhdr_mtch = self.TRSEC_COL_HDR.match(line.rstrip("\n"))
                if colhdr_mtch is not None and len(self.column_hdrs) == 0:
                    self.column_hdrs = list(colhdr_mtch.groups())
                elif colhdr_mtch is None:
                    raise ValueError("invalid column headers")
                
                if self.debug:
                    self.logger.debug("column headers: '{0}'".format(",".join(self.column_hdrs)))
                    
                continue
            
            if line.startswith("----"):
                    continue
            else:
                if len(line.rstrip("\n")) < self.TRSEC_LINE_LENGTH:
                    field_line = "{0:<105}".format(line.rstrip("\n"))
                else:
                    field_line = line[:].rstrip("\n")

                f = cStringIO.StringIO(field_line)
                buf = []
                for ln in f:
                    flds = self.unpacker(ln)
                    for i in self.field_indices:
                        buf.append(self.fieldspecs[i][3](flds[i]))

                self.buffers.append(buf)

        if self.debug:
            self.logger.info("enpdwn column headers created: {0:d}".format(len(self.column_hdrs)))
            self.logger.info("empdwn data rows saved: {0:d}".format(len(self.buffers)))

        end_tm = datetime.datetime.now()            
        self.logger.info("parse complete (elapsed time: {0})".format(end_tm - start_tm))

    def write_data(self):
        from os import getcwd
        
        start_tm = datetime.datetime.now()
        self.logger.info("start write_data")
        fname = "{0}/{1}.csv".format(getcwd(), splitext(basename(self.fp.name))[0])
        with open(fname, "wb") as csv_fp:
            self.logger.info("writing parsed data to {0}".format(fname))
            writer = csv.writer(csv_fp, dialect='excel', delimiter=',')
            writer.writerow(self.column_hdrs)
            
            written_buf = 0
            for buf in self.buffers:
                writer.writerow(buf)
                written_buf += 1
                
            if self.debug:
                self.logger.info("buffers written: {0:d}".format(written_buf))
                
            end_tm = datetime.datetime.now()
            self.logger.info("write_data complete (elapsed time: {0})".format(end_tm - start_tm))

if __name__ == "__main__":
    start_time = datetime.datetime.now()
    root = logging.getLogger(splitext(basename(sys.argv[0]))[0])
    root.setLevel(logging.DEBUG)    
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - (%(filename)s:%(module)s:%(lineno)d) - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    
    root.info("start {0}".format(basename(sys.argv[0])))
    
    try:
        if len(sys.argv) != 2:
            raise ValueError("usage: {0} tslist_input_fname tslist_out_fname.csv".format(basename(sys.argv[0])))
        
        file_parser = empdwn(open(sys.argv[1], "r"), root, True)
        file_parser.parse()
        file_parser.write_data()
                
    except (ValueError,
            TypeError
           ) as e:
        root.critical(e)
        sys.exit(1)
    except Exception as e:
        root.critical("unexpected exception: {0}".format(e))
        sys.exit(1)
    else:
        root.info("end {0}".format(basename(sys.argv[0])))
        sys.exit(0)    