'''
Created on Feb 29, 2016

@author: "Ron Reidy"
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

class tslist(object):
    """
    tslist - Module to parse TSLIST table data

    Note:  This parser makes assumption on the format of the TSLIST report.  Data parsing
           in this module is dependent on position (see the class-level variable
           'fieldspecs').

    Report format:
                                                                                                   1         1         1         1         1
         1         2         3         4         5         6         7         8         9         0         1         2         3         4
....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....v....0....V....0....v....0....v....0....v....0
001 Wells Fargo Dealer Services              SECURITY TRANSACTION LIST              TSLIST  REPORT 01/15/16  RUN 01/15/16  PAGE    1
AP TRC LN TRX# TT DESC                JOB DESCRIPTIONS
AC AA  00 6430 IQ ALLOCATE ADDTL PRIN
AC AP  00 6404 FM AVAIL PATTERNS INQ
AC CP  00 6409 FM LOAN PATTERN MAINT
AC CPH 00 6441 IQ CL PATTERN HIST
AC CPR 00 6413 FM CL REQUISITION
AC CP1 00 6414 FM CL SETUP
AC DC  00 6405 FM CONFIRM DELETE
AC DD  00 6416 IQ NEW ACLS SCREEN 1
AC DL  00 6417 IQ NEW ACLS SCREEN 2
AC DR  00 6421 FM DISBURSEMENT REQ
AC DS  00 6425 IQ DISBURSEMENT SUMM
AC GC  00 6403 FM COMMERCIAL PATRN FM
AC GR  00 6402 FM GENERIC PATTERN FM
AC IR  00 6411 FM INSPECTION REPORT
AC LD  00 6410 FM LOT DETAILS
AC LP  00 6460 FM LIP DETAILS
AC LS  00 6440 IQ LOAN SUMMARY
AC MD  00 6412 FM MISC DISBURSEMENT
AC MM  00 6401 FM MAIN MENYU
AC MM2 00 6415 IQ LIP SETUP
AC MSG 00 6450 IQ MESSAGES
AC NE  00 6418 FM NOTING EVENTS
AC RH  00 6424 IQ DISBURSEMENT HIST
AC RP  00 6408 FM RESIDENT DRAL PATRN
AC SI  00 6406 IQ LOAN STATUS INQUIRY
AC TBD 00 6407 FM TITLE BRING DOWN
AP DC  00 8050 FM DISTRIBUTION CORR
AP DD  00 8012 FM DISTRIBUTION DELETE
AP DI  00 8024 IQ DISTRIBUTION INQ    71
CD AIN 00 4401 IQ ACCRUED INT INQ     06 08 71 72 82 84
CD AIQ 00 5099 IQ ALPHA INQUIRY       08 71 72
CD ALC 00 5019 FM ALT NAME ADDR CHNGE
CD ALD 00 5020 FM ALT NAM ADDR DELETE
CD ALS 00 5018 FM ALT NAME ADDR SETUP
CD AR2 00 4203 IQ AUDIO BAL INQ
CD BAL 00 4201 IQ BALANCE INQUIRY     06 08 53 64 71 72 82 84
CF AIQ 00 5099 IQ CIF ALPHA INQ       06 08 12 14 15 16 18 19 20 22 25 27 30 43 44 45 46 48 49 50 51 52 53 58 64 66 69 70 71 72 82
CF ALT 00 5109 FM ALT NAME ADDR MAINT 12 14 15 16 18 19 20 22 25 27 30 46 49 50 52 58 64 84
CF ALU 00 5115 IQ ACCT NUMBER LOOKUP  06 08 12 14 15 16 18 19 20 22 25 27 29 30 43 44 45 46 47 48 49 50 51 52 53 58 64 66 69 70 71
CF APR 00 5102 IQ ACCOUNT PROFILE     06 08 12 14 15 16 18 25 27 29 30 43 44 45 46 47 48 49 50 51 52 53 58 64 66 69 70 71 72 82 84
CF CCR 00 5124 IQ COMB 2 CUST - REQ   12 14 15 16 18 19 20 22 25 27 30 44 48 49 50 52 58 66 71
CF CCU 00 5125 FM COMB 2 CUST - UPDT  12 15 16 18 19 20 22 25 27 30 49 50 52 58
CF CFC 00 5123 IQ CUST INQ-NATIVE MOD 12 14 15 16 18 27 30 43 44 45 47 49 50 51 52 53 58 64 66 71 91
CF CFP 00 5119 IQ PLATFORM INQUIRY    12 19 20 22 25 27 30 43 44 45 46 48 49 50 58 71
CF CIQ 00 5112 IQ NEXT TRAN REPLY     08 12 14 15 16 18 19 20 22 25 27 29 30 43 44 45 46 48 49 50 52 53 58 64 66 71 84 90 91
CF CLD 00 5137 FM COLLAT SYS SCREENS  12 14 15 16 18 19 20 22 27 29 30 58 66 98
CF CLM 00 5136 FM COLLAT SYS MENU     12 14 15 16 18 19 20 22 25 27 29 30 58 66
CF CLQ 00 5138 IQ COLLAT SYS INQ      14 15 16 18 66
CF CLT 00 5130 FM COLLAT TRACK TEST
002 Wells Fargo Dealer Services              SECURITY TRANSACTION LIST              TSLIST  REPORT 01/15/16  RUN 01/15/16  PAGE    1
AP TRC LN TRX# TT DESC                JOB DESCRIPTIONS
CF AIQ 00 5099 IQ CIF ALPHA INQ       12 14 15 16 18 19 20 22 25 27 30 66 69 70 91 98
CF ALT 00 5109 FM ALT NAME ADDR MAINT 12 14 15 16 18 19 20 22 25 27 30
CF ALU 00 5115 IQ ACCT NUMBER LOOKUP  12 14 15 16 18 19 20 22 25 27 29 30 66 69 70 91 98
CF APR 00 5102 IQ ACCOUNT PROFILE     12 14 15 16 18 25 27 29 30 66 69 70 91 98
CF CCR 00 5124 IQ COMB 2 CUST - REQ   12 14 15 16 18 19 20 22 25 27 30 66
CF CCU 00 5125 FM COMB 2 CUST - UPDT  12 15 16 18 19 20 22 25 27 30
CF CFC 00 5123 IQ CUST INQ-NATIVE MOD 12 14 15 16 18 27 30 66 91
CF CFP 00 5119 IQ PLATFORM INQUIRY    12 19 20 22 25 27 30
CF CIQ 00 5112 IQ NEXT TRAN REPLY     12 14 15 16 18 19 20 22 25 27 29 30 66 91
CF CLD 00 5137 FM COLLAT SYS SCREENS  12 14 15 16 18 19 20 22 27 29 30 66 98
CF CLM 00 5136 FM COLLAT SYS MENU     12 14 15 16 18 19 20 22 25 27 29 30 66
CF CLQ 00 5138 IQ COLLAT SYS INQ      14 15 16 18 66
CF CLT 00 5130 FM COLLAT TRACK TEST

           In the above sample data:
           1.  Any line beginning with "00[1|2]" is a report header line.  The
               first 3 characters are saved to indicate which report the data
               originated from.
           2.  Any line beginning with "AP TRC " is a column header line (see the variable
               "fieldspecs") below.  Column headers are only processed once.
           3.  Line lengths in the report are of variable length.  Lines without a
               "JOB DESCRIPTION" column are right padded with spaces to column 38 to allow
               the "unpacker" function to properly parse the line (see the builtin 'struct'
               module for details on unpacking).
           4.  The data in the "JOB DESCRIPTION" column can contain leading white space
               (tabs, spaces, etc.).  This white space is stripped to conserve space and
               allow ease of readability in the final CSV output file.
    """

    #
    #   class variables
    #
    column_hdrs        = []
    TSLIST_RPT_HEADER  = re.compile(r"^(?P<rpt>00[1|2])")
    TSLIST_COL_HDR     = re.compile(r"^AP TRC ")
    TSLIST_LINE_LENGTH = 38                     # see Report format above
    cnv_text           = lambda s: s.rstrip()   # helper function for input file field conversion

    fieldspecs         = [
        # column_namne  start_pos  len, conversion_function (defined at the script level)
        ('AP',          0,         2,   cnv_text),
        ('TRC',         3,         3,   cnv_text),
        ('LN',          7,         2,   cnv_text),
        ('TRX',         10,        4,   cnv_text),
        ('TT',          15,        2,   cnv_text),
        ('DESC',        18,        19,  cnv_text),
    ]

    def __init__(self, fp, logger, debug=False):
        start_tm = datetime.datetime.now()
        logger.info("tslist initialization started")
        
        if type(fp) is not file and fp.mode != 'r':
            raise TypeError("tslist: first argument must be a file object opened for read")

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
            self.logger.debug("unpack_len = {0}, unpack_fmt = {1}".format(unpack_len, unpack_fmt))

        self.unpacker = struct.Struct(unpack_fmt).unpack_from
        end_tm = datetime.datetime.now()
        self.logger.info("tslist initialization complete (elapsed time: {0})".format(end_tm - start_tm))

    def _test_array_numeric(self, arr):
        if self.debug:
            self.logger.debug("_test_array_numeric(): {0}".format(",".join(arr)))
            
        is_numeric = True
        for a in arr:
            if not a.isdigit():
                is_numeric = False
                
        return is_numeric

    def _validate_column_header(self, line):
        valid_hdr = True
        COLUMN_HEADERS = "AP TRC LN TRX# TT DESC JOB DESCRIPTIONS"
        single_word_col_headers = COLUMN_HEADERS.split(" ")[:-2:]
        
        for hdr in single_word_col_headers:
            if hdr not in line:
                valid_hdr = False
                
        if not " ".join(COLUMN_HEADERS.split(" ")[-2:]) in line:
            valid_hdr = False
            
        return valid_hdr
        
    def parse(self):
        """
        parse - parse data lines

        Parsed data lines are saved to the class variable 'buffers'
        """
        start_tm = datetime.datetime.now()
        self.logger.info("starting parse")
        hdr_lvl     = -1
        old_hdr_lvl = None

        for line in self.fp.readlines():
            hdr_mtch = self.TSLIST_RPT_HEADER.match(line.rstrip("\n"))
            if hdr_mtch is not None:
                if not "SECURITY TRANSACTION LIST" in line:
                    raise ValueError("invalid report header: expected \"SECURITY TRANSACTION LIST\" in header line")
                
                hdr_lvl = int(hdr_mtch.groups('rpt')[0])
                if old_hdr_lvl is None:
                    old_hdr_lvl = hdr_lvl
                continue

            colhdr_mtch = self.TSLIST_COL_HDR.match(line.rstrip("\n"))
            if colhdr_mtch is not None:
                if not self._validate_column_header(line):
                    raise ValueError("invalid column header: expected \"{0}\": found \"{1}\"".format(line))

                if len(self.column_hdrs) == 0:
                    self.column_hdrs.append('ASSN #')
                    self.column_hdrs.append(line[0:2])
                    self.column_hdrs.append(line[3:6])
                    self.column_hdrs.append(line[7:9])
                    self.column_hdrs.append(line[10:14])
                    self.column_hdrs.append(line[15:17])
                    self.column_hdrs.append('TRNSEC code')
                    self.column_hdrs.append(line[18:22])
                    self.column_hdrs.append('Assigned level')

                    if self.debug:
                        self.logger.debug("tslist column headers: {0}".format(",".join(self.column_hdrs)))
            else:
                if len(line.rstrip("\n")) < self.TSLIST_LINE_LENGTH:
                    field_line = "{0:<38}".format(line.rstrip("\n"))
                else:
                    field_line = line[:].rstrip("\n")

                job_descriptions = None
                if len(line.rstrip("\n")) > self.TSLIST_LINE_LENGTH:
                    job_descriptions = "{0}".format(line[38:].lstrip().rstrip("\n"))

                f = cStringIO.StringIO(field_line)
                buf = []
                buf.append(old_hdr_lvl)
                for ln in f:
                    flds = self.unpacker(ln)
                    for i in self.field_indices:
                        if i == 5:
                            buf.append("{0}-{1}{2}".format(buf[5], buf[1], buf[2]))
                        buf.append(self.fieldspecs[i][3](flds[i]))                        


                if job_descriptions is not None:
                    original_buf = list(buf)
                    job_ids = job_descriptions.split(' ')
                    if self._test_array_numeric(job_ids):
                        for job_id in job_ids:
                            buf.append("={0}".format(job_id))
                            self.buffers.append(buf)
                            buf = list()
                            buf = list(original_buf)
                else:
                    self.buffers.append(buf)

                if old_hdr_lvl != hdr_lvl:
                    old_hdr_lvl = hdr_lvl

        if self.debug:
            self.logger.debug("tslist column headers created: {0:d}".format(len(self.column_hdrs)))
            self.logger.debug("tslist data rows saved: {0:d}".format(len(self.buffers)))

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

    try:
        if len(sys.argv) != 2:
            raise ValueError("usage: {0} tslist_input_fname tslist_out_fname.csv".format(basename(sys.argv[0])))

        root.info("start {0}".format(basename(sys.argv[0])))
        file_parser = tslist(open(sys.argv[1], "r"), root, True)
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
