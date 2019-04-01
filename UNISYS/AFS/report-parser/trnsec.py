'''
Created on Mar 1, 2016

@author: rereidy
'''

import re
import sys
import csv
import logging
import datetime
from os.path import basename
from os.path import splitext

class trnsec(object):
    """
    trsec - Module to parse trsec data

    Note:  This parse makes assumptions of the format of the data report.

        Report format:
                                                                                                   1         1         1         1
                                                                                                   0         1         2         3
....v....1....v....2....v....3....v....4....v....5....v....6....v....7....v....8....v....9....v....0....v....0....v....0....v....0....v
001 Wells Fargo Dealer Services              TRANSACTION SECURITY REPORT            TRNSEC  REPORT 01/15/16  RUN 01/15/16  PAGE    1
                                             JD/AUTHORITY LEVEL 11 TRANSACTIONS
      FM-MSEMP FM-MSEM3 FM-MSINA FM-MSMAA FM-MSTAS FM-MSTRA IQ-MSCUR IQ-MSEM0 IQ-MSEM9
EMPL NO: 0876-3 HR NO: X8173 NAME: RICHARD COLLINS                  BR NO: 620 LAST PIN CHG: 12/30/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 1522-2 HR NO: A1400 NAME: DENNIS COX                       BR NO: 620 LAST PIN CHG: 11/25/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 2299-6 HR NO: U1392 NAME: DAVID WEISS                      BR NO: 620 LAST PIN CHG: 12/07/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 2466-1 HR NO: U1512 NAME: MANNY ROSALES                    BR NO: 620 LAST PIN CHG: 12/28/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 2467-9 HR NO: U1512 NAME: THOMAS RICHMAN                   BR NO: 620 LAST PIN CHG: 12/30/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 3273-0 HR NO: U1996 NAME: NICOLE KAHN                      BR NO: 620 LAST PIN CHG:  1/14/16 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 3893-5 HR NO: U2211 NAME: ERIK FLETCHER                    BR NO: 620 LAST PIN CHG:  1/08/16 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 3908-1 HR NO: U2523 NAME: JOSHUA BURNHAM                   BR NO: 620 LAST PIN CHG: 12/11/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 4216-8 HR NO: A5770 NAME: DAMIAN MEDRANO                   BR NO: 620 LAST PIN CHG: 12/01/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 5096-3 HR NO: u3203 NAME: CHRISTINE HARO                   BR NO: 620 LAST PIN CHG: 12/29/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 5486-6 HR NO: A4737 NAME: RIGO GARCIA                      BR NO: 620 LAST PIN CHG: 12/21/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 5538-4 HR NO: U3513 NAME: LOUIS J ROSE                     BR NO: 620 LAST PIN CHG:  1/11/16 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 5684-6 HR NO: A4604 NAME: GLENN HEREN                      BR NO: 620 LAST PIN CHG: 12/16/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 5738-0 HR NO: U3592 NAME: ED GONZALEZ                      BR NO: 620 LAST PIN CHG: 12/29/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 6263-8 HR NO: U3820 NAME: DEZIYON RAYFORD                  BR NO: 620 LAST PIN CHG:  1/02/16 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 6264-6 HR NO: U3808 NAME: DEVON SIOMA                      BR NO: 620 LAST PIN CHG: 11/30/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 6474-1 HR NO: u3910 NAME: ELOID CAMARENA                   BR NO: 620 LAST PIN CHG:  1/04/16 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 7025-0 HR NO: u4097 NAME: NARIMAN SALAHSHOUR               BR NO: 620 LAST PIN CHG:  3/30/15 PIN CHG DAYS: 60 GLOBAL: 0
001 Wells Fargo Dealer Services              TRANSACTION SECURITY REPORT            TRNSEC  REPORT 01/15/16  RUN 01/15/16  PAGE    2
                                             JD/AUTHORITY LEVEL 11 TRANSACTIONS
EMPL NO: 7028-4 HR NO: u4102 NAME: COREY GOLSTON                    BR NO: 620 LAST PIN CHG: 12/11/15 PIN CHG DAYS: 60 GLOBAL: 0
EMPL NO: 8483-0 HR NO: U4425 NAME: ALISON OLDHAM                    BR NO: 620 LAST PIN CHG: 12/21/15 PIN CHG DAYS: 60 GLOBAL: 0
"""

    column_hdrs      = []
    TRSEC_RPT_HEADER = re.compile(r"^00[1|2]")
    TRSEC_LEVEL_LINE = re.compile(r'^\s+JD\/AUTHORITY LEVEL\s+(?P<level>\d+)')
    TRSEC_COL_HDR    = re.compile(r"^\s{7}\S+")
    TRSEC_TRANSCODE  = re.compile(r"^\s{7}(?P<trans_code>([A-Z]{2}|[+ |- ]\w{3,4}))+")
    TRSEC_DATA_LINE  = re.compile(r"^(?P<emp>EMPL NO: \d+\-\d+)\s*(?P<hr>HR NO: \w+)\s*(?P<name>NAME: \w+\s{1}\w+)\s+(?P<br>BR NO: \d+)\s+(?P<pin_chg>LAST PON CHG: \d{2}\/\d{2}\/\d{2})\s+(?P<pin_chg_days>PIN CHG DAYS: \d+)\s+(?P<global>GLOBAL: \d+)")

    def __init__(self, fp, logger, debug=False):
        start_tm = datetime.datetime.now()
        logger.info("trnsec initialization started")
        
        self.fp                = fp
        self.logger            = logger
        self.debug             = debug
        self.buffers           = []
        self.transaction_codes = {}
        
        if type(fp) is not file and fp.mode != 'r':
            raise TypeError("first argument must be a file object opened for read")

        end_tm = datetime.datetime.now()
        self.logger.info("trnsec initialization complete (elapsed time: {0})".format(end_tm - start_tm))

    def parse(self):
        start_tm = datetime.datetime.now()
        self.logger.info("starting parse")

        level = None
        old_level = None
        trans_codes = []
        
        for line in self.fp.readlines():
            hdr_mtch = self.TRSEC_RPT_HEADER.match(line.rstrip("\n"))
            if hdr_mtch is not None:
                continue
            
            lvl_mtch = self.TRSEC_LEVEL_LINE.match(line.rstrip("\n"))
            if lvl_mtch is not None:
                level = lvl_mtch.groups('level')
                if old_level != level:
                    self.transaction_codes[old_level] = trans_codes
                    trans_codes = list()
                    old_level = level
                    
                continue
            
            transcd_mtch = self.TRSEC_TRANSCODE.match(line.rstrip("\n"))
            if transcd_mtch is not None:
                for transcd in transcd_mtch.groups():
                    trans_codes.append(transcd)
                continue

            line_mtch = self.TRSEC_DATA_LINE.match(line.rstrip)
            if line.mtch is not None:
                buf = []
                if len(self.column_hdrs) == 0:
                    for col in line_mtch.groups():
                        (cname, data) = col.split(':')
                        self.column_hdrs.append(cname)
                        buf.append(data)

                    if self.debug:
                        self.logger.debug("column headers: '{0}'".format(",".join(self.column_hdrs)))
                else:
                    for col in line.mtch.groups():
                        buf.append(col.split(':')[1].rstrip(' '))

                self.buffers.append(buf)

        if self.debug:
            self.logger.debug("trnesec column headers created: {0:d}".format(len(self.column_hdrs)))
            self.logger.debug("trnsec transaction codes saved: {0:d}".format(len(self.transaction_codes.keys())))
            self.logger.debug("trnsec data rows saved: {0:d}".format(len(self.buffers)))
            
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
        
        file_parser = trnsec(open(sys.argv[1], "r"), root, True)
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