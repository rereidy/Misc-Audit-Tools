from __future__ import print_function

"""
afs_rpt_parser.py - UNISYS data report parser

Author:  Ron Reidy, WFAS
Creation date:

This program runs under Python 2.7 or higher and Jython 2.7 or higher

Synopsys: pyython afs_rpt_parse.py args

usage: USRRPT_parser.py [-h] [-v] [-c] [-d FLD_DELIM] -i INFILE -t
                        {empdwn,tslist,trnsec} [-b] -o OUTFILE

AFS report parser

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -c, --copyright       print copyright statement and exit
  -i INFILE, --infile INFILE
                        name of file to parse
  -t {empdwn,tslist}, --type {empdwn,tslist}
                        Unisys report file type
  -d, --debug           enable debug logging

This Python program will parse the following UNISYS mainframe reports that can be obtained
during fieldwork.
"""

import os
import sys
import atexit
import logging
import argparse
import datetime

from os.path import basename
from os.path import splitext

import empdwn
import trnsec
import tslist

_version      = "1.0"
_author       = "Ron Reidy"
_copyright    = """
{0} - UNISYS/AFS report parser

Author: {1}

Parse Unisys mainframe reports into Excel spreadsheet (CSV) format

Copyright 2016:  This program is the property of {1}.  You may copy and/or use it
as you feel, but may not claim it as your own work.
""".format(os.path.basename(sys.argv[0]), _author)

start_time = datetime.datetime.now()
root = logging.getLogger(splitext(basename(sys.argv[0]))[0])

@atexit.register
def finish():
    end_time = datetime.datetime.now()
    root.info("end {0}: (elapsed time: {1})".format(os.path.basename(sys.argv[0]),
                                                    end_time - start_time
                                                   )
              )

def print_copyright(ver):
    """
        print_copyright - print copyright statement
    """
    print(ver, file=sys.stderr)
    print(_copyright, file=sys.stderr)

if __name__ == "__main__":
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - (%(filename)s:%(module)s:%(lineno)d) - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    
    root.info("starting {0}".format(sys.argv[0]))

    ap = argparse.ArgumentParser(
        description = "Unisys/AFS report parser",
        version     = "{0}: Release {1} Production on {2}".format(os.path.basename(sys.argv[0]),
                                                                  _version,
                                                                  datetime.datetime.now().strftime("%c")
                                                                 )
    )

    ap.add_argument("-c", "--copyright",
                    dest    = "copyright",
                    help    = "print copyright statement and exit",
                    default = False,
                    action  = "store_true"
                   )

    ap.add_argument("-i", "--infile",
                    dest     = "infile",
                    help     = "name of file to parse",
                    action   = "store",
                    type     = argparse.FileType('r')
                   )

    ap.add_argument("-t", "--type",
                    dest     = "filetype",
                    help     = "Unisys/AFS report file type",
                    action   = "store",
                    choices  = ['tslist', 'trnsec', 'empdwn']
                   )

    ap.add_argument("-d", "--debug",
                    dest    = "debug",
                    help    = "enable debug logging",
                    action  = "store_true",
                    default = False
                   )

    options = ap.parse_args()

    if options.copyright:
        print_copyright(ap.version)
        ap.print_help()
        sys.exit(0)
        
    if not options.infile:
        raise ValueError("error: argument -i/--infile is required")
    
    if not options.filetype:
        raise ValueError("error: argument -t/--type is required")
    
    try:
        if options.filetype == 'userdatafile':
            fp = empdwn.empdwn(options.infile, root, options.debug)
        elif options.filetype == 'tslist':
            fp = tslist.tslist(options.infile, root, options.debug)
        elif options.filetype == 'trnsec':
            fp = trnsec.trnsec(options.infile, root, options.debug)
            
        fp.parse()
        fp.write_data()

    except (ValueError, Exception) as e:
        root.critical(e)
        sys.exit(1)
    else:
        sys.exit(0)
    
    
