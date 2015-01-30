#!/usr/bin/env python

import argparse as arg
import gzip as gz
import os

def __main__():
  '''
  Split A given EPACTS file into constituent chromosomes.
  '''
  parser = arg.ArgumentParser()
  parser.add_argument('-f')
  parser.add_argument('-d')
  args = parser.parse_args()

  ## read off header
  f = gz.open(args.f)
  header = f.next()

  ## initialize files for writing
  outFiles = {}
  for chrm in xrange(1, 23):
    outFiles[str(chrm)] = gz.open(args.d + os.path.basename(args.f).strip('.gz') + '.chr' + str(chrm) + '.gz', 'wb')
    outFiles[str(chrm)].write(header)
  
  ## direct read stream into chromosome specific files
  for line in f:
    chrm = line.strip().split()[0]
    outFiles[str(chrm)].write(line)

  ## close file connections
  for chrm in xrange(1, 23):
    outFiles[str(chrm)].close()

if __name__ == '__main__':
  __main__()
