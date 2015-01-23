#!/usr/bin/env python

import argparse as arg


def __main__():
  '''
  Summarize association results by reporting the most-significant, independent
  associations, and multiply associated SNPs. This script should be able to 
  consume EPACTS output, and generate swiss output that described below.

  Input:
    EPACTS multiple trait file

  Pipeline:
  1. Run swiss for each trait (by chromosome)
  2. Rename most significant markers with associated trait
  3. Run swiss on most significant markers for all traits

  Result:
  The result is a Swiss generated output that contains the most significant
  trait association at a locus, and a list of secondary (but still 
  significant) associations.
  '''
  ## parse inputs
  parser = arg.ArgumentParser()
  parse.add_argument("--epacts-results", dest="epactsRes")
  parse.add_argument("--out", dest="outFile")
  args = parse.parse_args()

  

  ## run swiss for each trait
  
  
  
  ## 


if __name__ == '__main__':
  __main__()

