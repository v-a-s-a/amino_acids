#!/usr/bin/env python

from clusterlib.scheduler import submit
import os
import pandas as pd
import yaml
from processing_functions import *


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
  ## load variables
  parse_config('/net/snowwhite/home/trubetsk/projects/amino_acids/analyses/1000Gexomechip.yml')

  ## merge the traits and record the output
  aminoAcidFrames = [ merge_results('/net/snowwhite/home/trubetsk/projects/amino_acids/data/%s/swiss/' % dataSet, aa) for aa in config['studyAcids'] ]
  final = pd.concat(aminoAcidFrames)
  final.to_csv('/net/snowwhite/home/trubetsk/projects/amino_acids/results/%s/all_associated_markers.txt')

if __name__ == '__main__':
  __main__()

