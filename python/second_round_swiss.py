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


  ## merge swiss output across chromosomes for each amino acid
  genomeFrames = [ merge_results('/net/snowwhite/home/trubetsk/projects/amino_acids/data/%s/swiss/' % config['dataSet'], aa) for aa in config['studyAcids'] ]
  firstRound = pd.concat(genomeFrames)
  firstRoundFile = config['swissDir'] + '%s_firstRound_merged_swiss.txt' % config['dataSet']
  firstRound.to_csv(firstRoundFile, sep = '\t', index=False)

  ## run swiss again
  for aa in config['studyAcids']:
    secondRoundAAFile =  config['swissDir'] + '%s_secondRound_%s.txt' % (config['dataSet'], aa)
    swissCmd = process_epacts(aminoAcid = aa,
                              inputFile = firstRoundFile,
                              outFile = secondRoundAAFile,
                              additionalOpts = ["--pval-col PVALUE",
                                                "--trait TRAIT"],
                              config = config)
    os.system(swissCmd)
    #script = submit('sbatch ' + swissCmd,
    #                job_name="r2_%s_%s" % (config['dataSet'], aa),
    #                log_directory=os.path.dirname(secondRoundAAFile))
    #print script
    #os.system(script)


if __name__ == '__main__':
  __main__()

