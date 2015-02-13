#!/usr/bin/env python

from clusterlib.scheduler import submit
import os
import pandas as pd
import yaml
from processing_functions import *



## Parse analysis configuration file 
config = yaml.load(open('/net/snowwhite/home/trubetsk/projects/amino_acids/analyses/1000Gexomechip.yml'))
config['swissOptions'] = [ s % config['vcf'] if '%' in s else s for s in config['swissOptions']]
config['traits'] = [  config['trait'] % aa for aa in config['studyAcids'] ]
config['swissDir'] = config['swissDir'] % config['dataSet']


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

  for chrm in [ str(c) for c in range(1, 23) ]:
    for aa in config['studyAcids']: 
      epactsFile =  config['epactsFile'] % chrm
      resultsFile = '/net/snowwhite/home/trubetsk/projects/amino_acids/data/%s/swiss/%s_%s_%s.out' % (config['dataSet'], config['dataSet'], chrm, aa)
      swissCmd = process_epacts(aminoAcid=aa,
                                inputFile = epactsFile,
                                outFile = resultsFile,
                                additionalOpts=['--multi-assoc',
                                                '--pval-col ln_%s_agebmi_inv.P' % aa,
                                                '--trait ln_%s_agebmi_inv' % aa],
                                config = config)
      #os.system(swissCmd)
      script = submit('sbatch ' + swissCmd,
                      job_name="r1_%s_%s_%s" % (config['dataSet'], chrm, aa),
                      log_directory=os.path.dirname(resultsFile))
      #os.system(script)

if __name__ == '__main__':
  __main__()

