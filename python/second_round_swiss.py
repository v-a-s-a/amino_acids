#!/usr/bin/env python

from clusterlib.scheduler import submit
import os
import pandas as pd
import yaml



## Parse analysis configuration file 
config = yaml.load(open('/net/snowwhite/home/trubetsk/projects/amino_acids/analyses/1000Gexomechip.yml'))
config['swissOptions'] = [ s % config['vcf'] if '%' in s else s for s in config['swissOptions']]
config['traits'] = [  config['trait'] % aa for aa in config['studyAcids'] ]
config['swissDir'] = config['swissDir'] % config['dataSet']

def process_epacts(aminoAcid, inputFile, outFile, additionalOpts=[], config = None):
  '''
  Construct a swiss command line for a given trait and chromosome.

  Make sure to pass in extra arguments via additionalOpts list: 
    --multi-assoc
    --pval-col {column name}

  Execute this on the cluster.
  '''
  virtualEnv = 'source /net/snowwhite/home/trubetsk/python/my_python/bin/activate; '
  ## construct swiss command for each trait
  swissCmd = virtualEnv + config['swissPath'] + \
              ' --assoc %s' % inputFile + \
              ' --snp-col MARKER_ID' + \
              ' --chrom-col \#CHROM' + \
              ' --pos-col BEG' + \
              ' --out %s ' % outFile
  swissCmd = swissCmd + ' ' + reduce(lambda x, y: x+' '+y, config['swissOptions'])
  swissCmd = swissCmd + ' ' + reduce(lambda x, y: x+' '+y, additionalOpts)
  return swissCmd


## run swiss on merged results
def merge_results(direc, aminoAcid):
  '''
  rename markers and cat swiss outputs together
  '''
  ## get clump files for all chromosomes for a trait (if they exist)
  for root, dirs, files in  os.walk(direc):
    clumpFiles = [ root+x for x in files if (x.endswith('clump') and (aminoAcid in x)) ]
  ## read into data frames
  frames = [ pd.read_csv(x, sep='\t') for x in clumpFiles ]


  ## rename markers with trailing _{aminoAcid}
  def rename_markers(df, aa):
    df.MARKER_ID = df.MARKER_ID + '_' + aa
    return df
  modFrames = map(lambda x: rename_markers(x, aminoAcid), frames)
  ## merge into single data frame, and subset to columns of interest
  chrmFrame = pd.concat(modFrames)
  return chrmFrame
  


def _cleanup_swiss(direc):
  '''
  Remove SLURM and swiss log files.
  '''
  for root, dirs, files in os.walk(direc):
    logs = [ root+x for x in files if x.endswith('.txt') or x.endswith('.log') ]
    for log in logs: os.remove(log)

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

  ### merge the traits and record the output
  #aminoAcidFrames = [ merge_results('/net/snowwhite/home/trubetsk/projects/amino_acids/data/%s/swiss/' % dataSet, aa) for aa in config['studyAcids'] ]
  #final = pd.concat(aminoAcidFrames)
  #final.to_csv('/net/snowwhite/home/trubetsk/projects/amino_acids/results/%s/all_associated_markers.txt')

if __name__ == '__main__':
  __main__()

