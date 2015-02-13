#!/usr/bin/env python

def parse_config(configFile):
  '''
  Parse the analysis configuration file, and set up variables.
  '''
  ## Parse analysis configuration file 
  config = yaml.load(open(configFile))
  config['swissOptions'] = [ s % config['vcf'] if '%' in s else s for s in config['swissOptions']]
  config['traits'] = [  config['trait'] % aa for aa in config['studyAcids'] ]
  config['swissDir'] = config['swissDir'] % config['dataSet']
  print "Running analysis on {0} data.".format(config['dataSet'])


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




