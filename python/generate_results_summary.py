#!/usr/bin/env python

from clusterlib.scheduler import submit
import os

## constants -- copied from tanya's original script
vcf = "/net/snowwhite/home/welchr/projects/FFA/metsim_got2d_exomechip.json"
got2dLdClumpOptions = "  --build hg19 --ld-clump --clump-p 5e-7 --clump-ld-thresh .1 --ld-clump-source %s --ld-gwas-source %s --gwas-cat fusion" % (vcf, vcf)
dataPath = "/net/snowwhite/home/teslo/ExomeChip/AminoAcids"
swissPath = "/usr/cluster/boehnke/bin/welchr/swiss/bin/swiss"

def process_epacts(aminoAcid, chrm, inputFile, outFile):
  '''
  Construct a swiss command line for a given trait and chromosome.

  Execute this on the cluster.
  '''
  virtualEnv = 'source /net/snowwhite/home/trubetsk/python/my_python/bin/activate; '
  ## construct swiss command for each trait
  swissCmd = virtualEnv + swissPath + \
              ' --assoc %s' % inputFile + \
              ' --multi-assoc' + \
              ' --trait ln_%s_agebmi_inv' % aminoAcid + \
              ' --snp-col MARKER_ID' + \
              ' --chrom-col \#CHROM' + \
              ' --pos-col BEG' + \
              ' --pval-col ln_%s_agebmi_inv.P' % aminoAcid + \
              ' --out %s' % outFile + \
              got2dLdClumpOptions
  return swissCmd


## run swiss on merged results
def merge_results():
  '''
  rename markers and cat swiss outputs together
  '''
  pass


def _cleanup_swiss():
  '''
  remove unnecessary files after swiss run
  '''
  pass

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
  chrm = '22'
  dataSet = '1000Gexomechip'
  for aa in ['Ala', 'Gln', 'Gly', 'His', 'Ile', 'Leu', 'Phe', 'Tyr', 'Val']: 
    epactsFile = '/net/snowwhite/home/trubetsk/projects/amino_acids/data/1000Gexomechip/epacts_multi.epacts.chr%s.gz' % chrm
    resultsFile = 'data/test/%s_%s_%s.out' % (dataSet, chrm, aa)
    swissCmd = process_epacts(aminoAcid=aa, chrm = chrm, inputFile = epactsFile, outFile = resultsFile)
    script = submit('sbatch ' + swissCmd,
                    job_name="%s_%s_%s" % (dataSet, chrm, aa),
                    log_directory=os.path.dirname(resultsFile))
    os.system(script)

  ## merge and process swiss output

  ## run swiss again


if __name__ == '__main__':
  __main__()

