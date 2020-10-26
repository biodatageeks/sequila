#!/usr/bin/env bash

export REF_FILE=/Users/aga/workplace/sequila/src/test/resources/reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta


## SAMTOOLS
# -x ignore overlaps
# -B Disable base alignment quality (BAQ) computation
# -A (--count-orphans) Do not skip anomalous read pairs in variant calling.
# -q 0 base quality > 0
# -Q 0 maping quality > 0

samtools mpileup --fasta-ref $REF_FILE -B -x  -A -q 0 -Q 0  NA12878.multichrom.md.bam > samtools.csv


## GATK
# run on cdh00. GATK complains about not complete fasta reference file

#export REF_PATH=/Users/aga/workplace/sequila/src/test/resources/reference
#export BAM_PATH=/Users/aga/workplace/sequila/src/test/resources/multichrom/mdbam/

# 1. generate reference dict file
export REF_PATH=/data/work/projects/pileup/data
docker run --rm -it --entrypoint="java" -v $REF_PATH:/data broadinstitute/picard -jar /usr/picard/picard.jar CreateSequenceDictionary R=/data/Homo_sapiens_assembly18.fasta O=/data/Homo_sapiens_assembly18.dict

#docker run --rm -it --entrypoint="java" -v $REF_PATH:/data broadinstitute/picard -jar /usr/picard/picard.jar CreateSequenceDictionary R=/data/Homo_sapiens_assembly18_chr1_chrM.small.fasta O=/data/Homo_sapiens_assembly18_chr1_chrM.small.dict

# 2. calculate pileup
export BAM_PATH=BAM_PATH=/data/work/projects/pileup/data/data2/slice
docker run --rm -it -v $REF_PATH:/ref -v $BAM_PATH:/data broadinstitute/gatk gatk Pileup -R /ref/Homo_sapiens_assembly18.fast -I /data/NA12878.multichrom.md.bam -O /data/gatk.csv
