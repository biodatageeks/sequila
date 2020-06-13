#bam to cram conversion
samtools view -C ../mdbam/NA12878.multichrom.md.bam -T ../../reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta -o NA12878.multichrom.md.cram --output-fmt-option store_md=1
samtools index NA12878.multichrom.md.cram

#bam without MD
samtools view -C ../bam/NA12878.multichrom.bam -T ../../reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta -o NA12878.multichrom.cram
samtools index NA12878.multichrom.cram

#add MD tags to bam
samtools fillmd -@2 -bAr NA12878.multichrom.bam ../../reference/Homo_sapiens_assembly18_chr1_chrM.small.fasta > NA12878.multichrom.md.bam