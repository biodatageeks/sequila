val reads = spark.read.parquet("/data/granges/NA12878.hiseq.wgs.bwa.recal.adam")
reads.createOrReplaceTempView("reads")
val targets = spark.read.parquet("tgp_exome_hg18.adam")
targets.createOrReplaceTempView("targets")


    val reads = spark.read.parquet("/data/granges/NA12878.ga2.exome.maq.recal.adam")
    reads.createOrReplaceTempView("reads")
    val targets = spark.read.parquet("/data/granges/tgp_exome_hg18.adam")
    targets.createOrReplaceTempView("targets")


    SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
         ON (targets.contigName=reads.contigName
         AND
         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
         AND
         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
         )
         GROUP BY targets.contigName,targets.start,targets.end
         having contigName='chr1' AND    start=118996 AND  end=119116
     

chr1    118996  119116

SELECT * FROM reads JOIN targets
     ON (targets.contigName=reads.contigName
     AND
     CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
     AND
     CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
     )



SELECT targets.contigName,targets.start,targets.end,count(*) FROM reads JOIN targets
         ON (targets.contigName=reads.contigName
         AND
         CAST(reads.end AS INTEGER)>=CAST(targets.start AS INTEGER)
         AND
         CAST(reads.start AS INTEGER)<=CAST(targets.end AS INTEGER)
         )
         GROUP BY targets.contigName,targets.start,targets.end
         having contigName='chr1' AND    start=20138 AND  end=20294
