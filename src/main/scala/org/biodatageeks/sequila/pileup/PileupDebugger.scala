package org.biodatageeks.sequila.pileup

object PileupDebugger {

}

//        val conf = new Configuration()
//        val schema = s"struct<${Columns.CONTIG}:string,${Columns.START}:int,${Columns.END}:int,${Columns.REF}:String,${Columns.COVERAGE}:smallint>"
//        conf.set("orc.mapred.output.schema", schema )
//        conf.set("orc.compress", "SNAPPY")
//        aggregates
//          .toCoverageFastMode(bounds)
//        .saveAsNewAPIHadoopFile(
//          "/tmp/test.orc",
//          classOf[NullWritable],
//          classOf[OrcStruct],
//          classOf[OrcOutputFormat[OrcStruct]],
//          conf
//        )