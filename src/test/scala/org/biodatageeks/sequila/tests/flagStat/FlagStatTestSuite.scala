package org.biodatageeks.sequila.tests.flagStat

import org.apache.avro.io.Encoder
import org.apache.spark.sql.{DataFrame, SequilaSession}
import org.biodatageeks.sequila.flagStat.FlagStatRow
import org.biodatageeks.sequila.utils.Columns
import org.biodatageeks.sequila.tests.flagStat.FlagStatTestBase;

class FlagStatTestSuite extends FlagStatTestBase {
  test("Against a stable implementation (samtools)") {
    val result = ss.sql(flagStatQuery);
    performAssertions(result);
  }

  val Expected = Map[String, Long](
    "RCount" -> 22607,
    "QCFail" -> 0,
    "DUPES" -> 1532,
    "MAPPED" -> 22277,
    "UNMAPPED" -> 330,
    "PiSEQ" -> 22607,
    "Read1" -> 11309,
    "Read2" -> 11298,
    "PPaired" -> 21647,
    "WIaMM" -> 21924,
    "Singletons" -> 353
  );
  private def performAssertions(df:DataFrame):Unit ={
    assert(df.count() == 1);

    //val query = Expected.map(data => s"${data._1} == ${data._2}").mkString(" and ");
    //val merge = result.where(query);
    //assert(merge.count(), 1);

    val obtained = df.where("RCount > 0");
    assert(obtained.count(), 1);
    val results = obtained.first();
    Expected.foreach(kv => {
      println(s"Checking: ${kv._1} == ${kv._2}...");
      val index = results.fieldIndex(kv._1);
      assert(results.getLong(index), kv._2);
    })
  }
}
