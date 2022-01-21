package org.apache.spark.sql


/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.Locale
import org.apache.spark.sql.ResolveTableValuedFunctionsSeq.tvf
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, TypeCoercion, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, _}
import org.biodatageeks.sequila.flagStat.FlagStat
import org.biodatageeks.sequila.pileup.conf.QualityConstants
import org.biodatageeks.sequila.utils.Columns

/**
  * Rule that resolves table-valued function references.
  */
object ResolveTableValuedFunctionsSeq extends Rule[LogicalPlan] {
  /**
    * List of argument names and their types, used to declare a function.
    */
  private case class ArgumentList(args: (String, DataType)*) {
    /**
      * Try to cast the expressions to satisfy the expected types of this argument list. If there
      * are any types that cannot be casted, then None is returned.
      */
    def implicitCast(values: Seq[Expression]): Option[Seq[Expression]] = {
      if (args.length == values.length) {
        val casted = values.zip(args).map { case (value, (_, expectedType)) =>
          TypeCoercion.ImplicitTypeCasts.implicitCast(value, expectedType)
        }
        if (casted.forall(_.isDefined)) {
          return Some(casted.map(_.get))
        }
      }
      None
    }

    override def toString: String = {
      args.map { a =>
        s"${a._1}: ${a._2.typeName}"
      }.mkString(", ")
    }
  }

  /**
    * A TVF maps argument lists to resolver functions that accept those arguments. Using a map
    * here allows for function overloading.
    */
  private type TVF = Map[ArgumentList, Seq[Any] => LogicalPlan]

  /**
    * TVF builder.
    */
  private def tvf(args: (String, DataType)*)(pf: PartialFunction[Seq[Any], LogicalPlan])
  : (ArgumentList, Seq[Any] => LogicalPlan) = {
    (ArgumentList(args: _*),
      pf orElse {
        case args =>
          throw new IllegalArgumentException(
            "Invalid arguments for resolved function: " + args.mkString(", "))
      })
  }

  /**
    * Internal registry of table-valued functions.
    */
  private val builtinFunctions: Map[String, TVF] = Map(
    "pileup" -> Map(
      /* pileup(tableName, sampleId, refPath)  COVERAGE ONLY */
      tvf("table" -> StringType, "sampleId" -> StringType, "refPath" -> StringType)
      { case Seq(table: Any, sampleId: Any, refPath: Any) =>
        PileupTemplate(table.toString, sampleId.toString, refPath.toString, false, false, None)
      },
      /* pileup(tableName, sampleId, refPath, alts) COVERAGE + ALTS  */
      tvf("table" -> StringType, "sampleId" -> StringType, "refPath" -> StringType, "alts"->BooleanType)
      { case Seq(table: Any, sampleId: Any, refPath: Any, alts:Any) =>
        PileupTemplate(table.toString, sampleId.toString, refPath.toString, alts.toString.toBoolean, false, None)
      },
      /* pileup(tableName, sampleId, refPath, alts, quals ) COVERAGE + ALTS + QUALS */
      tvf("table" -> StringType, "sampleId" -> StringType, "refPath" -> StringType, "alts"->BooleanType, "quals"->BooleanType)
      { case Seq(table: Any, sampleId: Any, refPath: Any, alts:Any, quals:Any) =>
        PileupTemplate(table.toString, sampleId.toString, refPath.toString, alts.toString.toBoolean, quals.toString.toBoolean, None)
      },
      /* pileup(tableName, sampleId, refPath, alts, quals, binSize ) COVERAGE + ALTS + QUALS + BINNING */
      tvf("table" -> StringType, "sampleId" -> StringType, "refPath" -> StringType, "alts"->BooleanType, "quals"->BooleanType, "binSize"->IntegerType)
      { case Seq(table: Any, sampleId: Any, refPath: Any, alts:Any, quals:Any, binSize:Any) =>
        PileupTemplate(table.toString, sampleId.toString, refPath.toString, alts.toString.toBoolean, quals.toString.toBoolean, Some(binSize.toString.toInt))
      }
    ),

    "flagstat" -> Map(
      /* flagStat(tableName) */
      tvf("tableName" -> StringType)
      { case Seq(tableName: Any) =>
        FlagStatTemplate(tableName.toString, null)
      },
      /* flagStat(tableNameOrPath, sampleId) */
      tvf("tableNameOrPath" -> StringType, "sampleId" -> StringType)
      { case Seq(tableNameOrPath: Any, sampleId: Any) =>
        FlagStatTemplate(tableNameOrPath.toString, sampleId.toString)
      }
    ),

    "coverage" -> Map(
      /* coverage(tableName) */
      tvf("table" -> StringType, "sampleId" -> StringType, "refPath" -> StringType)
      { case Seq(table: Any, sampleId: Any, refPath: Any) =>
        PileupTemplate(table.toString, sampleId.toString, refPath.toString, false, false, None)
      }),
    "range" -> Map(
      /* range(end) */
      tvf("end" -> LongType) { case Seq(end: Long) =>
        Range(0, end, 1, None)
      }),

     "bdg_grange"-> Map(
       /* range(end) */
       tvf("contigName" -> StringType, "start" -> IntegerType, "end" -> IntegerType) { case Seq(contigName: Any, start: Int, end: Int) =>
         GenomicInterval(contigName.toString,start,end)
       })
  )

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case u: UnresolvedTableValuedFunction if u.functionArgs.forall(_.resolved) =>
      val resolvedFunc = builtinFunctions.get(u.functionName.toLowerCase(Locale.ROOT)) match {
        case Some(tvf) =>
          val resolved = tvf.flatMap { case (argList, resolver) =>
            argList.implicitCast(u.functionArgs) match {
              case Some(casted) =>
                Some(resolver(casted.map(_.eval())))
              case _ =>
                None
            }
          }
          resolved.headOption.getOrElse {
            val argTypes = u.functionArgs.map(_.dataType.typeName).mkString(", ")
            u.failAnalysis(
              s"""error: table-valued function ${u.functionName} with alternatives:
                 |${tvf.keys.map(_.toString).toSeq.sorted.map(x => s" ($x)").mkString("\n")}
                 |cannot be applied to: (${argTypes})""".stripMargin)
          }
        case _ =>
          u.failAnalysis(s"could not resolve `${u.functionName}` to a table-valued function")
      }

      // If alias names assigned, add `Project` with the aliases
      if (u.output.nonEmpty) {
        val outputAttrs = resolvedFunc.output
        // Checks if the number of the aliases is equal to expected one
        if (u.output.size != outputAttrs.size) {
          u.failAnalysis(s"Number of given aliases does not match number of output columns. " +
            s"Function name: ${u.functionName}; number of aliases: " +
            s"${u.output.size}; number of output columns: ${outputAttrs.size}.")
        }
        val aliases = outputAttrs.zip(u.output).map {
          case (attr, name) => Alias(attr, name.toString())()
        }
        Project(aliases, resolvedFunc)
      } else {
        resolvedFunc
      }
  }
}

case class FlagStatTemplate(tableNameOrPath: String, sampleId: String, output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {

  override def newInstance(): FlagStatTemplate = copy(output = output.map(_.newInstance()))

  def toSQL(): String = {
    s"""
          SELECT RCount, QCFail, DUPES, MAPPED, UNMAPPED, PiSEQ, Read1, Read2, PPAired, WIaMM, Singletons
          AS `${output.head.name}`
          FROM flagStat('$tableNameOrPath')"""
  }

  override def toString: String = {
    s"FlagStatFunction ('$tableNameOrPath')"
  }
}

object FlagStatTemplate {
  private def output() = {
    FlagStat.Schema.toAttributes;
  }

  def apply(tableNameOrPath: String, sampleId: String) = {
    new FlagStatTemplate(tableNameOrPath, sampleId, output());
  }
}




object PileupTemplate {


  private def output(alts:Boolean, quals: Boolean) ={

    val basicOutput = StructType(Seq(
      StructField(Columns.CONTIG,StringType,nullable = true),
      StructField(Columns.START,IntegerType,nullable = false),
      StructField(Columns.END,IntegerType,nullable = false),
      StructField(Columns.REF,StringType,nullable = false),
      StructField(Columns.COVERAGE,ShortType,nullable = false)
    ))

    val output = if (!quals && !alts)
      basicOutput
    else if (!quals && alts)
      basicOutput
        .add(StructField(Columns.COUNT_REF,ShortType,nullable = false))
        .add(StructField(Columns.COUNT_NONREF,ShortType,nullable = false))
        .add(StructField(Columns.ALTS,MapType(ByteType,ShortType),nullable = true))
    else
      basicOutput
        .add(StructField(Columns.COUNT_REF,ShortType,nullable = false))
        .add(StructField(Columns.COUNT_NONREF,ShortType,nullable = false))
        .add(StructField(Columns.ALTS,MapType(ByteType,ShortType),nullable = true))
        .add(StructField(Columns.QUALS,MapType(IntegerType,ArrayType(ShortType)),nullable = true))

    output
  }

  def apply(table:String, sampleId: String, refPath: String, alts:Boolean, quals: Boolean, binSize: Option[Int]): PileupTemplate = {
    val outputAttrs = output(alts, quals).toAttributes
    new PileupTemplate(table, sampleId, refPath, alts, quals, binSize, outputAttrs)
  }

  def apply(path: String, refPath: String, alts: Boolean, quals: Boolean) = {
    val outputAttrs = output(alts, quals).toAttributes
    new PileupTemplate(path, null, refPath, alts, quals, None, outputAttrs )
  }
}

case class PileupTemplate(tableNameOrPath: String,
                          sampleId: String,
                          refPath: String,
                          alts: Boolean,
                          quals:Boolean,
                          binSize: Option[Int],
                          output: Seq[Attribute] )
  extends LeafNode with MultiInstanceRelation {


  override def newInstance(): PileupTemplate = copy(output = output.map(_.newInstance()))

  def toSQL(): String = {

    s"""
      SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.REF}, ${Columns.COVERAGE}
      AS `${output.head.name}`
      FROM pileup('$tableNameOrPath')"""
  }

  override def toString: String = {
    s"PileupFunction ('$tableNameOrPath')"
  }
}

case class Coverage(contig:String, pos_start:Int, pos_end: Int, ref: String, coverage:Short)

object Pileup{
  def apply(contig:String, pos_start:Int, pos_end: Int,
            ref: String, coverage:Short, countRef: Short, countNonRef: Short, alts: Map[Byte,Short]) : Pileup = {
    Pileup(contig, pos_start, pos_end,
      ref, coverage, countRef, countNonRef, alts, None)

  }
}
case class Pileup(contig:String, pos_start:Int, pos_end: Int,
                  ref: String, coverage:Short, countRef: Short, countNonRef: Short, alts: Map[Byte,Short], quals: Option[Map[Byte, Array[Short]]]=None)