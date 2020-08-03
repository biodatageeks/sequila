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

import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, TypeCoercion, UnresolvedTableValuedFunction}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, _}
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
      /* pileup(tableName, sampleId, refPath) */
      tvf("table" -> StringType, "sampleId" -> StringType, "refPath" -> StringType)
      { case Seq(table: Any, sampleId: Any, refPath: Any) =>
        PileupTemplate(table.toString, sampleId.toString, refPath.toString, qual = false)
      },
      /* pileup(tableName, sampleId, refPath, baseQual) */
      tvf("table" -> StringType, "sampleId" -> StringType, "refPath" -> StringType, "qual"->BooleanType)
      { case Seq(table: Any, sampleId: Any, refPath: Any, qual:Any) =>
        PileupTemplate(table.toString, sampleId.toString, refPath.toString, qual.toString.toBoolean)
      }
    ),

    "bdg_coverage" -> Map(
      /* coverage(tableName) */
      tvf("table" -> StringType, "sampleId" -> StringType, "result" -> StringType)
      { case Seq(table: Any,sampleId:Any, result:Any) =>
        BDGCoverage(table.toString,sampleId.toString,result.toString, None)
      },
      /* coverage(tableName) */
      tvf("table" -> StringType, "sampleId" -> StringType, "result" -> StringType, "target" -> StringType)
      { case Seq(table: Any,sampleId:Any, result:Any, target: Any) =>
        BDGCoverage(table.toString,sampleId.toString,result.toString, Some(target.toString))
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




object BDGCoverage {
  def apply(tableName:String, sampleId:String, result: String, target: Option[String]): BDGCoverage = {

    val output = StructType(Seq(
      StructField(Columns.CONTIG,StringType,nullable = true),
      StructField(Columns.START,IntegerType,nullable = false),
      StructField(Columns.END,IntegerType,nullable = false),
      target match {
        case Some(t) =>  StructField(Columns.COVERAGE,FloatType,nullable = false)
        case None =>     StructField(Columns.COVERAGE,ShortType,nullable = false)
   })).toAttributes

    new BDGCoverage(tableName:String,sampleId.toString, result, target, output)
  }

}

case class BDGCoverage(tableName:String, sampleId:String, result: String, target:Option[String],
                       output: Seq[Attribute])
  extends LeafNode with MultiInstanceRelation {


  val numElements: BigInt = 1

  def toSQL(): String = {

    s"SELECT contigName,start,end,coverage AS `${output.head.name}` FROM bdg_coverage('$tableName')"
  }

  override def newInstance(): BDGCoverage = copy(output = output.map(_.newInstance()))

  def computeStats(conf: SQLConf): Statistics = {
    val sizeInBytes = LongType.defaultSize * numElements
    Statistics( sizeInBytes = sizeInBytes )
  }

  override def simpleString: String = {
    s"BDGCoverage ('$tableName')"
  }
}

object PileupTemplate {

  def apply(table:String, sampleId: String, refPath: String, qual: Boolean): PileupTemplate = {

    val basicOutput = StructType(Seq(
      StructField(Columns.CONTIG,StringType,nullable = true),
      StructField(Columns.START,IntegerType,nullable = false),
      StructField(Columns.END,IntegerType,nullable = false),
      StructField(Columns.REF,StringType,nullable = false),
      StructField(Columns.COVERAGE,ShortType,nullable = false),
      StructField(Columns.COUNT_REF,ShortType,nullable = false),
      StructField(Columns.COUNT_NONREF,ShortType,nullable = false),
      StructField(Columns.ALTS,MapType(ByteType,ShortType),nullable = true)
    ))

    val output = if (!qual)
                  basicOutput
                else
                  basicOutput
                    .add(StructField(Columns.QUALS,MapType(ByteType,ArrayType(ShortType)),nullable = true))

    val outputAttrs = output.toAttributes

    new PileupTemplate(table, sampleId, refPath, qual, outputAttrs)
  }
}

case class PileupTemplate(tableName: String, sampleId: String, refPath: String, qual:Boolean, output: Seq[Attribute] )
  extends LeafNode with MultiInstanceRelation {

  override def newInstance(): PileupTemplate = copy(output = output.map(_.newInstance()))

  def toSQL(): String = {

    s"""
      SELECT ${Columns.CONTIG}, ${Columns.START}, ${Columns.END}, ${Columns.REF}, ${Columns.COVERAGE}
      AS `${output.head.name}`
      FROM pileup('$tableName')"""
  }

  override def simpleString: String = {
    s"PileupFunction ('$tableName')"
  }
}