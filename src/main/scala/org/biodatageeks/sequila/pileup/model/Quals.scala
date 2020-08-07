package org.biodatageeks.sequila.pileup.model

import org.biodatageeks.sequila.pileup.conf.QualityConstants
import org.biodatageeks.sequila.utils.FastMath

import scala.collection.mutable

object Quals {
  type SingleLocusQuals = mutable.HashMap[Byte, Array[Short]]
  val SingleLocusQuals = mutable.HashMap[Byte, Array[Short]] _

  type MultiLociQuals = mutable.IntMap[Quals.SingleLocusQuals]
  val MultiLociQuals = mutable.IntMap[Quals.SingleLocusQuals] _

  implicit class SingleLocusQualsExtension(val map: Quals.SingleLocusQuals) {
    def derivedCoverage:Short = map.map({case (k,v) => v.sum}).sum

    def totalEntries:Long = map.map({case (k,v) => 1}).sum

    def merge(mapOther: SingleLocusQuals): SingleLocusQuals = {

      val fastMerge = FastMath.merge(map, mapOther)
      if (fastMerge.isDefined)
        return FastMath.merge(map, mapOther).get.asInstanceOf[SingleLocusQuals]

      val keyset = map.keySet++mapOther.keySet
      val mergedMap = new SingleLocusQuals()
      for (k <- keyset)
        mergedMap(k) = addArrays(map.get(k), mapOther.get(k))
      mergedMap
    }

    def addArrays(arrOp1: Option[Array[Short]], arrOp2:Option[Array[Short]] ): Array[Short] ={
      if (arrOp1.isEmpty)
        return arrOp2.get
      if (arrOp2.isEmpty)
        return arrOp1.get

      val arr1 = arrOp1.get
      val arr2 = arrOp2.get

      if (arr1.length >= arr2.length) {
        for(ind <- arr2.indices)
          arr1(ind) = (arr1(ind) + arr2(ind)).toShort
        arr1
      }else {
        for(ind <- arr1.indices)
          arr2(ind) = (arr1(ind) + arr2(ind)).toShort
        arr2
      }

    }

    def trim: SingleLocusQuals = {
      map.map({case(k,v) => k->v.take(v(QualityConstants.MAX_QUAL_IND) + 1)})
    }

    def addQualityForAlt(alt: Char, quality: Byte, updateMax: Boolean=true):Unit ={
      val altByte = alt.toByte
      var array = map.getOrElse(altByte, new Array[Short](QualityConstants.QUAL_ARR_SIZE))
      val qualityIndex = quality
      if(updateMax ) {
        array(qualityIndex) = (array(qualityIndex) + 1).toShort
        if (qualityIndex > array(array.length - 1))
          array(array.length - 1) = qualityIndex.toShort
      } else {
        if (qualityIndex >= array.length){
          val extendedArray = new Array[Short](QualityConstants.QUAL_ARR_SIZE)
          System.arraycopy(array,0,extendedArray, 0, array.length)
          array = extendedArray
        }
        array(qualityIndex) = (array(qualityIndex) + 1).toShort
      }
      map.update(altByte,array)
    }

  }


  implicit class MultiLociQualsExtension (val map: Quals.MultiLociQuals) {
    def ++ (that: Quals.MultiLociQuals): Quals.MultiLociQuals = (map ++ that).asInstanceOf[Quals.MultiLociQuals]

    def trim: MultiLociQuals = map.map({case (k,v) => k-> v.trim})

    def  updateQuals(position: Int, alt: Char, quality: Byte, updateMax:Boolean = true): Unit = {

      val singleLocusQualMap = map.getOrElse(position, new SingleLocusQuals())
      singleLocusQualMap.addQualityForAlt(alt,quality, updateMax)
      map.update(position, singleLocusQualMap)
    }

    def merge(mapOther: MultiLociQuals): MultiLociQuals = {
      val fastMerge = FastMath.merge(map, mapOther)
      if (fastMerge.isDefined)
        return FastMath.merge(map, mapOther).get.asInstanceOf[MultiLociQuals]

      val keyset = map.keySet++mapOther.keySet
      var mergedQualsMap = new MultiLociQuals()
      for (k <- keyset)
        mergedQualsMap += k -> map.getOrElse(k, new SingleLocusQuals()).merge(mapOther.getOrElse(k, new SingleLocusQuals()))
      mergedQualsMap
    }

    def getTotalEntries: Long =  {
      map.map{case(k,v) => k -> map(k).totalEntries}.foldLeft(0L)(_ + _._2)
    }

    def getQualitiesCount: mutable.LongMap[Int] = {
      val res = new mutable.LongMap[Int]()
      map.map{ case(k,v) =>
        v.map{ case (kk,vv) =>
          for (index <- vv.indices by 2) {
            val item = vv(index)
            if(res.contains(item)) res.update(item, res(item)+1)
            else res.update(item, 1)
          }
        }
      }
      res
    }

  }
}
