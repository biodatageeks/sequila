package org.biodatageeks.sequila.rangejoins.exp.iitii

case class TreeMetadata(K: Int, arraySizeIncludingImaginaryNodes: Int)

object TreeMetadataRetriever {

  def resolveKAndArraySizeIncludingImaginaryNodes(numberOfElems: Int): TreeMetadata = {
    var levels = 0
    var counter: Int = 1
    while (counter <= numberOfElems) {
      counter *= 2
      levels += 1
    }
    TreeMetadata(levels - 1, counter - 1)
  }
}
