package org.biodatageeks.sequila.utils

object FileFuncs {

  def getFileExtension (path: String) = path.split('.').last.toLowerCase

}
