package org.biodatageeks.sequila.utils

object ScalaFuncs {

  import scala.reflect.runtime.universe._

  def classAccessors[T: TypeTag]: Seq[MethodSymbol] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }.toSeq.reverse

}
