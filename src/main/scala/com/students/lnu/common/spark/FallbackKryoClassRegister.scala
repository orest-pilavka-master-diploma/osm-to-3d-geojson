package com.students.lnu.common.spark

object FallbackKryoClassRegister extends KryoClassRegister {
  def classes: Array[Class[_]] = Array(
//        classOf[ClassName],
//        Class.forName("scala.Enumeration$Val"),
  )
}
