package com.nice.zoocache
import java.lang.reflect.{ Type => JType, Array => _, _ }
import scala.reflect.Manifest.{ classType, intersectionType, arrayType, wildcardType }
/**
 * User: arnonrgo
 * Date: 12/30/12
 * Time: 11:25 AM
 */
object TypeToManifest {

    def intersect(tps: JType*): Manifest[_] = intersectionType(tps map javaType: _*)
    def javaType(tp: JType): Manifest[_] =
      tp match {
      case x: Class[_] => classType(x)
      case x: ParameterizedType =>
        val owner = x.getOwnerType
        val raw = x.getRawType() match { case clazz: Class[_] => clazz }
        val targs = x.getActualTypeArguments() map javaType
        (owner == null, targs.isEmpty) match {
          case (true, true) => javaType(raw)
          case (true, false) => classType(raw, targs.head, targs.tail: _*)
          case (false, _) => classType(javaType(owner), raw, targs: _*)
        }
      case x: GenericArrayType => arrayType(javaType(x.getGenericComponentType))
      case x: WildcardType => wildcardType(intersect(x.getLowerBounds: _*), intersect(x.getUpperBounds: _*))
      case x: TypeVariable[_] => intersect(x.getBounds(): _*)
}

}
