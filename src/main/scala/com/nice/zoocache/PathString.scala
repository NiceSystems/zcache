package com.nice.zoocache

/**
 * User: arnonrgo
 * Date: 1/14/13
 * Time: 9:40 AM
 */
object PathString{
  implicit def extendString(s : String) = new PathString(s)
  implicit def deflateString(s: PathString)=s.toString()
}

class PathString(private val s: String) {
  private val str= if (s.startsWith("/")) s else "/"+s

  def :>( that: String) :String ={
    ensure(this) + clean(that)
  }
  def :/(that: String) : String={
    this :> that + "/"
  }

  private def clean(s:String): String ={
    if (s.startsWith("/")) s.substring(1,s.length) else s
  }
  private def ensure(ms: PathString) ={
    if (!ms.str.endsWith("/")) ms.str + "/" else ms.s
  }

  override def toString = str
  def noPath = str.replace("/","")
}

