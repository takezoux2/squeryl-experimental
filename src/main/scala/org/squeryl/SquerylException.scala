package org.squeryl

/**
 * Created by IntelliJ IDEA.
 * User: takezou
 * Date: 11/09/04
 * Time: 21:45
 * To change this template use File | Settings | File Templates.
 */

class SquerylException(message : String , e : Throwable) extends Exception(message,e){

  def this(message: String) = this(message,null)

}