package com.jnpersson.kmers

import com.jnpersson.kmers.minimizer.{MinSplitter, MinimizerPriorities}

/** Management of the set of minimizer formats supported by an application.
 * Allows for constructing a MinSplitter from a configuration, and for looking up SplitterFormat by
 * id (string) or by class.
 * @tparam C Supported configuration type
 */
trait MinimizerFormats[C <: MinimizerCLIConf] {
  protected def formatsById: Map[String, SplitterFormat[_]]
  protected def formatsByCls: Map[Class[_], SplitterFormat[_]]

  /** Make a MinSplitter from a configuration. */
  def makeSplitter(config: C): MinSplitter[_]

  /** Obtain a previously registered SplitterFormat by id */
  def getFormat(id: String): SplitterFormat[_] = synchronized {
    formatsById.getOrElse(id, throw new Exception(s"No such format $id"))
  }

  /** Obtain a previously registered SplitterFormat by class */
  def getFormat[P <: MinimizerPriorities](cls: Class[_ <: P]): SplitterFormat[P] = synchronized {
    formatsByCls.getOrElse(cls, throw new Exception(s"No format for class $cls")).asInstanceOf[SplitterFormat[P]]
  }

  def getFormat[P <: MinimizerPriorities](p: P): SplitterFormat[P] =
    getFormat(p.getClass)
}

