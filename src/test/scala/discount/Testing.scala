package discount

import scala.collection.Seq
import discount.hash.MotifSpace

object Testing {

  val all2mersTestOrder = Seq("AT", "AG", "CT", "GG", "CC",
    "AC", "GT", "GA", "TC",
    "CG", "GC",
    "TG", "CA", "TA",
    "TT", "AA")

  val space = MotifSpace.using(all2mersTestOrder)
//
  def m(code: String, pos: Int) = space.get(code, pos)
  def ms(motifs: Seq[(String, Int)]) = motifs.map(x => m(x._1, x._2))
}