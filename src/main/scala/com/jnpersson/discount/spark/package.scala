/*
 * This file is part of Discount. Copyright (c) 2019-2025 Johan Nyström-Persson.
 *
 * Discount is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Discount is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson.discount

import com.jnpersson.kmers.minimizer._
import org.apache.spark.sql.{Encoder, Encoders}

package object spark {

  /** Spark encoders */
  object SparkEncoders {
    private type MC[P <: MinimizerPriorities] = MinSplitter[CanonicalPriorities[P]]

    /** Obtain a Spark Encoder for a splitter.
     * This is necessary because splitters are polymorphic and Spark does not have built-in encoders for them.
     */
    def encoder[S <: MinSplitter[_]](spl: S): Encoder[S] = synchronized {
      spl.priorities match {
        case CanonicalPriorities(inner) => inner match {
          case _: MinTable => Encoders.product[MC[MinTable]].asInstanceOf[Encoder[S]]
          case _: RandomXOR => Encoders.product[MC[RandomXOR]].asInstanceOf[Encoder[S]]
          case _: ExtendedTable => Encoders.product[MC[ExtendedTable]].asInstanceOf[Encoder[S]]
        }

        case _: MinTable => Encoders.product[MinSplitter[MinTable]].asInstanceOf[Encoder[S]]
        case _: RandomXOR => Encoders.product[MinSplitter[RandomXOR]].asInstanceOf[Encoder[S]]
        case _: ExtendedTable => Encoders.product[MinSplitter[ExtendedTable]].asInstanceOf[Encoder[S]]
      }
    }
  }
}
