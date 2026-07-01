/*
 * This file is part of Discount. Copyright (c) 2019-2026 Johan Nyström-Persson.
 *
 *  Discount is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Discount is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Discount.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.jnpersson

import com.jnpersson.discount.spark.DiscountConf
import com.jnpersson.kmers.{MinimizerFormats, RandomXORFormat, SparkConfiguration, SplitterFormat, StandardFormat}
import com.jnpersson.kmers.minimizer.{ExtendedFormat, ExtendedTable, MinSplitter, MinTable, RandomXOR}


/**
 * Root package for the Discount k-mer counter.
 */
package object discount {

  /** Minimizer formats supported by Discount. */
  object AllMinimizerFormats extends MinimizerFormats[DiscountConf] {
    protected val formatsById = Map[String, SplitterFormat[_]](
      "standard" -> new StandardFormat(),
      "randomXOR" -> new RandomXORFormat(),
      "extended" -> new ExtendedFormat())

    protected val formatsByCls = Map[Class[_], SplitterFormat[_]](
      classOf[MinTable] -> new StandardFormat(),
      classOf[RandomXOR] -> new RandomXORFormat(),
      classOf[ExtendedTable] -> new ExtendedFormat())

    def makeSplitter(config: DiscountConf): MinSplitter[_] = {
      config.minimizerConfig().getSplitter(None, None)
    }
  }

}
