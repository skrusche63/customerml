package de.kp.insight.util
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Shopify-Insight project
* (https://github.com/skrusche63/shopify-insight).
* 
* Shopify-Insight is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Shopify-Insight is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Shopify-Insight. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

object CosineSimilarity {
  /**
   * Length of input arrays must be equal; due to performance
   * issues, we do not check this requirement
   */
  def compute(x:Array[Double],y:Array[Double]):Double = {
//    require(x.size == y.size)
    dotProduct(x, y) / (magnitude(x) * magnitude(y))
  }
  
  private def dotProduct(x: Array[Double], y: Array[Double]): Double = {
    x.zip(y).map(v => v._1 * v._2).sum
  }
  
  private def magnitude(x: Array[Double]): Double = {
    math.sqrt(x.map(i => i*i).sum)
  }

}