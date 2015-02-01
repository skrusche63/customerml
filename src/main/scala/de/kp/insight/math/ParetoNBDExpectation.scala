package de.kp.insight.math
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

object ParetoNBDExpectation extends Serializable {
 
  def apply(r:Double,alpha:Double,s:Double,beta:Double,x:Double,tx:Double,T:Double, t:Double):Double = {

    val tmp1 = (r+x) * (beta+T) / ((alpha+T) * (s-1))
	val tmp2 = Math.pow((beta+T) / (beta+T+t),s-1)
	
    val ce = tmp1 * (1-tmp2) * ParetoNBDAlive(r,alpha,s,beta,x,tx,T)
	ce
	
  }

}