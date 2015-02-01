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

object ParetoNBDAlive extends Serializable {
  
  def apply(r:Double,alpha:Double,s:Double,beta:Double, x:Double, tx:Double, T:Double) = {

    val maxab = Math.max(alpha,beta)
	val absab = Math.abs(alpha-beta)
	
	val param1 = r+s+x
	val param2 = if (alpha < beta) r + x else s+1
	
	val F0 = Math.pow(alpha + T , r + x)*Math.pow(beta + T,s)

	val F1 = Gauss(param1, param2, param1+1, absab / (maxab+tx)) / (Math.pow(maxab+tx,param1))
	val F2 = Gauss(param1, param2, param1+1, absab / (maxab+T)) / (Math.pow(maxab+T,param1))
		
	val alive = 1 / (1 + (s / param1) * F0 * (F1-F2))
    alive
	
  }

}