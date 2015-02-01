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

/**
 * A simple (and relatively robust) numerical method for the evaluation 
 * of the Gaussian hypergeometric function: continue adding terms to the 
 * series until "uj" is less than "machine epsilon" (the smallest number 
 * that a specific computer recognizes as being bigger than zero).
 */
object Gauss {
  
  def apply(a:Double,b:Double,c:Double,z:Double):Double = {
		
    if (Math.abs(z) >= 1) return Double.NaN
    if (a <= 0 | b <= 0 | c <= 0 | z <= 0) return Double.NaN
	
    val EPS:Double = 1E-13
    
	var	j:Double  = 0
    var uj:Double = 1
    
	var y:Double = uj
    var diff:Double = 1
		
    while (diff > EPS) {
			
      var last_y:Double = y
	  j += 1
			
	  uj = uj*(a+j-1)*(b+j-1)*z / ((c+j-1) * j)
	  y += uj
		
      diff = Math.abs(last_y - y)
	
    }
	
    y
    
  }

}