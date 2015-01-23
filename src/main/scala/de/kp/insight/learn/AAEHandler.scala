package de.kp.insight.learn
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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.model._

class AAEHandler(ctx:RequestContext) {
  
  def get(params:Map[String,String]):Map[String,String] = null

  def train(params:Map[String,String]):Map[String,String] = {
    
    val base = ctx.getBase
    val name = params(Names.REQ_NAME)
    
    val uid = params(Names.REQ_UID)
    val url = String.format("""%s/%s/%s/1""", base, name, uid)
    
    val k = params.get("k") match {
      case None => 10.toString
      case Some(value) => value
    }
    
    val minconf = params.get("minconf") match {
      case None => 0.8.toString
      case Some(value) => value
    }
    
    val delta = params.get("delta") match {
      case None => 2.toString
      case Some(value) => value
    }
    
    params ++ Map(
        
      /* Algorithm specification */
      Names.REQ_ALGORITHM -> "TOPKNR",
      
      /*
       * The TOPKNR algorithm requires the following parameters:
       * 
       * a) k, b) minconf and c) delta
       * 
       * If these parameters are not provided, default values are
       * used here
       */
      "k" -> k,
      "minconf" -> minconf,
      "delta" -> delta,
      
      /* Data source description */
      Names.REQ_URL -> url,        
      Names.REQ_SOURCE -> "PARQUET"
        
    )
    
  }
  
}