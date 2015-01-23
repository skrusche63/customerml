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

import scala.collection.mutable.Buffer

class IREHandler(ctx:RequestContext) {

  def train(params:Map[String,String]):Map[String,String] = {
    
    val base = ctx.getBase
    val name = params(Names.REQ_NAME)
    
    val uid = params(Names.REQ_UID)
    val url = String.format("""%s/%s/%s/1""", base, name, uid)
 
    val scale = params.get(Names.REQ_SCALE) match {
      case None => 1.toString
      case Some(value) => value
    }
     
    val steps = params.get(Names.REQ_STEPS) match {
      case None => 5.toString
      case Some(value) => value
    }

    val states = Buffer.empty[String]
    
    (1 to 5).foreach(i => {
      (1 to 5).foreach(j => {
        states += "" + i + j
      })
    })
   
    params ++ Map(
      /* Algorithm specification */
      Names.REQ_ALGORITHM -> "MARKOV",
      Names.REQ_INTENT -> "STATE",
      
      Names.REQ_SCALE -> scale,
      Names.REQ_STEPS -> steps,
      
      Names.REQ_STATES -> states.mkString(","),
      
      /* Data source description */
      Names.REQ_URL -> url,        
      Names.REQ_SOURCE -> "PARQUET"
    )
    
  }

}