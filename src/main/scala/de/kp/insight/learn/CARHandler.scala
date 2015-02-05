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

class CARHandler(ctx:RequestContext) {
  
  def get(params:Map[String,String]):Map[String,String] = null

  def train(params:Map[String,String]):Map[String,String] = {
    
    val base = ctx.getBase
    val name = params(Names.REQ_NAME)
    
    val uid = params(Names.REQ_UID)
    val url = String.format("""%s/%s/%s/4""", base, name, uid)

    val init_mean = params.get("init_mean") match {
      case None => 0.01.toString
      case Some(value) => value
    }
      
    val init_stdev = params.get("init_stdev") match {
      case None => 0.01.toString
      case Some(value) => value
    }
    
    val k0 = params.get("k0") match {
      case None => true.toString
      case Some(value) => value
    }
    
    val k1 = params.get("k1") match {
      case None => true.toString
      case Some(value) => value
    }

    val reg_c = params.get("reg_c") match {
      case None => 0.0.toString
      case Some(value) => value
    }
    
    val reg_v = params.get("reg_v") match {
      case None => 0.01.toString
      case Some(value) => value
    }
    
    val reg_m = params.get("reg_m") match {
      case None => 0.01.toString
      case Some(value) => value
    }
    
    val num_partitions = params.get("num_partitions") match {
      case None => 20.toString
      case Some(value) => value
    }
    
    val num_factor = params.get("num_factor") match {
      case None => 20.toString
      case Some(value) => value
    }
    
    val num_iter = params.get("num_iter") match {
      case None => 100.toString
      case Some(value) => value
    }
    
    val learn_rate = params.get("learn_rate") match {
      case None => 0.01.toString
      case Some(value) => value
    }
    
    val num_attribute = params.get("num_attribute") match {
      case None => throw new Exception("Number of attributes must be provided.")
      case Some(value) => value
    }
    
    params ++ Map(
        
      /* Algorithm specification */
      Names.REQ_ALGORITHM -> "FM",
      
      /*
       * The Factorizationmachine algorithm requires the following parameters:
       */
      "init_mean"  -> init_mean,
      "init_stdev" -> init_stdev,
      
      "learn_rate" -> learn_rate,
      
      "k0" -> k0,
      "k1" -> k1,
      
      "reg_c" -> reg_c,
      "reg_v" -> reg_v,
      "reg_m" -> reg_m,
      
      
      "num_partitions" -> num_partitions,
      "num_factor" -> num_factor,
 
      "num_iter" -> num_iter,
      "num_attribute" -> num_attribute,
      
      /* Data source description */
      Names.REQ_URL -> url,        
      Names.REQ_SOURCE -> "PARQUET"
        
    )
    
  }

}