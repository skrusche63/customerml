package de.kp.insight.enrich
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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight._
import de.kp.insight.model._

abstract class BaseEnricher(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {
        
  override def receive = {
   
    case message:StartEnrich => {
      
      val req_params = params
      
      val uid = req_params(Names.REQ_UID)
      val name = req_params(Names.REQ_NAME)
      
      val start = new java.util.Date().getTime.toString            
      ctx.putLog("info",String.format("""[UID: %s] %s enrichment request received at %s.""",uid,name,start))
      
      try {
       
        enrich
        
        val end = new java.util.Date().getTime.toString
        ctx.putLog("info",String.format("""[UID: %s] %s enrichment finished at %s.""",uid,end))
       
        
      } catch {
        case e:Exception => {

          ctx.putLog("error",String.format("""[UID: %s] %s enrichment failed due to an internal error.""",uid,name))
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params

          context.parent ! EnrichFailed(params)            
          context.stop(self)
          
        }
    
      }
    
    }

  }
  
  protected def enrich {}
  
}