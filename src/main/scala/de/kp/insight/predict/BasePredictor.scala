package de.kp.insight.predict
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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight._
import de.kp.insight.model._

abstract class BasePredictor(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {
  
  override def receive = {
    
    case msg:StartPrepare => {

      val req_params = params
      
      val uid = req_params(Names.REQ_UID)
      val name = req_params(Names.REQ_NAME)
      
      try {
      
        val start = new java.util.Date().getTime.toString            
        ctx.putLog("info",String.format("""[UID: %s] %s prediction request received at %s.""",uid,name,start))
        
        predict(req_params)

        val end = new java.util.Date().getTime
        ctx.putLog("info",String.format("""[UID: %s] %s prediction finished at %s.""",uid,name,end.toString))

        val params = Map(Names.REQ_MODEL -> name) ++ req_params
        context.parent ! PredictFinished(params)

      } catch {
        case e:Exception => {

          ctx.putLog("error",String.format("""[UID: %s] %s prediction exception: %s.""",uid,name,e.getMessage))
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params
          context.parent ! PredictFailed(params)
        
        }

      } finally {
        
        context.stop(self)
        
      }
    
    }
  
  }
  
  protected def predict(params:Map[String,String])

}