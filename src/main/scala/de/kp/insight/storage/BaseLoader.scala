package de.kp.insight.storage
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

import de.kp.insight._
import de.kp.insight.model._

abstract class BaseLoader(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {

  override def receive = {
   
    case message:StartLoad => {
      
      val req_params = params
      
      val uid = req_params(Names.REQ_UID)
      val name = req_params(Names.REQ_NAME)
      
      try {
      
        ctx.putLog("info",String.format("""[UID: %s] %s load request received.""",uid,name))
        
        load(req_params)
        
        ctx.putLog("info",String.format("""[UID: %s] Purchase forecast model loading finished.""",uid))

        val data = Map(Names.REQ_UID -> uid,Names.REQ_MODEL -> name)            
        context.parent ! LoadFinished(data)           
            
        context.stop(self)
         
      } catch {
        case e:Exception => {

          ctx.putLog("error",String.format("""[UID: %s] %s loading failed due to an internal error.""",uid,name))
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params

          context.parent ! LoadFailed(params)            
          context.stop(self)
          
        }
    
      }
    
    }
   
  }  
  
  protected def load(params:Map[String,String])
  
}