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

import akka.actor.Props

import de.kp.spark.core.Names
import de.kp.spark.core.actor._

import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.model._

import de.kp.insight.BaseActor

class SAELearner(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {
  
  private val config = ctx.getConfig
  
  override def receive = {
   
    case message:StartLearn => {
      
      val req_params = params
      
      val uid = req_params(Names.REQ_UID)
      val name = req_params(Names.REQ_NAME)
      
      val start = new java.util.Date().getTime.toString            
      ctx.putLog("info",String.format("""[UID: %s] %s learning request received at %s.""",uid,name,start))
      
      /* 
       * Build service request message to invoke remote Similarity Analysis 
       * engine to build clusters from the respective dataset; the dataset
       * is referenced by the 'name' attribute
       */
      val service = "similarity"
      val task = "train"

      val data = new SAEHandler(ctx).train(req_params)
      val req  = new ServiceRequest(service,task,data)
      
      val serialized = Serializer.serializeRequest(req)
      val response = ctx.getRemoteContext.send(service,serialized).mapTo[String]  
      
      ctx.putLog("info",String.format("""[UID: %s] %s learning started.""",uid,name))
      
      /*
       * The RemoteSupervisor actor monitors the Redis cache entries of this
       * clustering request and informs this actor (as parent) that a certain 
       * status has been reached
       */
      val status = ResponseStatus.TRAINING_FINISHED
      val supervisor = context.actorOf(Props(new Supervisor(req,status,config)))
      
      /*
       * We evaluate the response message from the remote Association Analysis 
       * engine to check whether an error occurred
       */
      response.onSuccess {
        
        case result => {
 
          val res = Serializer.deserializeResponse(result)
          if (res.status == ResponseStatus.FAILURE) {
      
            ctx.putLog("error",String.format("""[UID: %s] %s learning failed due to an engine error.""",uid,name))
 
            context.parent ! LearnFailed(res.data)
            context.stop(self)

          }
         
        }

      }
      response.onFailure {
          
        case throwable => {
      
          ctx.putLog("error",String.format("""[UID: %s] %s learning failed due to an internal error.""",uid,name))
        
          val res_params = Map(Names.REQ_MESSAGE -> throwable.getMessage) ++ req_params
          context.parent ! LearnFailed(res_params)
          
          context.stop(self)
            
          }
	    }
       
    }
  
    case event:StatusEvent => {
      
      val uid = params(Names.REQ_UID)
      val name = params(Names.REQ_NAME)
      
      val end = new java.util.Date().getTime.toString            
      ctx.putLog("info",String.format("""[UID: %s] %s learning finished at %s.""",event.uid,name,end))

      val res_params = Map(Names.REQ_UID -> event.uid,Names.REQ_MODEL -> name)
      context.parent ! LearnFinished(res_params)
      
      context.stop(self)
      
    }
    
  }

}