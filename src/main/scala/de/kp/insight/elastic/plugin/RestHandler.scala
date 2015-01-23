package de.kp.insight.elastic.plugin
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

import java.io.IOException

import org.elasticsearch.rest._

import org.elasticsearch.client.Client

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.{XContentFactory,XContentType}

import org.elasticsearch.rest.RestStatus.OK

import de.kp.spark.core.model._

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import scala.concurrent.ExecutionContext.Implicits.global

abstract class RestHandler(settings:Settings,client:Client) extends BaseRestHandler(settings,client) {

  protected def getParams(request:RestRequest):Map[String,Any] = {

    val data = HashMap.empty[String,Any]
    
    /* Append request parameters */
    request.params().foreach(entry => {
      data += entry._1-> entry._2
    })
    
    /* Append content parameters */
    val params = XContentFactory.xContent(request.content()).createParser(request.content()).mapAndClose()
    params.foreach(entry => {
      data += entry._1-> entry._2
    })
      
    data.toMap

  }
 
  protected def onError(channel:RestChannel,t:Throwable) {
        
    try {
      channel.sendResponse(new BytesRestResponse(channel, t))
        
    } catch {
      case e:Throwable => logger.error("Failed to send a failure response.", e);
  
    }
    
  }
  
  protected def sendResponse(channel:RestChannel, request:RestRequest, response:String) {
	        
    try {
	  
      val contentType = XContentType.JSON.restContentType()
	  channel.sendResponse(new BytesRestResponse(RestStatus.OK,contentType,response))
	            
    } catch {
      case e:IOException => throw new Exception("Failed to build a response.", e)
    
    }   
    
  }

}