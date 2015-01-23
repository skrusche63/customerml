package de.kp.insight.collect
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
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import de.kp.spark.core.Names

import de.kp.insight._
import de.kp.insight.model._

import de.kp.insight.shopify.ShopifyContext

class ORDCollector(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {

  private val shopifyContext = new ShopifyContext(ctx)
  
  override def receive = {

    case message:StartCollect => {
      
      val uid = params(Names.REQ_UID)
             
      val start = new java.util.Date().getTime.toString            
      ctx.putLog("info",String.format("""[UID: %s] ORD collection request received at %s.""",uid,start))
      
      try {
      
        ctx.putLog("info",String.format("""[UID: %s] ORD collection started.""",uid))
            
        val start = new java.util.Date().getTime            
        val orders = shopifyContext.getOrders(params)
       
        ctx.putLog("info",String.format("""[UID: %s] Order base loaded from store.""",uid))

        /*
         * STEP #1: Write orders of a certain period of time 
         * to the orders/base index and 
         */
        val sources = orders.map(x=> buildOrder(params,x))       
        ctx.putSources("orders", "base", sources)
        
        val end = new java.util.Date().getTime
        ctx.putLog("info",String.format("""[UID: %s] ORD collection finished at %.""",uid,end.toString))
        
        val new_params = Map(Names.REQ_MODEL -> "ORD") ++ params

        context.parent ! CollectFinished(new_params)
        context.stop(self)
        
      } catch {
        case e:Exception => {

          ctx.putLog("error",String.format("""[UID: %s] ORD collection failed due to an internal error.""",uid))
          
          val new_params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ params

          context.parent ! CollectFailed(params)            
          context.stop(self)
          
        }
      }
      
    }
    case _ =>  
      
  }
  
  private def buildOrder(params:Map[String,String],order:Order):XContentBuilder = {
    
    val uid = params("uid")
    val timestamp = params("timestamp").toLong
    /*
     * Note, that we must index the time period as timestamps
     * as these parameters are used to filter orders later on
     */
    val created_at_min = unformatted(params("created_at_min"))
    val created_at_max = unformatted(params("created_at_max"))
    
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
               
   /********** METADATA **********/
    
	/* uid */
	builder.field("uid",uid)
	
	/* last_sync */
	builder.field("last_sync",timestamp)
	
	/* created_at_min */
	builder.field("created_at_min",created_at_min)
	
	/* created_at_max */
	builder.field("created_at_max",created_at_max)
	
	/* site */
	builder.field("site",order.site)
             
    /********** ORDER DATA **********/
	
	/* user */
	builder.field("user",order.user)
	
	/* amount */
	builder.field("amount",order.amount)
	
	/* discount */
	builder.field("discount",order.discount)
	
	/* shipping */
	builder.field("shipping",order.shipping)
	
	/* timestamp */
	builder.field("timestamp",order.timestamp)
	
	/* group */
	builder.field("group",order.group)
	
	/* ip_address */
	builder.field("ip_address",order.ip_address)
	
	/* user_agent */
	builder.field("user_agent",order.user_agent)
 	
	/* items */
	builder.startArray("items")
	
	for (item <- order.items) {
	  
	  builder.startObject()
	  
	  /* item */
	  builder.field("item",item.item)
	  
	  /* quantity */
	  builder.field("quantity",item.quantity)

	  /* category */
	  builder.field("category",item.category)

	  /* vendor */
	  builder.field("vendor",item.vendor)
	  
	  builder.endObject()
	  
	}
	
    builder.endArray()
	
	builder.endObject()	
	builder

  }

}