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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import de.kp.spark.core.Names

import de.kp.insight._
import de.kp.insight.model._

import de.kp.insight.shopify.ShopifyContext

class CSMCollector(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {

  private val shopifyContext = new ShopifyContext(ctx)

  override def receive = {

    case message:StartCollect => {
      
      val uid = params(Names.REQ_UID)
             
      val start = new java.util.Date().getTime.toString            
      ctx.putLog("info",String.format("""[UID: %s] CSM collection request received at %s.""",uid,start))
       
      try {
      
        ctx.putLog("info",String.format("""[UID: %s] CSM collection started.""",uid))
            
        val start = new java.util.Date().getTime            
        val customers = shopifyContext.getCustomers(params)
       
        ctx.putLog("info",String.format("""[UID: %s] Customer base loaded from store.""",uid))

        val ids = customers.map(_.id)
        val sources = customers.map(x=> toSource(params,x))
        
        ctx.putSources("customers", "base", ids, sources)
 
        val end = new java.util.Date().getTime
        ctx.putLog("info",String.format("""[UID: %s] CSM collection finished at %s.""",uid,end.toString))
         
        val new_params = Map(Names.REQ_MODEL -> "CSM") ++ params

        context.parent ! CollectFinished(new_params)
        context.stop(self)
        
      } catch {
        case e:Exception => {

          ctx.putLog("error",String.format("""[UID: %s] CSM collection failed due to an internal error.""",uid))
          
          val new_params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ params

          context.parent ! CollectFailed(new_params)            
          context.stop(self)
          
        }
      }
      
    }
    case _ =>  
      
  }

  private def toSource(params:Map[String,String],customer:Customer):XContentBuilder = {
    
    val timestamp = params("timestamp").toLong
    
    val builder = XContentFactory.jsonBuilder()
	builder.startObject()
	
	/* site */
	builder.field("site",customer.site)
	
	/* id */
	builder.field("id",customer.id)
	
	/* first_name */
	builder.field("first_name",customer.firstName)
	
	/* last_name */
	builder.field("last_name",customer.lastName)
	
	/* signup_date */
	builder.field("signup_date",customer.created_at)
	
	/* last_sync */
	builder.field("last_sync",timestamp)

    builder.field("email",customer.emailAddress)
    builder.field("email_verified",customer.emailVerified)

    builder.field("accepts_marketing",customer.marketing)

    builder.field("operational_state",customer.state)

	builder.endObject()
    builder
    
  }
 
}