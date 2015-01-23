package de.kp.insight.woo
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

import org.joda.time.format.DateTimeFormat

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight._
import de.kp.insight.model._

import scala.collection.mutable.{Buffer,HashMap}

class WooContext(ctx:RequestContext) {
  
  private val (secret,key,url) = Configuration.woocommerce
  private val client = new WooClient(secret,key,url)

  def getCustomers(params:Map[String,String]):List[Customer] = {
    
    val start = new java.util.Date().getTime
    /*
     * Load WooCommerce customers from the REST interface
     */
    val uid = params(Names.REQ_UID)
    val wooMapper = new WooMapper(ctx)
    
    val req_params = params
    val customers = client.getCustomers(req_params).customers.map(x => wooMapper.extractCustomer(key, x))

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Customers loaded in %s milli seconds.""",uid,(end-start).toString))
 
    customers.toList
    
  }
  
  def getOrders(params:Map[String,String]):List[Order] = {
    
    val start = new java.util.Date().getTime
    /*
     * Load WooCommerce orders from the REST interface
     */
    val uid = params(Names.REQ_UID)
    val wooMapper = new WooMapper(ctx)
    
    val req_params = validateOrderParams(params)
    val orders = client.getOrders(req_params).orders.map(x => wooMapper.extractOrder(key, x))

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Orders loaded in %s milli seconds.""",uid,(end-start).toString))
 
    orders.toList
    
  }
  
  def getProducts(params:Map[String,String]):List[Product] = {
    
    val start = new java.util.Date().getTime
    /*
     * Load WooCommerce products from the REST interface
     */
    val uid = params(Names.REQ_UID)
    val wooMapper = new WooMapper(ctx)
    
    val req_params = params
    val products = client.getProducts(req_params).products.map(x => wooMapper.extractProduct(key, x))

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Customers loaded in %s milli seconds.""",uid,(end-start).toString))
 
    products.toList
    
  }

  /**
   * This method is used to format a certain timestamp, provided with 
   * a request to collect data from a certain Shopify store
   */
  private def formatted(time:Long):String = {

    //2008-12-31
    val pattern = "yyyy-MM-dd"
    val formatter = DateTimeFormat.forPattern(pattern)
    
    formatter.print(time)
    
  }

  /**
   * A helper method to transform the request parameters into validated params
   */
  private def validateOrderParams(params:Map[String,String]):Map[String,String] = {

    val requestParams = HashMap.empty[String,String]
    
    if (params.contains("created_at_min")) {
      /*
       * Show orders created after date (format: 2008-12-31)
       */
      val time = params("created_at_min").toLong
      requestParams += "created_at_min" -> formatted(time)
      
    }
    
    if (params.contains("created_at_max")) {
      /*
       * Show orders created before date (format: 2008-12-31)
       */
      val time = params("created_at_max").toLong
      requestParams += "created_at_max" -> formatted(time)
      
    }
    
    if (params.contains("updated_at_min")) {
      /*
       * Show orders last updated after date (format: 2008-12-31)
       */
      val time = params("updated_at_min").toLong
      requestParams += "updated_at_min" -> formatted(time)
      
    }
    
    if (params.contains("updated_at_max")) {
      /*
       * Show orders last updated before date (format: 2008-12-31)
       */
      val time = params("updated_at_max").toLong
      requestParams += "updated_at_max" -> formatted(time)
      
    }
    
    requestParams.toMap
  
  }

}