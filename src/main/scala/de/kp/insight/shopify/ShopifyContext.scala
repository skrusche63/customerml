package de.kp.insight.shopify
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

import scala.collection.mutable.{Buffer,HashMap}

class ShopifyContext(ctx:RequestContext) {

  /*
   * Determine Shopify access parameters from the configuration file 
   * (application.conf). Configuration is the accessor to this file.
   */
  private val (endpoint, apikey, password) = Configuration.shopify
  private val storeConfig = new StoreConfig(endpoint,apikey,password)  
  /*
   * This is the reference to the Shopify REST client
   */
  private val shopifyClient = new ShopifyClient(storeConfig)
  /*
   * The 'apikey' is used as the 'site' parameter when indexing
   * Shopify data with Elasticsearch
   */
  def getSite = apikey
  /**
   * A public method to retrieve Shopify customers from the REST interface;
   * this method is used to synchronize the customer base
   */
  def getCustomers(params:Map[String,String]):List[Customer] = {
    
    val customers = Buffer.empty[Customer]
    
    val start = new java.util.Date().getTime
    /*
     * Load Shopify customers from the REST interface
     */
    val uid = params(Names.REQ_UID)
    /*
     * STEP #1: Retrieve customers count from a certain shopify store;
     * for further processing, we set the limit of responses to the
     * maximum number (250) allowed by the Shopify interface
     */
    val count = shopifyClient.getCustomersCount(params)
    ctx.putLog("info",String.format("""[UID: %s] Load total of %s customers from Shopify store.""",uid,count.toString))

    val pages = Math.ceil(count / 250.0)
    val excludes = List("limit","page")
    
    val shopifyMapper = new ShopifyMapper(ctx)
     
    var page = 1
    while (page <= pages) {
      /*
       * STEP #2: Retrieve customers via a paginated approach, retrieving a maximum
       * of 250 customers per request
       */
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map("limit" -> "250","page" -> page.toString)
      customers ++= shopifyClient.getCustomers(params).map(customer => shopifyMapper.extractCustomer(apikey,customer))
             
      page += 1
              
    }

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Customers loaded in %s milli seconds.""",uid,(end-start).toString))
 
    customers.toList
    
  }
  /**
   * A public method to retrieve Shopify products from the REST interface;
   * this method is used to synchronize the product base
   */
  def getProducts(params:Map[String,String]):List[Product] = {
    
    val products = Buffer.empty[Product]
    
    val start = new java.util.Date().getTime
    /*
     * Load Shopify products from the REST interface
     */
    val uid = params(Names.REQ_UID)
    /*
     * STEP #1: Retrieve products count from a certain shopify store;
     * for further processing, we set the limit of responses to the
     * maximum number (250) allowed by the Shopify interface
     */
    val count = shopifyClient.getProductsCount(params)
    ctx.putLog("info",String.format("""[UID: %s] Load total of %s products from Shopify store.""",uid,count.toString))

    val pages = Math.ceil(count / 250.0)
    val excludes = List("limit","page")
    
    val shopifyMapper = new ShopifyMapper(ctx)
     
    var page = 1
    while (page <= pages) {
      /*
       * STEP #2: Retrieve products via a paginated approach, retrieving a maximum
       * of 250 customers per request
       */
      val data = params.filter(kv => excludes.contains(kv._1) == false) ++ Map("limit" -> "250","page" -> page.toString)
      products ++= shopifyClient.getProducts(params).map(product => shopifyMapper.extractProduct(apikey,product))
             
      page += 1
              
    }

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Products loaded in %s milli seconds.""",uid,(end-start).toString))
 
    products.toList
    
  }
  /**
   * A public method to retrieve the Shopify orders of the last 30, 60 
   * or 90 days from the REST interface
   */
  def getOrders(params:Map[String,String]):List[Order] = {
    
    val orders = Buffer.empty[Order]
    
    val start = new java.util.Date().getTime
    /*
     * Load Shopify orders from the last 30, 60 or 90 days from
     * the respective REST interface
     */
    val order_params = params ++ setOrderParams(params)
    val uid = order_params(Names.REQ_UID)
    /*
     * STEP #1: Retrieve orders count from a certain shopify store;
     * for further processing, we set the limit of responses to the
     * maximum number (250) allowed by the Shopify interface
     */
    val count = shopifyClient.getOrdersCount(order_params)

    ctx.putLog("info",String.format("""[UID: %s] Load total of %s orders from Shopify store.""",uid,count.toString))

    val pages = Math.ceil(count / 250.0)
    val excludes = List("limit","page")
    
    val shopifyMapper = new ShopifyMapper(ctx)
    
    var page = 1
    while (page <= pages) {
      /*
       * STEP #2: Retrieve orders via a paginated approach, retrieving a maximum
       * of 250 orders per request
       */
      val data = order_params.filter(kv => excludes.contains(kv._1) == false) ++ Map("limit" -> "250","page" -> page.toString)
      orders ++= shopifyClient.getOrders(order_params).map(order => shopifyMapper.extractOrder(apikey,order))
             
      page += 1
              
    }

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Orders loaded in %s milli seconds.""",uid,(end-start).toString))
 
    orders.toList
    
  }
  
  private def setOrderParams(params:Map[String,String]):Map[String,String] = {

    val data = HashMap.empty[String,String]
    
    /*
     * We restrict to those orders that have been paid,
     * and that are closed already, as this is the basis
     * for adequate forecasts 
     */
    data += "financial_status" -> "paid"
    data += "status" -> "closed"

    data.toMap
    
  }

}