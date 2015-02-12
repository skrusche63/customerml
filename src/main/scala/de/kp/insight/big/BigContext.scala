package de.kp.insight.big
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

import org.joda.time.format.DateTimeFormat

import de.kp.insight._
import de.kp.insight.model._

import org.scribe.model.{Token,Verifier}

import scala.collection.mutable.{Buffer,HashMap}

class BigContext(ctx:RequestContext) extends ShopContext {
  
  private val EMPTY_TOKEN:Token = null
  private val EMPTY_VERIFIER:Verifier = null
  
  private val settings = Configuration.bigcommerce
  
  private val client_id = settings("client_id")
  private val client_secret = settings("client_secret")
  
  /*
   * Try to load the OAuth access token; this functionality is
   * based on a successful retrieval of authentication info
   * provided by Bigcommerce through OAuth callback request
   */
  val token = if (AuthUtil.loadAccessToken == null) {
    /*
     * The is no access token saven on the file system;
     * this implies, that we have to retrieve one from
     * Bigcommerce
     */
    val callback_url = settings("callback_url")
    
    val service = AuthUtil.createService(client_id,client_secret,callback_url)
    val accessToken = service.getAccessToken(EMPTY_TOKEN, EMPTY_VERIFIER)
    
    accessToken.getToken()
    
  } else AuthUtil.loadAccessToken.getToken()
  
  private val shopClient = new BigClient(client_id,token)
  
  /*
   * The 'client_id' is used as the 'site' parameter when indexing
   * Bigcommerce data with Elasticsearch
   */
  def getSite:String = client_id

  /**
   * A public method to retrieve Bigcommerce customers from the REST
   * interface; this method is used to synchronize the customer base
   */
  def getCustomers(params:Map[String,String]):List[Customer] = {
    
    val customers = Buffer.empty[Customer]
    
    val start = new java.util.Date().getTime
    val mapper = new BigMapper(ctx)
     
    var page = 1
    var finished = false
    
    while (finished == false) {
      /*
       * Retrieve customers via a paginated approach, retrieving a maximum
       * of 250 customers per request
       */
      val req_params = params ++ Map("limit" -> "250","page" -> page.toString)
      val result = shopClient.getCustomers(req_params).map(customer => mapper.extractCustomer(client_id,customer))
      
      customers ++= result

      page += 1
      /*
       * Check whether this request has been the last 
       * request; the respective condition is given,
       * if less than 250 customers are retrieved
       */
      if (result.size < 250) finished = true       
              
    }
    
    val uid = params(Names.REQ_UID)

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Customers loaded in %s milli seconds.""",uid,(end-start).toString))
 
    customers.toList

  }
  
  /**
   * A public method to retrieve the Bigcommerce orders of the last 30, 
   * 60 or 90 days from the REST interface
   */
  def getOrders(params:Map[String,String]):List[Order] = {
    
    val orders = Buffer.empty[Order]
    
    val start = new java.util.Date().getTime
    /*
     * Load Shopify orders from the last 30, 60 or 90 days from
     * the respective REST interface
     */
    val order_params = params ++ setOrderParams(params)
    val mapper = new BigMapper(ctx)
     
    var page = 1
    var finished = false
    
    while (finished == false) {
     /*
       * Retrieve orders via a paginated approach, retrieving a maximum
       * of 250 orders per request
       */
      val req_params = order_params ++ Map("limit" -> "250","page" -> page.toString)
      val result = shopClient.getOrders(req_params).map(order => mapper.extractOrder(client_id,order))
      
      orders ++= result

      page += 1
      /*
       * Check whether this request has been the last 
       * request; the respective condition is given,
       * if less than 250 customers are retrieved
       */
      if (result.size < 250) finished = true       
              
    }

    val uid = order_params(Names.REQ_UID)

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Orders loaded in %s milli seconds.""",uid,(end-start).toString))
 
    orders.toList
    
  }
  
  def getBrand(brand:Int) = shopClient.getBrand(brand)

  def getImages(product:Int):List[BigImage] = {
     
    val lineitems = Buffer.empty[BigImage]
     
    var page = 1
    var finished = false
    
    while (finished == false) {
      /*
       * Retrieve line items via a paginated approach, retrieving 
       * a maximum of 250 images per request
       */
      val req_params = Map("limit" -> "250","page" -> page.toString)
      val result = shopClient.getImages(product,req_params)
      
      lineitems ++= result

      page += 1
      /*
       * Check whether this request has been the last 
       * request; the respective condition is given,
       * if less than 250 images are retrieved
       */
      if (result.size < 250) finished = true       
              
    }
 
    lineitems.toList
   
  }
  
  /**
   * A public method to retrieve Bigcommerce lineitems from the REST 
   * interface for a certain order
   */
  def getLineItems(order:Int):List[BigLineItem] = {
     
    val lineitems = Buffer.empty[BigLineItem]
     
    var page = 1
    var finished = false
    
    while (finished == false) {
      /*
       * Retrieve line items via a paginated approach, retrieving 
       * a maximum of 250 line items per request
       */
      val req_params = Map("limit" -> "250","page" -> page.toString)
      val result = shopClient.getLineItems(order,req_params)
      
      lineitems ++= result

      page += 1
      /*
       * Check whether this request has been the last 
       * request; the respective condition is given,
       * if less than 250 line items are retrieved
       */
      if (result.size < 250) finished = true       
              
    }
 
    lineitems.toList
   
  }

  /**
   * A public method to retrieve Bigcommerce products from the REST 
   * interface; this method is used to synchronize the product base
   */
  def getProducts(params:Map[String,String]):List[Product] = {
     
    val products = Buffer.empty[Product]
    
    val start = new java.util.Date().getTime
    val mapper = new BigMapper(ctx)
     
    var page = 1
    var finished = false
    
    while (finished == false) {
      /*
       *  Retrieve products via a paginated approach, retrieving a maximum
       * of 250 customers per request
       */
      val req_params = params ++ Map("limit" -> "250","page" -> page.toString)
      val result = shopClient.getProducts(req_params).map(product => mapper.extractProduct(client_id,product))
      
      products ++= result

      page += 1
      /*
       * Check whether this request has been the last 
       * request; the respective condition is given,
       * if less than 250 products are retrieved
       */
      if (result.size < 250) finished = true       
              
    }

    val uid = params(Names.REQ_UID)

    val end = new java.util.Date().getTime
    ctx.putLog("info",String.format("""[UID: %s] Products loaded in %s milli seconds.""",uid,(end-start).toString))
 
    products.toList
   
  }
  
  private def setOrderParams(params:Map[String,String]):Map[String,String] = {

    val created_at_min = params("created_at_min")
    val created_at_max = params("created_at_max")
    
    /*
     * The date format must be mapped from the
     * default to the bigcommerce specific one
     */
    val min_date_created = DateUtil.unformatted(created_at_min, DateUtil.DEFAULT)
    val max_date_created = DateUtil.unformatted(created_at_max, DateUtil.DEFAULT)
    
    
    val data = Map(
      "min_date_created" -> DateUtil.formatted(min_date_created,DateUtil.BIG_COMMERCE),
      "max_date_created" -> DateUtil.formatted(max_date_created,DateUtil.BIG_COMMERCE),
      /*
       * "id": "0",  "name": "Incomplete"
       * "id": "1",  "name": "Pending"
       * "id": "2",  "name": "Shipped"
       * "id": "3",  "name": "Partially Shipped"
       * "id": "4",  "name": "Refunded"
       * "id": "5",  "name": "Cancelled"
       * "id": "6",  "name": "Declined"
       * "id": "7",  "name": "Awaiting Payment"
       * "id": "8",  "name": "Awaiting Pickup"
       * "id": "9",  "name": "Awaiting Shipment"
       * "id": "10", "name": "Completed"
       * "id": "11", "name": "Awaiting Fulfillment"
       * "id": "12", "name": "Manual Verification Required",
       */
      "status_id" -> 10.toString
    )

    data
    
  }
  
}