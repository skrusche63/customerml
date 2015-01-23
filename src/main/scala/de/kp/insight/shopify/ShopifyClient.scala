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

import java.io.IOException

import javax.ws.rs.HttpMethod
import javax.ws.rs.client.{Client,ClientBuilder,Entity,WebTarget}
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import de.kp.insight.StoreConfig
import de.kp.insight.model._

import org.slf4j.{Logger,LoggerFactory}

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

class ShopifyClient(configuration:StoreConfig) {

  private val LOG = LoggerFactory.getLogger(classOf[ShopifyClient])

  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)
  
  val client = ClientBuilder.newClient()
  val endpoint = configuration.getEndpoint().replaceFirst("://", "://" + 
      configuration.getApikey() + ":" + configuration.getPassword() + "@")
      
  val webTarget = client.target(endpoint).path("admin")   

  /**************************************************************************
   * 
   *                        CUSTOMER SUPPORT
   * 
   *************************************************************************/
  
  def getCustomers(params:Map[String,String]):List[ShopifyCustomer] = {
    getResponse("customers.json", params, null, HttpMethod.GET).customers
  }
  
  /**
   * A request to the Shopify's REST interface to retrieve
   * the total number of customers
   */
  def getCustomersCount(params:Map[String,String]):Int = {
    getResponse("customers/count.json", params, null, HttpMethod.GET).count
    
  }

  /**************************************************************************
   * 
   *                        PRODUCT SUPPORT
   * 
   *************************************************************************/
  
  def getProduct(pid:Long):ShopifyProduct = {
    getResponse("products/" + pid + ".json", null, null, HttpMethod.GET).product    
  }
  /**
   * Retrieve all products that match the provided parameters
   * from a certain Shopify store
   */
  def getProducts(params:Map[String,String]):List[ShopifyProduct] = {
    getResponse("products.json", params, null, HttpMethod.GET).products
  }
  
  /**
   * A request to the Shopify's REST interface to retrieve
   * the total number of products
   */
  def getProductsCount(params:Map[String,String]):Int = {
    getResponse("products/count.json", params, null, HttpMethod.GET).count
    
  }
 
  def getProductVariant(sku:String):ShopifyProductVariant = {
    
    val params = HashMap.empty[String,String]
    params += "field" -> "id,sku"
    
    val response = getResponse("variants.json", params.toMap, null, HttpMethod.GET)
    
    val variants = response.productVariants
    for (variant <- variants) {
      if (sku.equals(variant.sku)) {
        return getProductVariant(variant.id)
      }
      
    }
    
    null
    
    }

  def getProductVariant(productVariantId:Long):ShopifyProductVariant = {
    getResponse("variants/" + productVariantId + ".json", null, null, HttpMethod.GET).productVariant
  }

  /**************************************************************************
   * 
   *                        ORDER SUPPORT
   * 
   *************************************************************************/
  
  /** Retrieve a single order from a Shopify store; the order must be
   *  uniquely identified by its order identifier (iod)
   */
  def getOrder(oid:Long):ShopifyOrder = {
    getResponse("orders/" + oid + ".json", null, null, HttpMethod.GET).order
  }

  /**
   * Retrieve all orders that match the provided parameters
   * from a certain Shopify store
   */
  def getOrders(params:Map[String,String]):List[ShopifyOrder] = {
    getResponse("orders.json", params, null, HttpMethod.GET).orders
  }
  
  def getOrdersCount(params:Map[String,String]):Int = {
    getResponse("orders/count.json", params, null, HttpMethod.GET).count
    
  }
  
  /**************************************************************************
   * 
   *                        COLLECTION SUPPORT
   * 
   *************************************************************************/
 
  def createCollection(params:Map[String,String],collection:String):ShopifyCollection = {
    getResponse("custom_collections.json", params, null, HttpMethod.POST).custom_collection
   
  }

  private def getResponse(resourcePath:String,params:Map[String,String],body:String,method:String):ShopifyResponse = {
       
    try {
      
      var queryTarget = webTarget.path(resourcePath)

      for (entry <- params) {
        val (k,v) = entry
        queryTarget = queryTarget.queryParam(k,v)
      }

      val message = String.format("""Request parameters: %s %s""",resourcePath,params)      
      LOG.info(message)

      val jsonResponse = queryTarget
                           .request(MediaType.APPLICATION_JSON_TYPE)
                           .method(method, if (body == null) null else Entity.json(body), classOf[String])

      LOG.info("Response body: " + jsonResponse)

      val response = JSON_MAPPER.readValue(jsonResponse, classOf[ShopifyResponse])
      validate(response)
            
      return response
    
    } catch {
      case e:Exception => throw new Exception("Could not process query",e)
    }

  }

  private def validate(response:ShopifyResponse) {
    if (response.errors != null) {
      throw new Exception(response.errors)
    }
  }

}