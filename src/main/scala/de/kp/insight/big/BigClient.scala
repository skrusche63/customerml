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

import org.scribe.model._
import org.slf4j.LoggerFactory

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class BigClient(val key:String,val secret:String) {

  private val LOG = LoggerFactory.getLogger(classOf[BigClient])
  
  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  /*
   * Retrieve service & access token, either dynamically
   * or from file
   */
  val service = AuthUtil.loadService
  val accessToken = AuthUtil.loadAccessToken
  /*
   * Retrieve authentication info from file; note, that the
   * respective data must have been provided by Bigcommerce
   * through an Authentical Callback request
   */
  val authInfo = AuthUtil.loadAuthInfo
  val ENDPOINT = String.format("""https://api.bigcommerce.com/%s/v2/""",authInfo.context)

  def getBrands(requestParams:Map[String,String]):List[BigBrand] = {
 
    val endpoint = ENDPOINT + "brands"

    val request = new OAuthRequest(Verb.GET, endpoint)
    request.addHeader("accept", "application/json")

    request.addHeader("X-Auth-Client", key)
    request.addHeader("X-Auth-Token", accessToken.getToken())
		
    val response = request.send()
    if (response.getCode == 200) {
      
      val body = response.getBody
      JSON_MAPPER.readValue(body, classOf[List[BigBrand]])
      
    } else {
      throw new Exception("Bad request: " + response.getCode)
    }
    
  }

  def getCustomers(requestParams:Map[String,String]):List[BigCustomer] = {
 
    val endpoint = ENDPOINT + "customers"

    val request = new OAuthRequest(Verb.GET, endpoint)
    request.addHeader("accept", "application/json")

    request.addHeader("X-Auth-Client", key)
    request.addHeader("X-Auth-Token", accessToken.getToken())
		
    val response = request.send()
    if (response.getCode == 200) {
      
      val body = response.getBody
      JSON_MAPPER.readValue(body, classOf[List[BigCustomer]])
      
    } else {
      throw new Exception("Bad request: " + response.getCode)
    }
    
  }
  
  def getOrders(requestParams:Map[String,String]):List[BigOrder] = {
 
    val endpoint = ENDPOINT + "orders"

    val request = new OAuthRequest(Verb.GET, endpoint)
    request.addHeader("accept", "application/json")

    request.addHeader("X-Auth-Client", key)
    request.addHeader("X-Auth-Token", accessToken.getToken())
		
    val response = request.send()
    if (response.getCode == 200) {
      
      val body = response.getBody
      JSON_MAPPER.readValue(body, classOf[List[BigOrder]])
      
    } else {
      throw new Exception("Bad request: " + response.getCode)
    }
    
  }
  
  def getProducts(requestParams:Map[String,String]):List[BigProduct] = {
 
    val endpoint = ENDPOINT + "products"

    val request = new OAuthRequest(Verb.GET, endpoint)
    request.addHeader("accept", "application/json")

    request.addHeader("X-Auth-Client", key)
    request.addHeader("X-Auth-Token", accessToken.getToken())
		
    val response = request.send()
    if (response.getCode == 200) {
      
      val body = response.getBody
      JSON_MAPPER.readValue(body, classOf[List[BigProduct]])
      
    } else {
      throw new Exception("Bad request: " + response.getCode)
    }
    
  }

}