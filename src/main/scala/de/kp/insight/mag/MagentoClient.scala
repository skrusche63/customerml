package de.kp.insight.mag
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

class MagentoClient(val url:String,val key:String,val secret:String) {

  private val LOG = LoggerFactory.getLogger(classOf[MagentoClient])
  
  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  val service = AuthUtil.loadService
  val accessToken = AuthUtil.loadAccessToken

  def getCustomers(requestParams:Map[String,String]) {
  
    val endpoint = url + "/api/rest/customers"
    
  }

  def getInventory(requestParams:Map[String,String]) {
  
    val endpoint = url + "/api/rest/stockitems"
    
  }
  
  def getOrders(requestParams:Map[String,String]):List[MagentoOrder] = {
  
    val endpoint = url + "/api/rest/orders"
    /*
     * TODO: Add filter parameters to request
     */
    val request = new OAuthRequest(Verb.GET, endpoint)
    request.addHeader("accept", "application/json")
    
    service.signRequest(accessToken, request)
		
    val response = request.send()
    if (response.getCode == 200) {
      
      val body = response.getBody
      JSON_MAPPER.readValue(body, classOf[List[MagentoOrder]])
      
    } else {
      throw new Exception("Bad request: " + response.getCode)
    }
    
  }
  
  def getProducts(requestParams:Map[String,String]) {
  
    val endpoint = url + "/api/rest/products"
  
    val request = new OAuthRequest(Verb.GET, endpoint)
    request.addHeader("accept", "application/json")

    service.signRequest(accessToken, request)
		
    val response = request.send()
    if (response.getCode == 200) {
      
      val body = response.getBody
      
    } else {
      throw new Exception("Based request: " + response.getCode)
    }
    
  }
}