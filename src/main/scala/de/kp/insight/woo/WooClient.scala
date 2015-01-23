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

import java.net.URLEncoder

import java.security.MessageDigest

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.core.MediaType

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.slf4j.LoggerFactory

import org.apache.commons.codec.binary.Base64

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

private case class Pair(key:String,value:String)

class WooClient( val url:String,val key:String,val secret:String) {

  private val LOG = LoggerFactory.getLogger(classOf[WooClient])

  private val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  private val API_URL:String = "/wc-api/v2"
  private val ENC:String = "HMAC-SHA1"

  private val HASH_ALGORITHM:String = "HmacSHA1"
  
  private val uri = url + API_URL
  private val base64 = new Base64()
  
  val client = ClientBuilder.newClient()
  val webTarget = client.target(uri).path("/")

  def getCustomer(cid:Int):WooCustomer = {
            
    val endpoint = "customers/" + cid
    getResponse(endpoint, Map.empty[String,String], classOf[WooCustomer])   
    
  }

  def getCustomers(requestParams:Map[String,String]):WooCustomers = {
 
    val endpoint = "customers"
    getResponse(endpoint, requestParams, classOf[WooCustomers])   

  }

  def getOrder(oid:Int):WooOrder = {

    val endpoint = "orders/" + oid
    getResponse(endpoint, Map.empty[String,String], classOf[WooOrder])   
    
  }

  def getOrders(requestParams:Map[String,String]):WooOrders = {
 
    val endpoint = "orders"
    getResponse(endpoint, requestParams, classOf[WooOrders])   

  }

  def getProduct(oid:Int):WooProduct = {
            
    val endpoint = "products/" + oid
    getResponse(endpoint, Map.empty[String,String], classOf[WooProduct])   
    
  }

  def getProducts(requestParams:Map[String,String]):WooProducts = {
 
    val endpoint = "products"
    getResponse(endpoint, requestParams, classOf[WooProducts])   

  }

  private def getResponse[T](endpoint:String,requestParams:Map[String,String],clazz:Class[T]):T = {
      
    try {
    
      val response = getResponse(endpoint,requestParams)
      JSON_MAPPER.readValue(response, clazz)  
    
    } catch {
      case e:Exception => {
        println(e.getMessage())
        throw new Exception("Could not process query",e)
      }
    }
  
  }
  
  private def getResponse(endpoint:String,requestParams:Map[String,String]):String = {
      
    var queryTarget = webTarget.path(endpoint)
    if (url.contains("https")) {
      null
        
    } else {

      val millis = String.valueOf(System.currentTimeMillis())
      val params = ArrayBuffer.empty[Pair]

      /*
       * Add request specific parameters; actually a time filter
       * is supported, e.g. 'created_at_max' and 'created_at_min'
       */
      if (requestParams.isEmpty == false) {
        requestParams.foreach(entry => params += Pair(entry._1,entry._2))      
      }
      
      params += Pair("oauth_consumer_key",key)
      params += Pair("oauth_nonce",sha1(millis))

      params += Pair("oauth_signature_method",ENC)
      params += Pair("oauth_timestamp",time())

      /*
       * We define the parameters as list to make sure that this
       * ordering is also used when preparing the request url
       */     
      val sorted = params.sortBy(x => x.key)
      val signature = Pair("oauth_signature", generateOAuthSignature(endpoint,"GET",sorted))
     
      queryTarget = queryTarget.queryParam(signature.key,signature.value)
      for (param <- sorted) {
        queryTarget = queryTarget.queryParam(param.key,param.value)
      }
        
      val response = queryTarget.request(MediaType.APPLICATION_JSON_TYPE).method("GET", null, classOf[String])
      response   
    
    }
    
  }
  
  private def time():String = {
	return String.valueOf(new java.util.Date().getTime()).substring(0, 10)
  }

  private def generateOAuthSignature(endpoint:String,method:String,params:ArrayBuffer[Pair]):String = {
  
    val queryParams = ArrayBuffer.empty[String]
    for (param <- params) {
      
      val k = URLEncoder.encode(param.key,"UTF-8").replace("%", "%25")
      val v = URLEncoder.encode(param.value,"UTF-8").replace("%", "%25")
      
      val text = "" + k  + "%3D" + v
      queryParams += text
      
    }
    
    val queryString = queryParams.mkString("%26")
    val baseURI = URLEncoder.encode(url + API_URL + "/" + endpoint, "UTF-8")

    getSignature(baseURI,method,queryString)
    
 }
  
  private def getSignature(url:String,method:String,params:String):String = {
	/*
	 * StringBuilder has three parts that are connected by "&": 
	 * 
	 * 1) protocol 
	 * 2) URL (need to be URLEncoded) 
	 * 3) Parameter List (need to be URLEncoded)
	 */
	val sb = new StringBuilder()
		
	sb.append(method.toUpperCase() + "&")
	sb.append(url)
		
	sb.append("&")
	sb.append(params)

	val keyBytes = secret.getBytes("UTF-8")
	val secretKey = new SecretKeySpec(keyBytes, HASH_ALGORITHM);

	val mac = Mac.getInstance(HASH_ALGORITHM)
	mac.init(secretKey)

	val signature = new String(Base64.encodeBase64(mac.doFinal(sb.toString().getBytes("UTF-8"))))
    signature

  }
	
  private def sha1(password:String):String = {
		
    var sha1:String = ""
	
    try {
	 
      val crypt = MessageDigest.getInstance("SHA-1")
	  crypt.reset()
			
	  crypt.update(password.getBytes())
      sha1 = byteToHex(crypt.digest())
      
	} catch {
	  case e:Exception => e.printStackTrace()
	}
	
	return sha1
	
  }

  private def byteToHex(hash:Array[Byte]):String = {
    hash.map{ b => String.format("%02X", new java.lang.Integer(b & 0xff)) }.mkString
  } 

}