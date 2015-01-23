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

import org.elasticsearch.rest._
import org.elasticsearch.client.Client

import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings

class RecommendAction @Inject()(settings:Settings,client:Client,controller:RestController) extends RestHandler(settings,client) {

  logger.info("Add RecommendAction module") 
  controller.registerHandler(RestRequest.Method.POST,"/_recommendations/{method}", this)
  
  override protected def handleRequest(request:RestRequest,channel:RestChannel,client:Client) {

    try {

      logger.info("RecommendAction: Request received")
      /*
       * STEP #1: Retrieve request parameters and validate them
       */
      val params = getParams(request)
      if (params.contains("site") == false)
        throw new Exception("Parameter 'site' is missing.")

      if (params.contains("created_at") == false)
        throw new Exception("Parameter 'created_at' is missing.")
      
      if (params.contains("method") == false)
        throw new Exception("Parameter 'method' is missing.")
      
      val questor = new RecommendQuestor(client)
      
      /*
       * The method parameter determines which recommendation MUST
       * be built from the request parameters
       */
      params("method").asInstanceOf[String] match {
        
        case "recommended_products" => {
          /*
           * This method determines those products that been assigned to every single
           * customer as a result of the collaborative filtering approach; it does not
           * take any dynamic context (e.g. last purchase) into account and provides
           * those most rated products due to the similarity of a certain customer to
           * other customers 
           */
          if (params.contains("user") == false) {
            throw new Exception("Parameter 'user' is missing.")
          }
          
          val site = params("site").asInstanceOf[String]
          val user = params("user").asInstanceOf[String]
          
          val created_at = params("created_at").asInstanceOf[Long]
          val response = questor.recommended_products(site, user, created_at)
          
          sendResponse(channel,request,response)
           
        }
 
        case "related_products" => {
          /*
           * This method determines those products that are related to a list of products
           * provided with this request. The request supports the recommendation feature
           * 'customer who bought this also bought that' and leverages the product relation
           * model (PRM) determined prior to this request
           */
          if (params.contains("customer") == false) {
            throw new Exception("Parameter 'customer' is missing.")
          }

          if (params.contains("products") == false) {
            throw new Exception("Parameter 'products' is missing.")
          }

          val site = params("site").asInstanceOf[String]
          val products = params("products").asInstanceOf[List[Int]]
          
          val customer = params("customer").asInstanceOf[Int]
          val created_at = params("created_at").asInstanceOf[Long]
          
          val response = questor.related_products(site, customer, products, created_at)
          
          sendResponse(channel,request,response)

        }

        case "similar_products" => {
          /*
           * This method determines those products that are most similar to a certain
           * product provided with this request. The request supports the recommendation
           * feature 'more like this' and leverages the product similarity model (PPS)
           * determined prior to this request
           */
          if (params.contains("customer") == false) {
            throw new Exception("Parameter 'customer' is missing.")
          }

          if (params.contains("product") == false) {
            throw new Exception("Parameter 'product' is missing.")
          }

          val site = params("site").asInstanceOf[String]
          val product = params("product").asInstanceOf[Int]
          
          val customer = params("customer").asInstanceOf[Int]
          val created_at = params("created_at").asInstanceOf[Long]
          
          val response = questor.similar_products(site, customer, product, created_at)
          
          sendResponse(channel,request,response)
          
        }
        
        case "top_products" => {
          /*
           * This method determines those products with the highest purchase frequency;
           * we distinguish between requests that provide a certain customer type, and
           * those that do not refer to a certain customer segment 
           */
          val site = params("site").asInstanceOf[String]
          val created_at = params("created_at").asInstanceOf[Long]
          
          val response = if (params.contains("customer")) {
            
            val customer = params("customer").asInstanceOf[Int]
            questor.top_products(site, customer, created_at)
          
          } else questor.top_products(site,created_at)
          
          sendResponse(channel,request,response)
         
        }
        
        case _ => throw new Exception("The method '" + params("method") + "' is not supported.")
        
      }
      
      
    } catch {
      
      case e:Exception => onError(channel,e)
       
    }
    
  }

}