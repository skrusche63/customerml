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

import de.kp.insight._
import de.kp.insight.model._

import scala.collection.mutable.{Buffer,HashMap}

class BigContext(ctx:RequestContext) extends ShopContext {
  /*
   * The 'apikey' is used as the 'site' parameter when indexing
   * Bigcommerce data with Elasticsearch
   */
  def getSite:String = null

  /**
   * A public method to retrieve Bigcommerce customers from the REST
   * interface; this method is used to synchronize the customer base
   */
  def getCustomers(params:Map[String,String]):List[Customer] = null
  
  /**
   * A public method to retrieve the Bigcommerce orders of the last 30, 
   * 60 or 90 days from the REST interface
   */
  def getOrders(params:Map[String,String]):List[Order] = null

  /**
   * A public method to retrieve Bigcommerce products from the REST 
   * interface; this method is used to synchronize the product base
   */
  def getProducts(params:Map[String,String]):List[Product] = null
  
}