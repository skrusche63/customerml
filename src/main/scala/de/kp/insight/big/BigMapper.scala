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
import de.kp.spark.core.model._

import de.kp.insight._
import de.kp.insight.model._

import scala.collection.JavaConversions._

class BigMapper(ctx:RequestContext) {

  private val shopContext = new BigContext(ctx)

  def extractCustomer(site:String,customer:BigCustomer):Customer = {
    /*
     * The unique user identifier is retrieved from the
     * customer object and there from the 'id' field
     */
    val user = customer.id.toString
    /*
     * Retrieve the first & last name of a customer
     */
    val firstName = customer.first_name
    val lastName  = customer.last_name
    
    val created_at = DateUtil.unformatted(customer.date_created,DateUtil.BIG_COMMERCE)
    /*
     * Retrieve email data from customer
     */
    val emailAddress = customer.email
    /*
     * The subsequent parameters, emailVerified, marketing, 
     * and state is not used by WooCommerce and will be set
     * to default values 
     */
    val emailVerified = true
    val marketing = true
    
    val state = "not_used"
    
    Customer(site,user,firstName,lastName,created_at,emailAddress,emailVerified,marketing,state)

  }

  def extractOrder(site:String,order:BigOrder):Order = {
    
    /*
     * The unique identifier of a certain order is used
     * for grouping all the respective items; the order
     * identifier is an 'Int' and must be converted into
     * a 'String' representation
     */
    val group = order.id.toString

    val created_at = order.date_created
    val timestamp = toTimestamp(created_at)
    /*
     * The unique user identifier is retrieved from the
     * customer object and there from the 'id' field
     */
    val user = order.customer_id.toString
    /*
     * The IP address is assigned to an order to
     * determine the location dimension associated
     * with this request
     */
    val ip_address = order. ip_address
    val user_agent = "not_used"
    /*
     * The amount is retrieved from the total price
     * minus the total tax
     */
    val amount = order.total_ex_tax.toDouble
    /*
     * The total of discounts associated with this order
     */
    val discounts = order.discount_amount.toDouble
    /*
     * The shipping costs excluding tax
     */
    val shipping = order.shipping_cost_ex_tax.toDouble
    
    /*
     * Retrieve all line items for this order
     */
    val lineItems = shopContext.getLineItems(order.id)
    /*
     * Convert all line items of the respective order
     * into 'OrderItem' for indexing
     */
    val items = lineItems.map(lineItem => {

      val item = lineItem.product_id.toInt
      /*
       * The product database is accessed to retrieve additional
       * category and vendor data to enrich the OrderItem record
       */
      val product_str = ctx.getAsString("products", "base", item.toString)
      val product = ctx.JSON_MAPPER.readValue(product_str, classOf[InsightProduct])

      val category = product.category
      val vendor = product.vendor
      /*
       * In addition, we collect the following data from the line item
       */
      val name = lineItem.name
      val quantity = lineItem.quantity
      
      val currency = order.currency_code
      val price = lineItem.price_ex_tax.toDouble
      
      val sku = lineItem.sku
      
      new OrderItem(item,name,quantity,category,vendor,currency,price,sku)
    
    })

    Order(site,user,ip_address,user_agent,timestamp,group,amount,discounts,shipping,items)
    
  }
  
  def extractProduct(site:String,product:BigProduct):Product = {

    val id = product.id.toString
    val category = product.product_type
    
    val name = product.name
    /*
     * Retrieve brand from Bigcommerce shop
     */
    val brand = shopContext.getBrand(product.brand_id)
    val vendor = brand.name
    
    val tags = product.search_keywords
    val images = shopContext.getImages(product.id).map(x => Image(x.id.toString,x.sort_order,x.standard_url))
    
    Product(site,id,category,name,vendor,tags,images)
    
  }

  private def toTimestamp(text:String):Long = DateUtil.unformatted(text,DateUtil.BIG_COMMERCE)

}