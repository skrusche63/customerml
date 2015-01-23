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
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.model._

import org.joda.time.format.DateTimeFormat
import scala.collection.JavaConversions._
/**
 * The ShopifyMapper class is used by the RequestContext to transform
 * the proprietary data representation into an independent one, that
 * can be used to support more than one e-commerce source
 */
class ShopifyMapper(ctx:RequestContext) {

  def extractCustomer(site:String,customer:ShopifyCustomer):Customer = {
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
    
    val created_at = customer.created_at
    /*
     * Retrieve email data from customer
     */
    val emailAddress = customer.email
    val emailVerified = customer.verified_email
    /*
     * Determine marketing indicator and current state
     */
    val marketing = customer.accepts_marketing
    val state = customer.state
    
    Customer(site,user,firstName,lastName,created_at,emailAddress,emailVerified,marketing,state)
    
  }

  def extractProduct(site:String,product:ShopifyProduct):Product = {

    val id = product.id.toString
    val category = product.product_type
    
    val name = product.title
    val vendor = product.vendor
    
    val tags = product.tags
    val images = product.images.map(x => Image(x.id.toString,x.position,x.src))
    
    Product(site,id,category,name,vendor,tags,images)

  }

  /**
   * A public method to extract those fields from a Shopify
   * order that describes an 'Order'
   */
  def extractOrder(site:String,order:ShopifyOrder):Order = {
    
    /*
     * The unique identifier of a certain order is used
     * for grouping all the respective items; the order
     * identifier is a 'Long' and must be converted into
     * a 'String' representation
     */
    val group = order.id.toString
    /*
     * The datetime this order was created:
     * "2014-11-03T13:51:38-05:00"
     */
    val created_at = order.created_at
    val timestamp = toTimestamp(created_at)
    /*
     * The unique user identifier is retrieved from the
     * customer object and there from the 'id' field
     */
    val user = order.customer.id.toString
    /*
     * The IP address is assigned to an order to
     * determine the location dimension associated
     * with this request
     */
    val ip_address = order.client_details.browser_ip
    val user_agent = order.client_details.user_agent
    /*
     * The amount is retrieved from the total price
     * minus the total tax
     */
    val amount = order.total_price.toDouble - order.total_tax.toDouble
    /*
     * The total of discounts associated with this order
     */
    val discounts = order.total_discounts.toDouble
    /*
     * Aggregate the shipping_lines information (sum up)
     * to get a total_shipping attribute
     */
    val shipping = order.shipping_lines.map(x => x.price.toDouble).sum
    /*
     * Convert all line items of the respective order
     * into 'OrderItem' for indexing
     */
    val items = order.lineItems.map(lineItem => {
      /*
       * A shopify line item holds 3 different identifiers:
       * 
       * - 'id' specifies the unique identifier for this item,
       * 
       * - 'variant_id' specifies the product variant uniquely
       * 
       * - 'product_id' specifies the product uniquely
       * 
       * For further mining and prediction tasks, the 'product_id'
       * is used to uniquely identify a purchase item
       */
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
      
      val currency = order.currency
      val price = lineItem.price
      
      val sku = lineItem.sku
      
      new OrderItem(item,name,quantity,category,vendor,currency,price,sku)
    
    })

    Order(site,user,ip_address,user_agent,timestamp,group,amount,discounts,shipping,items)
  
  }
 
  private def toTimestamp(text:String):Long = {
      
    //2014-11-03T13:51:38-05:00
    val pattern = "yyyy-MM-dd'T'HH:mm:ssZ"
    val formatter = DateTimeFormat.forPattern(pattern)
      
    val datetime = formatter.parseDateTime(text)
    datetime.toDate.getTime
    
  }  
}