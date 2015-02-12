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

import org.codehaus.jackson.annotate.JsonProperty
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonIgnore}

@JsonIgnoreProperties(ignoreUnknown = true)
case class BigOrder(

  @JsonProperty("id")
  id:Int,

  @JsonProperty("date_created")
  date_created:String,

  @JsonProperty("ip_address")
  ip_address:String,

  @JsonProperty("currency_code")
  currency_code:String,

  @JsonProperty("items_total")
  items_total:Int,
  
  @JsonProperty("customer_id")
  customer_id:Int,
  
  @JsonProperty("status")
  status:String,

  /***** TOTAL COST *****/
  
  @JsonProperty("total_ex_tax")
  total_ex_tax:String,

  /***** TOTAL TAX *****/

  @JsonProperty("total_tax")
  total_tax:String,

  /***** SUB TOTAL *****/
  
  @JsonProperty("subtotal_ex_tax")
  subtotal_ex_tax:String,

  @JsonProperty("subtotal_tax")
  subtotal_tax:String,

  /***** SHIPPING COST *****/
  
  /*
   * Shipping costs are taken into account, excluding any
   * taxes associated with shipping; 'tax' is considered
   * as aggregated value 'total_tax'
   * 
   */
  @JsonProperty("shipping_cost_ex_tax")
  shipping_cost_ex_tax:String,

  /***** HANDLING COST *****/

  /*
   * Handling costs are taken into account, excluding any
   * taxes associated with handling; 'tax' is considered
   * as aggregated value 'total_tax'
   * 
   */
  @JsonProperty("handling_cost_ex_tax")
  handling_cost_ex_tax:String,

  /***** WRAPPING COST *****/
  
  /*
   * Wrapping costs are costs for wrapping gifts; we take
   * those costs into account, ecluding any associated taxes.
   * 'tax' is considered as aggregated value 'total_tax'
   * 
   */

  @JsonProperty("wrapping_cost_ex_tax")
  wrapping_cost_ex_tax:String,

  @JsonProperty("store_credit_amount")
  store_credit_amount:String,

  @JsonProperty("gift_certificate_amount")
  gift_certificate_amount:String,

  @JsonProperty("discount_amount")
  discount_amount:String,

  @JsonProperty("coupon_discount")
  coupon_discount:String
  
)
