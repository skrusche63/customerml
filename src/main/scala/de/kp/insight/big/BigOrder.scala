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

  @JsonProperty("customer_id")
  customer_id:Int,

  @JsonProperty("date_created")
  date_created:String,

  /*
   * The following parameters are ignored
   *
   * @JsonProperty("date_modified")
   * date_modified:String,
   *
   * @JsonProperty("date_shipped")
   * date_shipped:String,
   *
   * @JsonProperty("status_id")
   * status_id:Int,
   * 
   * @JsonProperty("shipping_cost_tax_class_id")
   * shipping_cost_tax_class_id:Int,
   * 
   * @JsonProperty("handling_cost_tax_class_id")
   * handling_cost_tax_class_id:Int,
   * 
   * @JsonProperty("wrapping_cost_tax_class_id")
   * wrapping_cost_tax_class_id:Int,
   * 
   * @JsonProperty("payment_provider_id")
   * payment_provider_id:Int,
   * 
   * @JsonProperty("geoip_country")
   * geoip_country:String, 
   *
   * @JsonProperty("geoip_country_iso2")
   * geoip_country_iso2:String, 
   *
   * @JsonProperty("currency_id")
   * currency_id:Int,
   * 
   * @JsonProperty("default_currency_id")
   * default_currency_id:Int, 
   *
   * @JsonProperty("default_currency_code")
   * default_currency_code:String,
   *
   * @JsonProperty("staff_notes")
   * staff_notes:String, 
   *
   * @JsonProperty("customer_message")
   * customer_message:String,
   * 
   * @JsonProperty("order_is_digital")
   * order_is_digital:Boolean,
   * 
   * @JsonProperty("is_deleted")
   * is_deleted:Boolean,
   * 
   * @JsonProperty("currency_exchange_rate")
   * currency_exchange_rate:String,
   * 
   * @JsonProperty("shipping_address_count")
   * shipping_address_count:Int,
   *
   * @JsonProperty("billing_address")
   * billing_address:BigBillingAddress,
   * 
   * @JsonProperty("shipping_addresses")
   * shipping_addresses:BigShippingAddresses,
   * 
   * @JsonProperty("items_shipped")
   * items_shipped:Int,
   * 
   * @JsonProperty("payment_method")
   * payment_method:String,
   *
   * @JsonProperty("payment_status")
   * payment_status:String,
   * 
   * @JsonProperty("coupons")
   * coupons:BigCoupons
   * 
   */
  
  @JsonProperty("status")
  status:String,

  /***** TOTAL COST *****/
  
  @JsonProperty("total_ex_tax")
  total_ex_tax:String,

//  @JsonProperty("total_inc_tax")
//  total_inc_tax:String,

  /***** TOTAL TAX *****/

  @JsonProperty("total_tax")
  total_tax:String,

  /***** SUB TOTAL *****/
  
  @JsonProperty("subtotal_ex_tax")
  subtotal_ex_tax:String,

//  @JsonProperty("subtotal_inc_tax")
//  subtotal_inc_tax:String,

  @JsonProperty("subtotal_tax")
  subtotal_tax:String,

  /***** SHIPPING COST *****/
  
  /*
   * Shipping costs are taken into account, excluding any
   * taxes associated with shipping; 'tax' is considered
   * as aggregated value 'total_tax'
   * 
   *
   * @JsonProperty("base_shipping_cost")
   * base_shipping_cost:String,
   *
   * @JsonProperty("shipping_cost_inc_tax")
   * shipping_cost_inc_tax:String, 
   *
   * @JsonProperty("shipping_cost_tax")
   * shipping_cost_tax:String,
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
   *
   * @JsonProperty("base_handling_cost")
   * base_handling_cost:String,
   *
   * @JsonProperty("handling_cost_inc_tax")
   * handling_cost_inc_tax:String,
   *
   * @JsonProperty("handling_cost_tax")
   * handling_cost_tax:String,
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
   * 
   * @JsonProperty("base_wrapping_cost")
   * base_wrapping_cost:String,
   *
   * @JsonProperty("wrapping_cost_inc_tax")
   * wrapping_cost_inc_tax:String,
   *
   * @JsonProperty("wrapping_cost_tax")
   * wrapping_cost_tax:String,
   * 
   */

  @JsonProperty("wrapping_cost_ex_tax")
  wrapping_cost_ex_tax:String,

  @JsonProperty("items_total")
  items_total:Int,

//  @JsonProperty("refunded_amount")
//  refunded_amount:String,

  @JsonProperty("store_credit_amount")
  store_credit_amount:String,

  @JsonProperty("gift_certificate_amount")
  gift_certificate_amount:String,

  @JsonProperty("ip_address")
  ip_address:String,

  @JsonProperty("currency_code")
  currency_code:String,

  @JsonProperty("discount_amount")
  discount_amount:String,

  @JsonProperty("coupon_discount")
  coupon_discount:String,

  @JsonProperty("products")
  products:BigProducts
)
