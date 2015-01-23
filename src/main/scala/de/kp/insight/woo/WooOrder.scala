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

import org.codehaus.jackson.annotate.JsonProperty
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonIgnore}

@JsonIgnoreProperties(ignoreUnknown = true)
case class WooOrder (

  @JsonProperty("id")
  id:Int,

  @JsonProperty("order_number")
  order_number:Int,

  @JsonProperty("created_at")
  created_at:String,

  @JsonProperty("updated_at")
  updated_at:String,

  @JsonProperty("completed_at")
  completed_at:String,

  @JsonProperty("status")
  status:String,

  @JsonProperty("currency")
  currency:String,

  @JsonProperty("total")
  total:String,
  
  /*
   * The variable 'total_line_items_quantity'
   * can be computed and must not be published
   * as part of the JSON dat structure
   * 
   * @JsonProperty("total_line_items_quantity")
   * total_line_items_quantity:Int,
   * 
   */
  @JsonProperty("total_tax")
  total_tax:String,

  @JsonProperty("total_shipping")
  total_shipping:String,

  @JsonProperty("cart_tax")
  cart_tax:String,

  @JsonProperty("shipping_tax")
  shipping_tax:String,

  @JsonProperty("total_discount")
  total_discount:String,

  @JsonProperty("cart_discount")
  cart_discount:String,

  @JsonProperty("order_discount")
  order_discount:String,

  /*
   * The variable 'shipping_methods' is actually
   * not supported, as we have to restrict to 22
   * fields (see Scala case class restriction)
   * 
   * @JsonProperty("shipping_methods")
   * shipping_methods:String,
   * 
   * The variable 'payment_details' is actually
   * not supported, as we have to restrict to 22
   * fields (see Scala case class restriction)
   * 
   * @JsonProperty("payment_details")
   * payment_details:WooPaymentDetails,
   * 
   */
  @JsonProperty("billing_address")
  billing_address:WooBillingAddress,

  @JsonProperty("shipping_address")
  shipping_address:WooShippingAddress,
  /*
   * The variable 'note' is actually not supported, 
   * as we have to restrict to 22 fields (see Scala 
   * case class restriction)
   * 
   * @JsonProperty("note")
   * note:String,
   * 
   * The variable 'view_order_url' is actually
   * not supported, as we have to restrict to 22
   * fields (see Scala case class restriction)
   * 
   * @JsonProperty("view_order_url")
   * view_order_url:String,
   * 
   */

  @JsonProperty("customer_ip")
  customer_ip:String,

  @JsonProperty("customer_user_agent")
  customer_user_agent:String,

  @JsonProperty("customer_id")
  customer_id:String,

  @JsonProperty("line_items")
  line_items:List[WooLineItem],
  /*
   * The variable 'shipping_lines' is actually
   * not supported, as we have to restrict to 22
   * fields (see Scala case class restriction)
   * 
   * @JsonProperty("shipping_lines")
   * shipping_lines:List[WooShippingLine],
   * 
   * The variable 'tax_lines' is actually
   * not supported, as we have to restrict to 22
   * fields (see Scala case class restriction)
   * 
   * @JsonProperty("fee_lines")
   *fee_lines:WooFeeLine,
   * 
   * The variable 'coupon_lines' is actually
   * not supported, as we have to restrict to 22
   * fields (see Scala case class restriction)
   * 
   * @JsonProperty("coupon_lines")
   * coupon_lines:WooCouponLine,
   * 
   */
  @JsonProperty("customer")
  customer:WooCustomer

)

@JsonIgnoreProperties(ignoreUnknown = true)
case class WooOrders(

  @JsonProperty("orders")
  orders:List[WooOrder]
 
)
