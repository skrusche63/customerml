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

import org.codehaus.jackson.annotate.JsonProperty
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonIgnore}

@JsonIgnoreProperties(ignoreUnknown = true)
case class ShopifyOrder(

  @JsonProperty("id")
  id:Long,

  /*
   * The datetime this order was created; the format used is:
   * "2014-11-03T13:51:38-05:00"
   */
  
  @JsonProperty("created_at")
  created_at:String,

  @JsonProperty("currency")
  currency:String,

  /*
   * The number of items in an order; it should be equal to
   * the size of the provided line items
   */
  @JsonProperty("number")
  number:Int,

  @JsonProperty("total_discounts")
  total_discounts:String,

  @JsonProperty("total_price")
  total_price:String,

  @JsonProperty("total_tax")
  total_tax:String,

  @JsonProperty("subtotal_price")
  subtotal_price:String,
  
  @JsonProperty("client_details")
  client_details:ShopifyClientDetails,
  
  @JsonProperty("customer")
  customer:ShopifyCustomer,

  @JsonProperty("line_items")
  lineItems:List[ShopifyLineItem],

  @JsonProperty("shipping_lines")
  shipping_lines:List[ShopifyShippingItem],
  
  @JsonProperty("tax_lines")
  taxLines:List[ShopifyTaxLine]
 
)