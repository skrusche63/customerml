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

import org.codehaus.jackson.annotate.JsonProperty
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonIgnore}

@JsonIgnoreProperties(ignoreUnknown = true)
case class MagentoOrder (
    
  /* id of the order */
  @JsonProperty("entity_id")
  entity_id:Int,
  
  /* id of the customer */
  @JsonProperty("customer_id")
  customer_id:String,

  @JsonProperty("base_discount_amount")
  base_discount_amount:Double,

  @JsonProperty("base_shipping_amount")
  base_shipping_amount:Double,

  @JsonProperty("base_shipping_tax_amount")
  base_shipping_tax_amount:Double,

  @JsonProperty("base_subtotal")
  base_subtotal:Double,

  @JsonProperty("base_tax_amount")
  base_tax_amount:Double,

  @JsonProperty("base_total_paid")
  base_total_paid:Double,

  @JsonProperty("base_total_refunded")
  base_total_refunded:Double,

  @JsonProperty("tax_amount")
  tax_amount:Double,

  @JsonProperty("total_paid")
  total_paid:Double,

  @JsonProperty("total_refunded")
  total_refunded:Double,

  @JsonProperty("base_shipping_discount_amount")
  base_shipping_discount_amount:Double,

  @JsonProperty("base_subtotal_incl_tax")
  base_subtotal_incl_tax:Double,

  @JsonProperty("base_total_due")
  base_total_due:Double,

  @JsonProperty("total_due")
  total_due:Double,

  @JsonProperty("base_currency_code")
  base_currency_code:String,

  @JsonProperty("tax_name")
  tax_name:String,

  @JsonProperty("tax_rate")
  tax_rate:String,

  @JsonProperty("addresses")
  addresses:List[MagentoOrderAddress],

  @JsonProperty("order_items")
  order_items:List[MagentoOrderItem]
 
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class MagentoOrders(

  @JsonProperty("orders")
  orders:List[MagentoOrder]
 
)
