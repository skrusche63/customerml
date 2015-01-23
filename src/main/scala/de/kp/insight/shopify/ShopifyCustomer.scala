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
case class ShopifyCustomer(

  @JsonProperty("id")
  id:Long,

  @JsonProperty("email")
  email:String,

  @JsonProperty("first_name")
  first_name:String,
    
  @JsonProperty("last_name")
  last_name:String,
  /*
   * The signup date of the customer
   */
  @JsonProperty("created_at")
  created_at:String,

  @JsonProperty("default_address")
  default_address:ShopifyAddress,

  /*
   * The customer data below are used to identify
   * those customers that have been made an order
   * within a certain period of time
   */
 
  @JsonProperty("accepts_marketing")
  accepts_marketing:Boolean,
 
  @JsonProperty("verified_email")
  verified_email:Boolean,
 
  @JsonProperty("state")
  state:String,
 
  @JsonProperty("last_order_id")
  last_order_id:Long,
 
  @JsonProperty("orders_count")
  orders_count:Long,
 
  @JsonProperty("total_spent")
  total_spent:String

)