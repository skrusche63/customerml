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
case class WooCustomer(
   
  @JsonProperty("id")
  id:Int,
   
  @JsonProperty("last_order_date")
  last_order_date:String,
   
  @JsonProperty("avatar_url")
  avatar_url:String,
   
  @JsonProperty("total_spent")
  total_spent:String,
   
  @JsonProperty("created_at")
  created_at:String,
   
  @JsonProperty("orders_count")
  orders_count:Int,
   
  @JsonProperty("billing_address")
  billing_address:WooBillingAddress,
   
  @JsonProperty("shipping_address")
  shipping_address:WooBillingAddress,
   
  @JsonProperty("first_name")
  first_name:String,
   
  @JsonProperty("username")
  username:String,
   
  @JsonProperty("last_name")
  last_name:String,
   
  @JsonProperty("last_order_id")
  last_order_id:String,
   
  @JsonProperty("email")
  email:String
  
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class WooCustomers (

  @JsonProperty("customers")
  customers:List[WooCustomer]
 
)