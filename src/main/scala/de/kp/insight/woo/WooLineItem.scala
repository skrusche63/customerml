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
case class WooLineItem (
   
  @JsonProperty("product_id")
  product_id:Int,
   
  @JsonProperty("quantity")
  quantity:Int,
   
  @JsonProperty("id")
  id:Int,
   
  @JsonProperty("sub_total")
  sub_total:String,
   
  @JsonProperty("price")
  price:String,
   
  @JsonProperty("tax_class")
  tax_class:String,
   
  @JsonProperty("sku")
  sku:String,
   
  @JsonProperty("total")
  total:String,
   
  @JsonProperty("name")
  name:String,
   
  @JsonProperty("total_tax")
  total_tax:String
)