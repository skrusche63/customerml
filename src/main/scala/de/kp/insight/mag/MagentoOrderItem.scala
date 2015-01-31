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
case class MagentoOrderItem (
    
  @JsonProperty("sku")
  sku:String,
    
  @JsonProperty("price")
  price:Double,
    
  @JsonProperty("base_price")
  base_price:Double,
    
  @JsonProperty("base_original_price")
  base_original_price:Double,
    
  @JsonProperty("tax_percent")
  tax_percent:Double,
    
  @JsonProperty("tax_amount")
  tax_amount:Double,
    
  @JsonProperty("base_tax_amount")
  base_tax_amount:Double,
    
  @JsonProperty("base_discount_amount")
  base_discount_amount:Double,
    
  @JsonProperty("base_row_total")
  base_row_total:Double,
    
  @JsonProperty("base_price_incl_tax")
  base_price_incl_tax:Double,
    
  @JsonProperty("base_row_total_incl_tax")
  base_row_total_incl_tax:Double

)