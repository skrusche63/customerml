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
case class ShopifyProduct (

  @JsonProperty("id")
  id:Long,

  @JsonProperty("html_body")
  html_body:String,

  @JsonProperty("created_at")
  created_at:String,

  @JsonProperty("product_type")
  product_type:String,

  @JsonProperty("vendor")
  vendor:String,

  @JsonProperty("title")
  title:String,

  /*
   * 'tags' describes a comma separated list of keywords
   * that describe a certain product
   */
  @JsonProperty("tags")
  tags:String,

  @JsonProperty("images")
  images:List[ShopifyProductImage],

  @JsonProperty("variants")
  variants:List[ShopifyProductVariant]

)