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
case class WooProduct (
   
  @JsonProperty("id")
  id:Int,
   
  @JsonProperty("title")
  title:String,
   
  @JsonProperty("sku")
  sku:String,
   
  @JsonProperty("price")
  price:String,
   
  @JsonProperty("regular_price")
  regular_price:String,
   
  @JsonProperty("sale_price")
  sale_price:String,

  @JsonProperty("created_at")
  created_at:String,

  @JsonProperty("updated_at")
  updated_at:String,

  @JsonProperty("stock_quantity")
  stock_quantity:Int,

  @JsonProperty("rating_count")
  rating_count:Int,

  @JsonProperty("total_sales")
  total_sales:Int,

  @JsonProperty("in_stock")
  in_stock:Boolean,

  @JsonProperty("average_rating")
  average_rating:String,

  @JsonProperty("cross_sell_ids")
  cross_sell_ids:List[Int],

  @JsonProperty("related_ids")
  related_ids:List[Int],

  @JsonProperty("upsell_ids")
  upsell_ids:List[Int],

  /*
   * Flags that are actually not supported
   *
   * 
   * @JsonProperty("shipping_required")
   * shipping_required:Boolean,
   *
   * @JsonProperty("managing_stock")
   * managing_stock:Boolean,
   *
   * @JsonProperty("backordered")
   * backordered:Boolean, 
   *
   * @JsonProperty("on_sale")
   * on_sale:Boolean, 
   *
   * @JsonProperty("purchaseable")
   * purchaseable:Boolean, 
   *
   * @JsonProperty("visible")
   * visible:Boolean,
   *
   * @JsonProperty("backorders_allowed")
   * backorders_allowed:Boolean,
   *
   * @JsonProperty("featured")
   * featured:Boolean,
   *
   * @JsonProperty("reviews_allowed")
   * reviews_allowed:Boolean,
   *
   * @JsonProperty("virtual")
   * virtual:Boolean,
   *
   * @JsonProperty("downloadable")
   * downloadable:Boolean,
   *
   * @JsonProperty("taxable")
   * taxable:Boolean,
   *
   * @JsonProperty("sold_individually")
   * sold_individually:Boolean,
   *   
   */
  
  @JsonProperty("shipping_taxable")
  shipping_taxable:Boolean,
  
  @JsonProperty("images")
  images:List[WooImage],
  /*
   * Fields that are actually not supported
   *
   * 
   * @JsonProperty("variations")
   * variations:List[Any],
   * 
   * @JsonProperty("categories")
   * categories:List[String],
   * 
   * @JsonProperty("permalink")
   * permalink:String,
   * 
   * @JsonProperty("attributes")
   * attributes:List[Any],
   * 
   * @JsonProperty("download_limit")
   * download_limit:Int,
   * 
   * @JsonProperty("description")
   * description:String,
   * 
   * @JsonProperty("price_html")
   * price_html:String,
   * 
   * @JsonProperty("short_description")
   * short_description:String,
   * 
   * @JsonProperty("purchase_note")
   * purchase_note:String,
   * 
   * @JsonProperty("downloads")
   * downloads:List[Any],
   * 
   * @JsonProperty("download_type")
   * download_type:String,
   * 
   * @JsonProperty("catalog_visibility")
   * catalog_visibility:String,
   * 
   * @JsonProperty("download_expiry")
   * download_expiry:Int,
   * 
   * @JsonProperty("parent")
   * parent:List[Any],
   * 
   * @JsonProperty("type")
   * type:String,
   *  
   * @JsonProperty("dimensions")
   * dimensions:WooDimensions,
   * 
   * @JsonProperty("weight")
   * weight:String,
   *  
   * @JsonProperty("shipping_class")
   * shipping_class:String,
   * 
   * @JsonProperty("shipping_class_id")
   * shipping_class_id:String,
   *   
   */
  
  @JsonProperty("tags")
  tags:List[String],
  
  @JsonProperty("tax_class")
  tax_class:String,
  
  @JsonProperty("tax_status")
  tax_status:String,
  
  @JsonProperty("status")
  status:String
  
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class WooProducts(

  @JsonProperty("products")
  products:List[WooProduct]
 
)