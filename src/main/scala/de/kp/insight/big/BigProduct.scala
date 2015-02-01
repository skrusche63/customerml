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
case class BigProduct(

  @JsonProperty("id")
  id:Int,

  @JsonProperty("name")
  name:String,

  @JsonProperty("type")
  product_type:String,

  @JsonProperty("description")
  description:String,

  @JsonProperty("search_keywords")
  search_keywords:String,

  @JsonProperty("price")
  price:String,

  @JsonProperty("cost_price")
  cost_price:String,

  @JsonProperty("retail_price")
  retail_price:String,

  @JsonProperty("sale_price")
  sale_price:String,

  @JsonProperty("calculated_price")
  calculated_price:String,

  /*
   * The following fields are not supported
   *
   * @JsonProperty("keyword_filter")
   * keyword_filter:String,
   *  
   * @JsonProperty("availability_description")
   * availability_description:String,
   *  
   * @JsonProperty("sort_order")
   * sort_order:Int, 
   *
   * @JsonProperty("is_visible")
   * is_visible:Boolean, 
   *
   * @JsonProperty("is_featured")
   * is_featured:Boolean,
   *
   * @JsonProperty("related_products")
   * related_products:String,
   *
   * @JsonProperty("inventory_level")
   * inventory_level:Int,
   *
   * @JsonProperty("inventory_warning_level")
   * inventory_warning_level:Int,
   * 
   * @JsonProperty("warranty")
   * warranty:String,
   *
   * @JsonProperty("weight")
   * weight:String,
   *
   * @JsonProperty("width")
   * width:String,
   *
   * @JsonProperty("height")
   * height:String,
   *
   * @JsonProperty("depth")
   * depth:String,
   * 
   * @JsonProperty("meta_keywords")
   * meta_keywords:String,
   * 
   * @JsonProperty("meta_description")
   * meta_description:String,
   * 
   * @JsonProperty("layout_file")
   * layout_file:String,
   * 
   * @JsonProperty("is_price_hidden")
   * is_price_hidden:Boolean,
   * 
   * @JsonProperty("price_hidden_label")
   * price_hidden_label:String,
   * 
   *  @JsonProperty("date_modified")
   * date_modified:String,
   * 
   * @JsonProperty("event_date_field_name")
   * event_date_field_name:String,
   * 
   * @JsonProperty("event_date_type")
   * event_date_type:String,
   * 
   * @JsonProperty("event_date_start")
   * event_date_start:String,
   * 
   * @JsonProperty("event_date_end")
   * event_date_end:String,
   * 
   * @JsonProperty("myob_asset_account")
   * myob_asset_account:String,
   * 
   * @JsonProperty("myob_income_account")
   * myob_income_account:String,
   * 
   * @JsonProperty("myob_expense_account")
   * myob_expense_account:String,
   * 
   * @JsonProperty("peachtree_gl_account")
   * peachtree_gl_account:String,
   * 
   * @JsonProperty("is_condition_shown")
   * is_condition_shown:Boolean,
   * 
   * @JsonProperty("preorder_release_date")
   * preorder_release_date:String,
   * 
   * @JsonProperty("is_preorder_only")
   * is_preorder_only:Boolean,
   * 
   * @JsonProperty("preorder_message")
   * preorder_message:String,
   * 
   * @JsonProperty("order_quantity_minimum")
   * order_quantity_minimum:Int,
   * 
   * @JsonProperty("order_quantity_maximum")
   * order_quantity_maximum:Int,
   * 
   * @JsonProperty("open_graph_type")
   * open_graph_type:String,
   * 
   * @JsonProperty("open_graph_title")
   * open_graph_title:String,
   * 
   * @JsonProperty("open_graph_description")
   * open_graph_description:String,
   * 
   * @JsonProperty("is_open_graph_thumbnail")
   * is_open_graph_thumbnail:Boolean,
   * 
   * @JsonProperty("upc")
   * upc:String,
   * 
   * @JsonProperty("date_last_imported")
   * date_last_imported:String,
   * 
   * @JsonProperty("option_set_id")
   * option_set_id:Int,
   *     
   * @JsonProperty("configurable_fields")
   * configurable_fields:BigReference,
   *   
   * @JsonProperty("custom_fields")
   * custom_fields:BigReference,
   *   
   * @JsonProperty("videos")
   * videos:BigReference,
   *   
   * @JsonProperty("skus")
   * skus:BigReference,
   *   
   * @JsonProperty("rules")
   * rules:BigReference,
   * 
   * option_set field is ignored
   * 
   * @JsonProperty("options")
   * options:BigReference,
   * 
   * @JsonProperty("tax_class_id")
   * tax_class_id:Int,
   *
   * @JsonProperty("option_set_display")
   * option_set_display:String,
   *
   * @JsonProperty("bin_picking_number")
   * bin_picking_number:String,
   *
   * @JsonProperty("custom_url")
   * custom_url:String,
   *
   * @JsonProperty("primary_image")
   * primary_image:BigImage,
   * 
   * @JsonProperty("fixed_cost_shipping_price")
   * fixed_cost_shipping_price:String,
   * 
   * @JsonProperty("is_free_shipping")
   * is_free_shipping:Boolean,
   * 
   * @JsonProperty("inventory_tracking")
   * inventory_tracking:String,
   * 
   * @JsonProperty("rating_total")
   * rating_total:Int,
   * 
   * @JsonProperty("rating_count")
   * rating_count:Int,
   *
   * @JsonProperty("discount_rules")
   * discount_rules:BigReference,
   *
   * @JsonProperty("availability")
   * availability:String,
   * 
   */
  @JsonProperty("total_sold")
  total_sold:Int,

  @JsonProperty("date_created")
  date_created:String,

  @JsonProperty("brand_id")
  brand_id:Int,

  @JsonProperty("view_count")
  view_count:Int,

  @JsonProperty("page_title")
  page_title:String,

  @JsonProperty("categories")
  categories:List[Int],

  @JsonProperty("condition")
  condition:String,
  
  @JsonProperty("brand")
  brand:BigReference,
  
  @JsonProperty("images")
  images:BigReference,
  
  @JsonProperty("tax_class")
  tax_class:BigReference

)