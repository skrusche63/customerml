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
case class ShopifyCollection(
  
  /* 
   * The unique numeric identifier for the custom collection 
   */
  @JsonProperty("id")
  id:Long,
  /*
   * The name of the custom collection. Limit of 255 characters.
   */
  @JsonProperty("title")
  title:String,
  /*
   * The description of the custom collection, complete with HTML markup. 
   * Many templates display this on their custom collection pages.
   */
  @JsonProperty("body_html")
  body_html:String,
  /*
   * A human-friendly unique string for the custom collection automatically 
   * generated from its title. This is used in shop themes by the Liquid 
   * templating language to refer to the custom collection. Limit of 255 
   * characters.
   */
  @JsonProperty("handle")
  handle:String,
  /*
   * This can have two different types of values, depending on whether the 
   * custom collection has been published (i.e., made visible to customers):
   *
   * If the custom collection is published, this value is the date and time 
   * when it was published. The API returns this value in ISO 8601 format.
   * 
   * If the custom collection is hidden (i.e., not published), this value is 
   * null. Changing a custom collection's status from published to hidden 
   * changes its published_at property to null.
   */
  @JsonProperty("published_at")
  published_at:String,
  /*
   * The date and time when the custom collection was last modified. 
   * The API returns this value in ISO 8601 format.
   */
  @JsonProperty("updated_at")
  updated_at:String,
  /*
   * The sales channels in which the custom collection is visible.
   */
  @JsonProperty("published_scope")
  published_scope:String,
  /*
   * The order in which products in the custom collection appear. 
   * Valid values are:
   * 
   * alpha-asc: Alphabetically, in ascending order (A - Z).
   * alpha-desc: Alphabetically, in descending order (Z - A).
   * best-selling: By best-selling products.
   * created: By date created, in ascending order (oldest - newest).
   * created-desc: By date created, in descending order (newest - oldest).
   * manual: Order created by the shop owner.
   * price-asc: By price, in ascending order (lowest - highest).
   * price-desc: By price, in descending order (highest - lowest).
   */
  @JsonProperty("sort_order")
  sort_order:String,

  @JsonProperty("products_count")
  products_count:Long,

  @JsonProperty("image")
  image:ShopifyProductImage
  
)