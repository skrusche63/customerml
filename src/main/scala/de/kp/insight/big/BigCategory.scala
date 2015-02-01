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
case class BigCategory(

  @JsonProperty("id")
  id:Int,

  @JsonProperty("parent_id")
  parent_id:Int,

  @JsonProperty("name")
  name:String,

  @JsonProperty("description")
  description:String,

  @JsonProperty("sort_order")
  sort_order:Int,

  @JsonProperty("page_title")
  page_title:String,

  @JsonProperty("meta_keywords")
  meta_keywords:String,

  @JsonProperty("meta_description")
  meta_description:String,

  @JsonProperty("layout_file")
  layout_file:String,

  @JsonProperty("image_file")
  image_file:String,

  @JsonProperty("is_visible")
  is_visible:Boolean,

  @JsonProperty("search_keywords")
  search_keywords:String,

  @JsonProperty("url")
  url:String,

  @JsonProperty("parent_category_list")
  parent_category_list:List[Int]

)