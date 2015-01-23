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
case class ShopifyAddress(

  @JsonProperty("address1")
  address1:String,

  @JsonProperty("address2")
  address2:String,

  @JsonProperty("city")
  city:String,

  @JsonProperty("company")
  company:String,

  @JsonProperty("country")
  country:String,

  @JsonProperty("first_name")
  firstName:String,

  @JsonProperty("last_name")
  lastName:String,

  @JsonProperty("phone")
  phone:String,

  @JsonProperty("province")
  province:String,

  @JsonProperty("zip")
  zip:String,

  @JsonProperty("name")
  name:String,

  @JsonProperty("country_code")
  countryCode:String,

  @JsonProperty("province_code")
  provinceCode:String

)