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
case class BigBillingAddress(

  @JsonProperty("first_name")
  first_name:String,

  @JsonProperty("last_name")
  last_name:String,

  @JsonProperty("company")
  company:String,

  @JsonProperty("street_1")
  street_1:String,

  @JsonProperty("street_2")
  street_2:String,

  @JsonProperty("city")
  city:String,

  @JsonProperty("state")
  state:String,

  @JsonProperty("zip")
  zip:String,

  @JsonProperty("country")
  country:String,

  @JsonProperty("country_iso2")
  country_iso2:String,

  @JsonProperty("phone")
  phone:String,

  @JsonProperty("email")
  email:String

)
