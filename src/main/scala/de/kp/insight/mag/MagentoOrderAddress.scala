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
case class MagentoOrderAddress (
    
  @JsonProperty("firstname")
  firstname:String,
    
  @JsonProperty("lastname")
  lastname:String,
    
  @JsonProperty("middlename")
  middlename:String,
    
  @JsonProperty("prefix")
  prefix:String,
    
  @JsonProperty("suffix")
  suffix:String,
    
  @JsonProperty("email")
  email:String,
    
  @JsonProperty("company")
  company:String,
    
  @JsonProperty("postcode")
  postcode:String,
    
  @JsonProperty("city")
  city:String,
    
  @JsonProperty("street")
  street:String,
    
  @JsonProperty("telephone")
  telephone:String,
    
  @JsonProperty("address_type")
  address_type:String,
    
  @JsonProperty("region")
  region:String,
     
  @JsonProperty("country_id")
  country_id:String
)