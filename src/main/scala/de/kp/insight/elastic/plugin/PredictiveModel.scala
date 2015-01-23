package de.kp.insight.elastic.plugin
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
case class EsCPR(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,

  /********** USER DATA **********/

  @JsonProperty("user")
  user:String,

  @JsonProperty("item")
  item:Int,

  @JsonProperty("score")
  score:Double,

  @JsonProperty("customer_type")
  customer_type:Int
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsFRQ(

  @JsonProperty("item")
  item:Int,

  @JsonProperty("supp")
  supp:Int
    
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsPOM(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,

  /********** METRIC DATA **********/

  @JsonProperty("total_orders")
  total_orders:Int,

  @JsonProperty("total_amount")
  total_amount:Double,

  @JsonProperty("total_avg_amount")
  total_avg_amount:Double,

  @JsonProperty("total_max_amount")
  total_max_amount:Double,

  @JsonProperty("total_min_amount")
  total_min_amount:Double,

  @JsonProperty("total_stdev_amount")
  total_stdev_amount:Double,

  @JsonProperty("total_variance_amount")
  total_variance_amount:Double,

  @JsonProperty("total_avg_timespan")
  total_avg_timespan:Double,

  @JsonProperty("total_max_timespan")
  total_max_timespan:Double,

  @JsonProperty("total_min_timespan")
  total_min_timespan:Double,

  @JsonProperty("total_stdev_timespan")
  total_stdev_timespan:Double,

  @JsonProperty("total_variance_timespan")
  total_variance_timespan:Double,
  
  @JsonProperty("total_day_supp")
  total_day_supp:List[EsFRQ],
  
  @JsonProperty("total_time_supp")
  total_time_supp:List[EsFRQ],
  
  @JsonProperty("total_item_supp")
  total_item_supp:List[EsFRQ]
  
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsPPS(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,

  /********** ITEM DATA **********/

  @JsonProperty("item")
  item:Int,

  @JsonProperty("other")
  other:Int,

  @JsonProperty("score")
  score:Double,

  @JsonProperty("customer_type")
  customer_type:Int
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsPRF(

  @JsonProperty("item")
  item:Int,

  @JsonProperty("score")
  score:Double
    
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsPPF(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,

  /********** SEGMENT DATA **********/

  @JsonProperty("item")
  item:Int,

  @JsonProperty("customer_supp")
  customer_supp:Int,

  @JsonProperty("purchase_supp")
  purchase_supp:Int,

  @JsonProperty("c_segment")
  c_segment:Int,

  @JsonProperty("p_segment")
  p_segment:Int,

  @JsonProperty("customer_type")
  customer_type:Int
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsPRM(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,

  /********** RULE DATA **********/

  @JsonProperty("antecedent")
  antecedent:Seq[Int],

  @JsonProperty("consequent")
  consequent:Seq[Int],

  @JsonProperty("support")
  support:Int,

  @JsonProperty("total")
  total:Int,

  @JsonProperty("confidence")
  confidence:Double,

  @JsonProperty("customer_type")
  customer_type:Int  
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsBPR(
                
  /********** METADATA **********/

  @JsonProperty("uid")
  uid:String,

  @JsonProperty("timestamp")
  timestamp:Long,

  @JsonProperty("site")
  site:String,

  @JsonProperty("user")
  user:String,
                
  /********** PRODUCT DATA **********/
  
  @JsonProperty("items")
  items:Seq[EsPRF],
  
  @JsonProperty("customer_type")
  customer_type:Int
  
)

@JsonIgnoreProperties(ignoreUnknown = true)
case class EsBPRList(entries:List[EsBPR])
