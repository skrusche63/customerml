package de.kp.insight.model
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

case class InsightOrderItem(
  
  item:Int,
  quantity:Int,

  category:String,
  vendor:String
  
)

case class InsightOrder(
               
  /********** METADATA **********/
  
  uid:String,  
  last_sync:Long,

  created_at_min:Long,
  created_at_max:Long,
  
  site:String,

  /********** ORDER DATA **********/
  
  user:String,
  
  amount:Double,
  discount:Double,

  shipping:Double,
  timestamp:Long,
  
  group:String,
  
  ip_address:String,  
  user_agent:String,
  
  items:List[InsightOrderItem]
  
)