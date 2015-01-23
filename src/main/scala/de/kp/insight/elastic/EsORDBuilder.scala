package de.kp.insight.elastic
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

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class EsORDBuilder {
  
  def createBuilder(mapping:String):XContentBuilder = {
 
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
               .startObject("properties")
               
               /********** METADATA **********/
               
               /* uid */
               .startObject("uid")
                 .field("type", "string")
                 .field("index", "not_analyzed")
               .endObject()

                /* last_sync */
                .startObject("last_sync")
                   .field("type", "long")
                   .field("index", "not_analyzed")
                .endObject()

                /* created_at_min */
                .startObject("created_at_min")
                  .field("type", "long")
                  .field("index", "not_analyzed")
                .endObject()

                /* created_at_max */
                .startObject("created_at_max")
                  .field("type", "long")
                  .field("index", "not_analyzed")
                .endObject()
                    
               /* site */
               .startObject("site")
                  .field("type", "string")
                  .field("index", "not_analyzed")
               .endObject()
               
               /********** ORDER DATA **********/
               
               /* user */
               .startObject("user")
                 .field("type", "string")
               .endObject()
               
               /* amount */
               .startObject("amount")
                 .field("type", "double")
               .endObject()
               
               /* discount */
               .startObject("discount")
                 .field("type", "double")
               .endObject()
               
               /* shipping */
               .startObject("shipping")
                 .field("type", "double")
               .endObject()

               /* timestamp */
               .startObject("timestamp")
                 .field("type", "long")
               .endObject()

               /* group */
               .startObject("group")
                 .field("type", "string")
               .endObject()
               
               /* ip_address */
               .startObject("ip_address")
                 .field("type", "string")
               .endObject()
               
               /* user_agent */
               .startObject("user_agent")
                 .field("type", "string")
               .endObject()

               /* items */
               .startObject("items")
                 .startObject("properties")
                 
                   .startObject("item")
                     .field("type","integer")
                   .endObject()
                   
                   .startObject("quantity")
                     .field("type","integer")
                   .endObject()
                   
                   .startObject("category")
                     .field("type","string")
                   .endObject()
                   
                   .startObject("vendor")
                     .field("type","string")
                   .endObject()
                   
                 .endObject()
               .endObject()
               
               .endObject()
             .endObject()
           .endObject()
           
    builder
    
  }
  
}