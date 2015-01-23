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

class EsCSMBuilder {

  import de.kp.spark.core.Names._
  
  def createBuilder(mapping:String):XContentBuilder = {
  
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              
              .startObject("_id")
                .field("path","id")
              .endObject()
              
              .startObject("properties")
               
                /* 
                 * site:
                 * 
                 * The 'apikey' of the Shopify cloud service is used as a
                 * unique identifier for the respective tenant or website
                 */
                .startObject(SITE_FIELD)
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()

                /* 
                 * id:
                 * 
                 * Unique identifier that designates a certain Shopify
                 * store customer
                 */
                .startObject("id")
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()
              
                /* first_name */
                .startObject("first_name")
                   .field("type", "string")
                .endObject()

                /* last_name */
                .startObject("last_name")
                   .field("type", "string")
                .endObject()

                /* signup_date */
                .startObject("signup_date")
                   .field("type", "string")
                .endObject()

                /* last_sync */
                .startObject("last_sync")
                   .field("type", "long")
                   .field("index", "not_analyzed")
                .endObject()
                
                /* email */
                .startObject("email")
                  .field("type", "string")
                .endObject()

                /* email_verified */
                .startObject("email_verified")
                  .field("type", "boolean")
                .endObject()

                /* accepts_marketing */
                .startObject("accepts_marketing")
                  .field("type", "boolean")
                .endObject()

                /* operational_state */
                .startObject("operational_state")
                  .field("type", "string")
                .endObject()

              .endObject() // properties
            
            .endObject()
          
          .endObject()
    
    builder
  
  }

}