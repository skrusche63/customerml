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

class EsPRDBuilder {

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

                /* id */
                .startObject("id")
                   .field("type", "string")
                   .field("index", "not_analyzed")
                .endObject()
              
                /* name */
                .startObject("name")
                   .field("type", "string")
                .endObject()
              
                /* 
                 * category:
                 * 
                 * The category assigned to a certain product, this field
                 * can be used to group similar products or to determine
                 * customer preferences
                 */
                .startObject("category")
                   .field("type", "string")
                .endObject()

                /* tags:
                 * 
                 * 'tags' describes a comma separated list of keywords
                 * that describe a certain product
                 */
                .startObject("tags")
                   .field("type", "string")
                .endObject()

                /* images */
                .startObject("images")
                  .startObject("properties")

                    .startObject("id")
                      .field("type","string")
                    .endObject

                    .startObject("position")
                      .field("type","integer")
                    .endObject

                    .startObject("source")
                      .field("type","string")
                    .endObject
                    
                .endObject()
              
                /* vendor */
                .startObject("vendor")
                   .field("type", "string")
                .endObject()
                
              .endObject()
              
            .endObject()
          .endObject()
    
    builder
  
  }

}