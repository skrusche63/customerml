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

class ESTaskBuilder {
  
  def createBuilder(mapping:String):XContentBuilder = {
  
    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              
              .startObject("_id")
                .field("path","key")
              .endObject()
              
              .startObject("properties")
              
                .startObject("key")
                   .field("type","string")
                   .field("index","not_analyzed")
                .endObject()

                .startObject("task")
                   .field("type","string")
                .endObject()

                .startObject("timestamp")
                   .field("type","long")
                   .field("index","not_analyzed")
                .endObject()
                
              .endObject()
              
            .endObject
          .endObject
    
    builder
  }

}