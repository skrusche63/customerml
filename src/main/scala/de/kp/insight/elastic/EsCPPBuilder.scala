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

import de.kp.spark.core.Names

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

class EsCPPBuilder {

  import de.kp.spark.core.Names._
  
  def createBuilder(mapping:String):XContentBuilder = {

    val builder = XContentFactory.jsonBuilder()
          .startObject()
            .startObject(mapping)
              .startObject("properties")
                
                /********** METADATA **********/

                /* uid */
                .startObject(UID_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
                
                /* timestamp */
                .startObject(TIMESTAMP_FIELD)
                  .field("type", "long")
                  .field("index", "not_analyzed")
                .endObject()
                    
                /* site */
                .startObject(SITE_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()
               
                /********** PROFILE DATA **********/

                /* user */
                .startObject(USER_FIELD)
                  .field("type", "string")
                  .field("index", "not_analyzed")
                .endObject()

                /* recency */
                .startObject("recency")
                   .field("type", "integer")
                .endObject()

                /* frequency */
                .startObject("frequency")
                   .field("type", "integer")
                .endObject()

                /* amount */
                .startObject("amount")
                  .field("type", "double")
                .endObject()

                /* r_segment */
                .startObject("r_segment")
                  .field("type", "integer")
                .endObject()

                /* f_segment */
                .startObject("f_segment")
                  .field("type", "integer")
                .endObject()

                /* m_segment */
                .startObject("m_segment")
                  .field("type", "integer")
                .endObject()

                /* l_segment */
                .startObject("l_segment")
                  .field("type", "integer")
                .endObject()

                /* customer_type */
                .startObject("customer_type")
                  .field("type", "integer")
                .endObject()
               
                /* d_type */
                .startObject("d_type")
                  .field("type", "integer")
                .endObject()

                /* d_distance */
                .startObject("d_distance")
                  .field("type", "double")
                .endObject()
               
                /* h_type */
                .startObject("h_type")
                  .field("type", "integer")
                .endObject()

                /* h_distance */
                .startObject("h_distance")
                  .field("type", "double")
                .endObject()
               
                /* r_type */
                .startObject("r_type")
                  .field("type", "integer")
                .endObject()

                /* r_distance */
                .startObject("r_distance")
                  .field("type", "double")
                .endObject()
               
                /* p_type */
                .startObject("p_type")
                  .field("type", "integer")
                .endObject()

                /* p_distance */
                .startObject("p_distance")
                  .field("type", "double")
                .endObject()

              .endObject() // properties
            .endObject()   // mapping
          .endObject()
                    
    builder

  }

}