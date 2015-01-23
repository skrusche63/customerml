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

import org.elasticsearch.client.Client

import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.index.query.QueryBuilder

import com.fasterxml.jackson.databind.{Module, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

class PredictiveQuestor(client:Client) {

  val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  def count(index:String,mapping:String,query:QueryBuilder):Int = {
    
    val response = client.prepareCount(index).setTypes(mapping).setQuery(query)
                     .execute().actionGet()
    /*
     * We restrict the count to an integer, as we use the result 
     * as the predefined size of a search request
     */                 
    response.getCount().toInt

  }
  
  def find(index:String,mapping:String,query:QueryBuilder):SearchResponse = {
    
    /*
     * Prepare search request: note, that we may have to introduce
     * a size restriction with .setSize method 
     */
    val response = client.prepareSearch(index).setTypes(mapping).setQuery(query)
                     .execute().actionGet()

    response
    
  }

  def find(index:String,mapping:String,query:QueryBuilder,size:Int):SearchResponse = {
    
    /*
     * Prepare search request: note, that we may have to introduce
     * a size restriction with .setSize method 
     */
    val response = client.prepareSearch(index).setTypes(mapping).setQuery(query).setSize(size)
                     .execute().actionGet()

    response
    
  }

  def getAsString(index:String,mapping:String,id:String):String = {
    
    val response = client.prepareGet(index,mapping,id).execute().actionGet()
    if (response.isExists()) response.getSourceAsString else null
    
  }

}