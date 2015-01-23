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

import org.elasticsearch.index.query._

import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._

class RecommendQuestor(client:Client) extends PredictiveQuestor(client) {

  private val CSM_INDEX = "customers"
  private val CPR_MAPPING = "recommendations"

  private val PRD_INDEX = "products"
  
  private val PPF_MAPPING = "segments"
  private val RRM_MAPPING = "relations"
 
  private val ORD_INDEX = "orders"
  private val POM_MAPPING = "metrics"
 
  def recommended_products(site:String,user:String,created_at:Long):String = {
    /*
     * STEP #1: Build filtered query
     */
    val filters = Buffer.empty[FilterBuilder]

    filters += FilterBuilders.termFilter("site", site)
    filters += FilterBuilders.termFilter("user", user)
    
    filters += FilterBuilders.rangeFilter("timestamp").gt(created_at)  
    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    /*
     * STEP #2: Determine all
     */
    val total = count(CSM_INDEX,CPR_MAPPING,qbuilder)    
    val response = find(CSM_INDEX, CPR_MAPPING, qbuilder,total)
    
    val hits = response.getHits()
    val total_hits = hits.totalHits()
    
    if (total_hits == 0) {
      """{}"""
      
    } else {    
      /*
       * The dataset, retrieved from the respective index, represents all products 
       * that have been recommended to the specific customer
       */
      val dataset = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[EsCPR]))
      val latest = dataset.map(x => x.timestamp).toSeq.sorted.last
      
      val filtered = dataset.filter(x => x.timestamp == latest)
      /*
       * Determine uid & customer_type from head of dataset
       */
      val (uid,customer_type) = (filtered.head.uid,filtered.head.customer_type)
      val items = filtered.map(x => EsPRF(x.item,x.score)).toList
      
      val result = EsBPR(uid,latest,site,user,items,customer_type)
      JSON_MAPPER.writerWithType(classOf[EsBPR]).writeValueAsString(result)
      
    }
     
  }

  def related_products(site:String,customer:Int,products:List[Int],created_at:Long):String = {
    /*
     * STEP #1: Build filtered query
     */
    val filters = Buffer.empty[FilterBuilder]

    filters += FilterBuilders.termFilter("site", site)
    filters += FilterBuilders.termFilter("customer_type", customer)

    filters += FilterBuilders.rangeFilter("timestamp").gt(created_at)  

    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    /*
     * STEP #2: Determine all product relation rules that match
     * the provided parameters
     */
    val total = count(PRD_INDEX,RRM_MAPPING,qbuilder)    
    val response = find(PRD_INDEX, RRM_MAPPING, qbuilder,total)
    
    val hits = response.getHits()
    val total_hits = hits.totalHits()
    
    if (total_hits == 0) {
      """{}"""
      
    } else {    
      /*
       * STEP #3: Score the consequent part of the respective rule by
       * 
       * a) the overlap ratio of the antecedent part with the provided 
       *    products, 
       *    
       * b) the confidence and 
       * 
       * c) the support ratio
       * 
       */
      val dataset = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[EsPRM]))
      val latest = dataset.map(x => x.timestamp).toSeq.sorted.last
      
      val filtered = dataset.filter(x => x.timestamp == latest)      
      val uid = filtered.head.uid

      val items = filtered.flatMap(rule => {
        
        val ratio = rule.support.toDouble / rule.total
        
        val antecedent = rule.antecedent        
        val weight = products.intersect(antecedent).size.toDouble / antecedent.size
        
        val score = weight * rule.confidence * ratio
        rule.consequent.map(x => EsPRF(x,score))
      
      })

      val result = EsBPR(uid,latest,site,"",items,customer)
      JSON_MAPPER.writerWithType(classOf[EsBPR]).writeValueAsString(result)
      
    }
    
  }
  def similar_products(site:String,customer:Int,product:Int,created_at:Long):String = {
    /*
     * STEP #1: Build filtered query
     */
    val filters = Buffer.empty[FilterBuilder]

    filters += FilterBuilders.termFilter("site", site)
    filters += FilterBuilders.termFilter("customer_type", customer)

    filters += FilterBuilders.termFilter("item", product)    
    filters += FilterBuilders.rangeFilter("timestamp").gt(created_at)  
    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    /*
     * STEP #2: Determine all
     */
    val total = count(CSM_INDEX,CPR_MAPPING,qbuilder)    
    val response = find(CSM_INDEX, CPR_MAPPING, qbuilder,total)
    
    val hits = response.getHits()
    val total_hits = hits.totalHits()
    
    if (total_hits == 0) {
      """{}"""
      
    } else {    
      /*
       * The dataset, retrieved from the respective index, represents all products 
       * that are similar to a certain product
       */
      val dataset = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[EsPPS]))
      val latest = dataset.map(x => x.timestamp).toSeq.sorted.last
      
      val filtered = dataset.filter(x => x.timestamp == latest)
      /*
       * Determine uid & customer_type from head of dataset
       */
      val (uid,customer_type) = (filtered.head.uid,filtered.head.customer_type)
      val items = filtered.map(x => EsPRF(x.other,x.score)).toList
      
      val result = EsBPR(uid,latest,site,"",items,customer_type)
      JSON_MAPPER.writerWithType(classOf[EsBPR]).writeValueAsString(result)
      
    }
     
  }
  
  def top_products(site:String,created_at:Long):String = {
    /*
     * STEP #1: Build filtered query
     */
    val filters = Buffer.empty[FilterBuilder]

    filters += FilterBuilders.termFilter("site", site)
    filters += FilterBuilders.rangeFilter("timestamp").gt(created_at)  

    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    /*
     * STEP #2: Determine all purchase metrics dataset that match
     * the provided parameters
     */
    val total = count(ORD_INDEX,POM_MAPPING,qbuilder)    
    val response = find(ORD_INDEX, POM_MAPPING, qbuilder,total)
    
    val hits = response.getHits()
    val total_hits = hits.totalHits()
    
    if (total_hits == 0) {
      """{}"""
      
    } else {    
      /*
       * The dataset, retrived from the respective index, represents all products
       * that have been purchased in a certain time window
       */    
      val dataset = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[EsPOM]))
      val latest = dataset.map(x => x.timestamp).toSeq.sorted.last
      
      val filtered = dataset.filter(x => x.timestamp == latest)(0)     
      val uid = filtered.uid
      
      val item_supp = filtered.total_item_supp
      val total = item_supp.size
      
      val items = item_supp.map(x => EsPRF(x.item, x.supp.toDouble / total))
      val result = EsBPR(uid,latest,site,"",items,0)
    
      JSON_MAPPER.writerWithType(classOf[EsBPR]).writeValueAsString(result)
    
    }
 
  }

  def top_products(site:String,customer:Int,created_at:Long):String = {
    /*
     * STEP #1: Build filtered query
     */
    val filters = Buffer.empty[FilterBuilder]

    filters += FilterBuilders.termFilter("site", site)
    filters += FilterBuilders.termFilter("customer_type", customer)

    filters += FilterBuilders.rangeFilter("timestamp").gt(created_at)  

    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)

    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    /*
     * STEP #2: Determine all purchase product frequency dataset that 
     * match the provided parameters
     */
    val total = count(PRD_INDEX,PPF_MAPPING,qbuilder)    
    val response = find(PRD_INDEX,PPF_MAPPING, qbuilder,total)
    
    val hits = response.getHits()
    val total_hits = hits.totalHits()
    
    if (total_hits == 0) {
      """{}"""
      
    } else {    
      /*
       * The dataset, retrived from the respective index, represents all products
       * that have been purchased by all customers of a certain customer type
       */
      val dataset = hits.hits().map(x => JSON_MAPPER.readValue(x.getSourceAsString,classOf[EsPPF]))
      val latest = dataset.map(x => x.timestamp).toSeq.sorted.last
     
      val filtered = dataset.filter(x => x.timestamp == latest)
      val uid = filtered.head.uid
      
      val items = filtered.map(x => {
        
        val item = x.item        
        val score = (x.c_segment * x.p_segment).toDouble / 25
        
        EsPRF(item,score)
        
      })
      
      val result = EsBPR(uid,latest,site,"",items,customer)
      JSON_MAPPER.writerWithType(classOf[EsBPR]).writeValueAsString(result)
    
    }
  }
  
}