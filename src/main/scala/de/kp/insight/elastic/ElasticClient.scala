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

//import de.kp.spark.core.Names

import org.elasticsearch.node.NodeBuilder._

import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.BulkResponse

import org.elasticsearch.action.search.SearchResponse

import org.elasticsearch.action.index.IndexRequest.OpType
import org.elasticsearch.action.index.IndexResponse

import org.elasticsearch.common.logging.Loggers
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import org.elasticsearch.index.query.QueryBuilder

import org.joda.time.DateTime
import java.util.concurrent.locks.ReentrantLock

class ElasticClient extends Serializable {

  /*
   * Create an Elasticsearch node by interacting with
   * the Elasticsearch server on the local machine
   */
  private val node = nodeBuilder().node()
  private val client = node.client()
  
  private val logger = Loggers.getLogger(getClass())
  private val indexCreationLock = new ReentrantLock()  

  init()
  
  /*
   * This method prepares the 'admin' index with two different mappings, 
   * one to log messages, and, another one to register the different
   * data tasks by this platform 
   */
  private def init() {

    if (createIndex("admin","logs","log") == false)
      throw new Exception("Index creation for 'admin/logs' has been stopped due to an internal error.")
    
    if (createIndex("admin","tasks","task") == false)
      throw new Exception("Index creation for 'admin/tasks' has been stopped due to an internal error.")
    
  }
  
  private def create(index:String,mapping:String,builder:XContentBuilder) {
        
    try {
      
      indexCreationLock.lock()
      val indices = client.admin().indices
      /*
       * Check whether referenced index exists; if index does not
       * exist, create one
       */
      val existsRes = indices.prepareExists(index).execute().actionGet()            
      if (existsRes.isExists() == false) {
        
        val createRes = indices.prepareCreate(index).execute().actionGet()            
        if (createRes.isAcknowledged() == false) {
          new Exception("Failed to create " + index)
        }
            
      }

      /*
       * Check whether the referenced mapping exists; if mapping
       * does not exist, create one
       */
      val prepareRes = indices.prepareGetMappings(index).setTypes(mapping).execute().actionGet()
      if (prepareRes.mappings().isEmpty) {

        val mappingRes = indices.preparePutMapping(index).setType(mapping).setSource(builder).execute().actionGet()
            
        if (mappingRes.isAcknowledged() == false) {
          new Exception("Failed to create mapping for " + index + "/" + mapping)
        }            

      }

    } catch {
      case e:Exception => {
        logger.error(e.getMessage())

      }
       
    } finally {
     indexCreationLock.unlock()
    }
    
  }

  def close() {
    if (node != null) node.close()
  }
    
  def createIndex(index:String,mapping:String,topic:String):Boolean = {
    
    try {
      
      /**********************************************************************
       * 
       *                     SUB PROCESS 'COLLECT'
       *                     
       *********************************************************************/
      
      if (topic == "CSM") {

        val builder = new EsCSMBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "ORD") {

        val builder = new EsORDBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "PRD") {

        val builder = new EsPRDBuilder().createBuilder(mapping)
        create(index,mapping,builder)
     
      } 
      
      /**********************************************************************
       * 
       *                     SUB PROCESS 'LOAD'
       *                     
       *********************************************************************/

      else if (topic == "CCS") {

        val builder = new EsCCSBuilder().createBuilder(mapping)
        create(index,mapping,builder)
      
      } else if (topic == "CDA") {

        val builder = new EsPSABuilder().createBuilder(mapping)
        create(index,mapping,builder)

      } else if (topic == "CHA") {

        val builder = new EsPSABuilder().createBuilder(mapping)
        create(index,mapping,builder)

      } else if (topic == "CLS") {

        val builder = new EsCLSBuilder().createBuilder(mapping)
        create(index,mapping,builder)

      } else if (topic == "CPA") {

        val builder = new EsPSABuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "CPF") {

        val builder = new EsCPFBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "CPP") {

        val builder = new EsCPPBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "CPR") {

        val builder = new EsCPRBuilder().createBuilder(mapping)
        create(index,mapping,builder)

      } else if (topic == "CSA") {

        val builder = new EsPSABuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "DPS") {

        val builder = new EsDPSBuilder().createBuilder(mapping)
        create(index,mapping,builder)
     
      } else if (topic == "LOC") {

        val builder = new EsLOCBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "PCR") {
        /* Note, we use the CRP builder here */
        val builder = new EsCPRBuilder().createBuilder(mapping)
        create(index,mapping,builder)
      
      } else if (topic == "POM") {

        val builder = new EsPOMBuilder().createBuilder(mapping)
        create(index,mapping,builder)
      
      } else if (topic == "PPF") {

        val builder = new EsPPFBuilder().createBuilder(mapping)
        create(index,mapping,builder)
      
      } else if (topic == "PPS") {

        val builder = new EsPPSBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "PRM") {

        val builder = new EsPRMBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } else if (topic == "RFM") {

        val builder = new EsRFMBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } 
      
      /**********************************************************************
       * 
       *                     SUB PROCESS 'PREDICT'
       *                     
       *********************************************************************/
      
      else if (topic == "RFM_F") {

        val builder = new EsRFM_FBuilder().createBuilder(mapping)
        create(index,mapping,builder)
       
      } 
       
      /**********************************************************************
       * 
       *                     LOG MANAGEMENT
       *                     
       *********************************************************************/
      
      else if (topic == "log") {

        val builder = new ESLogBuilder().createBuilder(mapping)
        create(index,mapping,builder)
           
      }
     
      /**********************************************************************
       * 
       *                     TASK MANAGEMENT
       *                     
       *********************************************************************/
      
      else if (topic == "task") {

        val builder = new ESTaskBuilder().createBuilder(mapping)
        create(index,mapping,builder)
           
      }
      
      true
    
    } catch {
      case e:Exception => false

    }
    
  }
  
  private def open(index:String,mapping:String):Boolean = {
        
    val readyToWrite = try {
      
      val indices = client.admin().indices
      /*
       * Check whether referenced index exists; if index does not
       * exist, through exception
       */
      val existsRes = indices.prepareExists(index).execute().actionGet()            
      if (existsRes.isExists() == false) {
        new Exception("Index '" + index + "' does not exist.")            
      }

      /*
       * Check whether the referenced mapping exists; if mapping
       * does not exist, through exception
       */
      val prepareRes = indices.prepareGetMappings(index).setTypes(mapping).execute().actionGet()
      if (prepareRes.mappings().isEmpty) {
        new Exception("Mapping '" + index + "/" + mapping + "' does not exist.")
      }
      
      true

    } catch {
      case e:Exception => {
        
        logger.error(e.getMessage())
        false
        
      }
       
    } finally {
    }
    
    readyToWrite
    
  }
  
  private def writeJSON(index:String,mapping:String,source:XContentBuilder):Boolean = {
    
    /*
     * The OpType INDEX (other than CREATE) ensures that the document is
     * 'updated' which means an existing document is replaced and reindexed
     */
    client.prepareIndex(index, mapping).setSource(source).setRefresh(true).setOpType(OpType.INDEX)
      .execute(new ActionListener[IndexResponse]() {
        override def onResponse(response:IndexResponse) {
          /*
           * Registration of provided source successfully performed; no further
           * action, just logging this event
           */
          val msg = String.format("""Successful registration for: %s""", source.toString)
          logger.info(msg)
        
        }      

        override def onFailure(t:Throwable) {
	      /*
	       * In case of failure, we expect one or both of the following causes:
	       * the index and / or the respective mapping may not exists
	       */
          val msg = String.format("""Failed to register %s""", source.toString)
          logger.error(msg,t)
	      
          close()
          throw new Exception(msg)
	    
        }
        
      })
      
    true
  
  }
  
  private def writeBulkJSON(index:String,mapping:String,sources:List[XContentBuilder]):Boolean = {
    
    /*
     * Prepare bulk request and fill with sources
     */
    val bulkRequest = client.prepareBulk()
    for (source <- sources) {
      bulkRequest.add(client.prepareIndex(index, mapping).setSource(source).setRefresh(true).setOpType(OpType.INDEX))
    }
    
    bulkRequest.execute(new ActionListener[BulkResponse](){
      override def onResponse(response:BulkResponse) {

        if (response.hasFailures()) {
          
          val msg = String.format("""Failed to register data for %s/%s""",index,mapping)
          logger.error(msg, response.buildFailureMessage())
                
        } else {
          
          val msg = "Successful registration of bulk sources."
          logger.info(msg)
          
        }        
      
      }
       
      override def onFailure(t:Throwable) {
	    /*
	     * In case of failure, we expect one or both of the following causes:
	     * the index and / or the respective mapping may not exists
	     */
        val msg = "Failed to register bulk of sources."
        logger.info(msg,t)
	      
        close()
        throw new Exception(msg)
	    
      }
      
    })
    
    true
  
  }
  
  private def writeBulkJSON(index:String,mapping:String,ids:List[String],sources:List[XContentBuilder]):Boolean = {
    
    val zipped = ids.zip(sources)

    /*
     * Prepare bulk request and fill with sources
     */
    val bulkRequest = client.prepareBulk()
    for ((id,source) <- zipped) {
      bulkRequest.add(client.prepareIndex(index,mapping,id).setSource(source).setRefresh(true).setOpType(OpType.INDEX))
    }
    
    bulkRequest.execute(new ActionListener[BulkResponse](){
      override def onResponse(response:BulkResponse) {

        if (response.hasFailures()) {
          
          val msg = String.format("""Failed to register data for %s/%s""",index,mapping)
          logger.error(msg, response.buildFailureMessage())
                
        } else {
          
          val msg = "Successful registration of bulk sources."
          logger.info(msg)
          
        }        
      
      }
       
      override def onFailure(t:Throwable) {
	    /*
	     * In case of failure, we expect one or both of the following causes:
	     * the index and / or the respective mapping may not exists
	     */
        val msg = "Failed to register bulk of sources."
        logger.info(msg,t)
	      
        close()
        throw new Exception(msg)
	    
      }
      
    })
    
    true
  
  }
  
  def putLog(level:String,message:String) {
     
    try {
    
      val timestamp = new DateTime().getMillis
    
      val builder = XContentFactory.jsonBuilder()
	  builder.startObject()
	
	  /* timestamp */
	  builder.field("timestamp",timestamp)
	
	  /* level */
	  builder.field("level",level)
	
	  /* message */
	  builder.field("message",message)
	
	  builder.endObject()
	  
	  writeJSON("admin", "logs", builder) 
    
    } catch {
      case e:Exception => logger.error("Logging failed", e)

    }

  }
    
  def putSource(index:String,mapping:String,source:XContentBuilder):Boolean = {
     
    try {
        
      if (open(index,mapping) == false) {
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        writeJSON(index, mapping, source)      
        true
      
      }
    
    } catch {
      case e:Exception => false
    }
   
  }
    
  def putSources(index:String,mapping:String,sources:List[XContentBuilder]):Boolean = {
     
    try {
        
      if (open(index,mapping) == false) {
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        writeBulkJSON(index, mapping, sources)      
        true
      
      }
    
    } catch {
      case e:Exception => false
    }
   
  }
  
  def putSources(index:String,mapping:String,ids:List[String],sources:List[XContentBuilder]):Boolean = {
     
    try {
        
      if (open(index,mapping) == false) {
      
        val msg = String.format("""Opening index '%s' and mapping '%s' for write failed.""",index,mapping)
        throw new Exception(msg)
      
      } else {
      
        writeBulkJSON(index,mapping,ids,sources)      
        true
      
      }
    
    } catch {
      case e:Exception => false
    }
   
  }

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

  def getAsMap(index:String,mapping:String,id:String):java.util.Map[String,Object] = {
    
    val response = client.prepareGet(index,mapping,id).execute().actionGet()
    if (response.isExists()) response.getSource else null
    
  }

  def getAsString(index:String,mapping:String,id:String):String = {
    
    val response = client.prepareGet(index,mapping,id).execute().actionGet()
    if (response.isExists()) response.getSourceAsString else null
    
  }

}