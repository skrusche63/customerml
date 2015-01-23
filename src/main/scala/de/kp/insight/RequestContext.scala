package de.kp.insight
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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.elastic._
import de.kp.insight.model._

import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.xcontent.XContentBuilder

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.elasticsearch.index.query.QueryBuilder

class RequestContext(
  /*
   * Reference to the common SparkContext; this context can be used
   * to access HDFS based data sources or leverage the Spark machine
   * learning library or other Spark based functionality
   */
  @transient val sparkContext:SparkContext) extends Serializable {

  val JSON_MAPPER = new ObjectMapper()  
  JSON_MAPPER.registerModule(DefaultScalaModule)

  val sqlCtx = new SQLContext(sparkContext)
  
  private val elasticClient = new ElasticClient()
  /*
   * The RemoteContext enables access to remote Akka systems and their actors;
   * this variable is used to access the engines of Predictiveworks
   */
  private val remoteContext = new RemoteContext()
  /*
   * Heartbeat & timeout configuration in seconds
   */
  private val (heartbeat, time) = Configuration.heartbeat  
  /**
   * The base directory for all file based IO
   */
  def getBase = Configuration.input(0)
  
  def getConfig = Configuration
  
  def getESConfig = Configuration.elastic
  
  /**
   * The time interval for schedulers (e.g. StatusSupervisor) to 
   * determine how often alive messages have to be sent
   */
  def getHeartbeat = heartbeat
  
  def getRemoteContext = remoteContext
  
  /**
   * The timeout interval used to supervise actor interaction
   */
  def getTimeout = time
  /**
   * The subsequent methods wrap the respective methods from the Elasticsearch
   * client and makes access to the multiple search indexes available from the
   * request context
   */  
  def createIndex(index:String,mapping:String,topic:String):Boolean 
    = elasticClient.createIndex(index,mapping,topic)

  def count(index:String,mapping:String,query:QueryBuilder):Int 
    = elasticClient.count(index,mapping,query)

  def find(index:String,mapping:String,query:QueryBuilder):SearchResponse 
    = elasticClient.find(index,mapping,query)

  def find(index:String,mapping:String,query:QueryBuilder,size:Int):SearchResponse 
    = elasticClient.find(index,mapping,query,size)

  def getAsMap(index:String,mapping:String,id:String):java.util.Map[String,Object] 
    = elasticClient.getAsMap(index,mapping,id)

  def getAsString(index:String,mapping:String,id:String):String 
    = elasticClient.getAsString(index,mapping,id)

  def putLog(level:String,message:String) = elasticClient.putLog(level,message)
  
  def putSource(index:String,mapping:String,source:XContentBuilder):Boolean 
    = elasticClient.putSource(index,mapping,source)
  
  def putSources(index:String,mapping:String,sources:List[XContentBuilder]):Boolean 
    = elasticClient.putSources(index,mapping,sources)

    def putSources(index:String,mapping:String,ids:List[String],sources:List[XContentBuilder]):Boolean 
    = elasticClient.putSources(index,mapping,ids,sources)
  
}