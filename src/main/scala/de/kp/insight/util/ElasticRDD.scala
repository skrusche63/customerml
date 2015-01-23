package de.kp.insight.util
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
import org.apache.spark.rdd.RDD

import org.apache.hadoop.io.{ArrayWritable,DoubleWritable,IntWritable,LongWritable,MapWritable,NullWritable,Text}

import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.elasticsearch.hadoop.mr.EsInputFormat

import scala.collection.JavaConversions._
import de.kp.insight.model._

class ElasticRDD(@transient sc:SparkContext) {
  /**
   * This method is based on EsInputFormat from the elastichsearch-hadoop project; 
   * note, that this format retrieves all entries of a certain index & mapping with
   * a default match_all query. 
   * 
   * Requests to Elasticsearch are internally based on the scroll mechanism.
   */
  def read(config:HadoopConfig):RDD[MapWritable] = {

    val source = sc.newAPIHadoopRDD(config, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])
    source.map(_._2)
    
  }
  
  /**
   * Convert MapWritable representation of a certain customer
   * segment into a customer rfm representation
   */
  def rfm(rawset:RDD[MapWritable]):RDD[(String,String,Double,Int,Int,Long)] = {
    
    rawset.map(x => {

      val entries = x.entrySet().map(kv => {
        
        val k = kv.getKey().asInstanceOf[Text].toString
        val v = kv.getValue() match {
          
          case valu:IntWritable => valu.get()
          case valu:DoubleWritable => valu.get()

          case valu:LongWritable => valu.get()          
          case valu:Text => valu.toString
          
          case _ => throw new Exception("Data type is not supported for orders.")
          
        }
      
        (k,v)
        
      }).toMap
   
      val site = entries("site").asInstanceOf[String]
      val user = entries("user").asInstanceOf[String]
    
      val amount = entries("amount").asInstanceOf[Double]

      val recency = entries("recency").asInstanceOf[Int]
      val frequency = entries("frequency").asInstanceOf[Int]
      
      val timestamp = entries("timestamp").asInstanceOf[Long]
      
      (site,user,amount,recency,frequency,timestamp)
      
    })
    
  }

  /**
   * Convert MapWritable representation of a certain order into
   * the corresponding case class InsightOrder
   */
  def orders(rawset:RDD[MapWritable]):RDD[InsightOrder] = {
    
    rawset.map(x => {

      val entries = x.entrySet().map(kv => {
        
        val k = kv.getKey().asInstanceOf[Text].toString
        val v = kv.getValue() match {
          
          case valu:ArrayWritable => {

            val array = valu.get
            array.map(record => {
              
              record.asInstanceOf[MapWritable].entrySet().map(entry => {
                
                val sub_k = entry.getKey().asInstanceOf[Text].toString()
                val sub_v = entry.getValue() match {
          
                  case sub_valu:IntWritable => valu.get()
                  case sub_valu:DoubleWritable => valu.get()
          
                  case sub_valu:LongWritable => valu.get()
                  case sub_valu:Text => valu.toString
                  
                }
                
                (sub_k,sub_v)
                
              }).toMap
              
            }).toList
            
          }
          
          case valu:IntWritable => valu.get()
          case valu:DoubleWritable => valu.get()
          
          case valu:LongWritable => valu.get()
          case valu:Text => valu.toString

          case _ => throw new Exception("Data type is not supported for orders.")
          
        }
      
        (k,v)
        
      }).toMap
    
      val uid = entries("uid").asInstanceOf[String]
      val last_sync = entries("last_sync").asInstanceOf[Long]

      val created_at_min = entries("created_at_min").asInstanceOf[Long]
      val created_at_max = entries("created_at_max").asInstanceOf[Long]
    
      val site = entries("site").asInstanceOf[String]
      val user = entries("user").asInstanceOf[String]
    
      val amount = entries("amount").asInstanceOf[Double]
      val discount = entries("discount").asInstanceOf[Double]

      val shipping = entries("shipping").asInstanceOf[Double]
      val timestamp = entries("timestamp").asInstanceOf[Long]
    
      val group = entries("group").asInstanceOf[String]
      val ip_address = entries("ip_address").asInstanceOf[String]
    
      val user_agent = entries("user_agent").asInstanceOf[String]
      val items = entries("items").asInstanceOf[List[Map[String,Any]]].map(x => {
        
        val item = x("item").asInstanceOf[Int]
        val quantity = x("quantity").asInstanceOf[Int]        

        val category = x("category").asInstanceOf[String]
        val vendor = x("vendor").asInstanceOf[String]
        
        InsightOrderItem(item,quantity,category,vendor)
        
      })
      
      InsightOrder(
          uid,
          last_sync,
          created_at_min,
          created_at_max,
          site,
          user,
          amount,
          discount,
          shipping,
          timestamp,
          group,
          ip_address,
          user_agent,
          items
      )
    
    })
    
  }
 
}