package de.kp.insight.storage
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
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.parquet._

import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

abstract class PSALoader(ctx:RequestContext,params:Map[String,String]) extends BaseLoader(ctx,params) {

  protected def extract(store:String):RDD[ParquetPSA] = {
   
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(row => {

      val values = row.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val cluster = data("cluster").asInstanceOf[Int]
      val item = data("item").asInstanceOf[Int]

      val label = data("label").asInstanceOf[String]
      val value = data("value").asInstanceOf[Double]
      
      ParquetPSA(cluster,item,label,value)    

    })

  }
  
  protected def transform(params:Map[String,String],personas:RDD[ParquetPSA]):List[XContentBuilder] = {
  
    personas.map(x => {
           
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
       
      /********** METADATA **********/
        
      /* uid */
      builder.field("uid",params("uid"))
       
      /* timestamp */
      builder.field("timestamp",params("timestamp").toLong)
      
      /* site */
      builder.field("site",params("site"))
      
      /********** PERSONA DATA **********/
        
      /* cluster */
      builder.field("cluster",x.cluster)
        
      /* item */
      builder.field("item",x.item)
        
      /* label */
      builder.field("label",x.label)
        
      /* value */
      builder.field("value",x.value)
      
      /* customer_type */
      builder.field("customer_type",params("customer").toInt)
        
      builder.endObject()
      builder
    
    }).collect.toList
    
  }

}