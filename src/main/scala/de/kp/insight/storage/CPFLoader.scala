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

/**
 * CPFLoader class loads the results of the CPFEnricher
 * into the customers/forecasts index.
 */
class CPFLoader(ctx:RequestContext,params:Map[String,String]) extends BaseLoader(ctx,params) {

  override def load(params:Map[String,String]) {

    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    val parquetFile = extract(store)
        
    val sources = transform(params,parquetFile)

    if (ctx.putSources("customers","forecasts",sources) == false)
      throw new Exception("Loading process has been stopped due to an internal error.")
    
  }

  private def extract(store:String):RDD[ParquetCPF] = {
   
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

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]

      val step = data("step").asInstanceOf[Int]

      val amount = data("amount").asInstanceOf[Double]
      val time = data("time").asInstanceOf[Long]

      val rm_state = data("rm_state").asInstanceOf[String]
      val score = data("score").asInstanceOf[Double]
      
      ParquetCPF(site,user,step,amount,time,rm_state,score)    

    })

  }

  private def transform(params:Map[String,String],forecasts:RDD[ParquetCPF]):List[XContentBuilder] = {

    val uid = params(Names.REQ_UID)
   
    forecasts.map(x => {
           
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
       
      /********** METADATA **********/
        
      /* uid */
      builder.field("uid",params("uid"))
       
      /* timestamp */
      builder.field("timestamp",params("timestamp").toLong)
      
      /* site */
      builder.field("site",x.site)
      
      /********** FORECAST DATA **********/
        
      /* user */
      builder.field("user",x.user)
        
      /* step */
      builder.field("step",x.step)
        
      /* amount */
      builder.field("amount",x.amount)
        
      /* time */
      builder.field("time",x.time)
        
      /* state */
      builder.field("state",x.rm_state)
        
      /* score */
      builder.field("score",x.score)
      
      /* customer_type */
      builder.field("customer_type",params("customer").toInt)
        
      builder.endObject()
      builder
    
    }).collect.toList
    
  }
  
}