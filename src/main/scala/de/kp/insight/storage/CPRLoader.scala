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
 * CPRLoader class directly loads the results of the CPREnricher
 * into the customers/recommendations index.
 */
class CPRLoader(ctx:RequestContext,params:Map[String,String]) extends BaseLoader(ctx,params) {

  override def load(params:Map[String,String]) {

    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/3""",ctx.getBase,name,uid)         
    val parquetFile = extract(store)
        
    val sources = transform(params,parquetFile)

    if (ctx.putSources("customers","recommendations",sources) == false)
      throw new Exception("Loading process has been stopped due to an internal error.")
    
  }

  private def extract(store:String):RDD[ParquetCPR] = {
    
    /* 
     * Read in the parquet file created above. Parquet files are self-describing 
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
      
      val item = data("item").asInstanceOf[Int]
      val score = data("score").asInstanceOf[Double]

      ParquetCPR(site,user,item,score)
      
    })
    
  }
  
  private def transform(params:Map[String,String],dataset:RDD[ParquetCPR]):List[XContentBuilder] = {
            
    dataset.map(x => {
      
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
      
      /********** METADATA **********/
      
      /* uid */
      builder.field(Names.UID_FIELD,params(Names.REQ_UID))
      
      /* timestamp */
      builder.field(Names.TIMESTAMP_FIELD,params("timestamp").toLong)
	  
	  /* site */
      builder.field(Names.SITE_FIELD,x.site)
      
      /********** USER DATA **********/
	  
	  /* user */
      builder.field(Names.USER_FIELD,x.user)

	  /* item */
	  builder.field("item",x.item)

	  /* score */
	  builder.field("score",x.score)
      
      /* customer_type */
      builder.field("customer_type",params("customer").toInt)
	  
	  builder.endObject()
	  
	  builder.endObject()
      builder
      
    }).collect.toList
  }
 
}