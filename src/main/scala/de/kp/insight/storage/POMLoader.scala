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

class POMLoader(ctx:RequestContext,params:Map[String,String]) extends BaseLoader(ctx,params) {

  override def load(params:Map[String,String]) {

    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    val parquetFile = extract(store)
        
    val sources = transform(params,parquetFile)

    if (ctx.putSources("orders","metrics",sources) == false)
      throw new Exception("Loading process has been stopped due to an internal error.")
    
  }

  private def extract(store:String):RDD[ParquetPOM] = {
    
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

      val total = data("total").asInstanceOf[Long]
      val total_amount = data("total_amount").asInstanceOf[Double]
      
      val total_avg_amount = data("total_avg_amount").asInstanceOf[Double]
      val total_max_amount = data("total_max_amount").asInstanceOf[Double]

      val total_min_amount = data("total_min_amount").asInstanceOf[Double]
      val total_stdev_amount = data("total_stdev_amount").asInstanceOf[Double]

      val total_variance_amount = data("total_variance_amount").asInstanceOf[Double]
      val total_avg_timespan = data("total_avg_timespan").asInstanceOf[Double]

      val total_max_timespan = data("total_max_timespan").asInstanceOf[Double]
      val total_min_timespan = data("total_min_timespan").asInstanceOf[Double]

      val total_stdev_timespan = data("total_stdev_timespan").asInstanceOf[Double]
      val total_variance_timespan = data("total_variance_timespan").asInstanceOf[Double]
 
      val total_day_supp = data("total_day_supp").asInstanceOf[Seq[(Int,Int)]]
      val total_time_supp = data("total_time_supp").asInstanceOf[Seq[(Int,Int)]]
      
      val total_item_supp = data("total_item_supp").asInstanceOf[Seq[(Int,Int)]]
      val total_items = data("total_items").asInstanceOf[Long]

      val total_customers = data("total_customers").asInstanceOf[Long]

      ParquetPOM(  
        total,
        total_amount,
        total_avg_amount,
        total_max_amount,
        total_min_amount,
        total_stdev_amount,
        total_variance_amount,
 
        total_avg_timespan,
        total_max_timespan,
        total_min_timespan,
        total_stdev_timespan,
        total_variance_timespan,

        total_day_supp,
        total_time_supp,

        total_item_supp,
        total_items,
        
        total_customers
        
      )
      
    })
    
  }
  
  private def transform(params:Map[String,String],dataset:RDD[ParquetPOM]):List[XContentBuilder] = {
            
    dataset.map(x => {
          
      val builder = XContentFactory.jsonBuilder()
	  builder.startObject()
      
      /********** METADATA **********/
      
      /* uid */
      builder.field(Names.UID_FIELD,params(Names.REQ_UID))
      
      /* timestamp */
      builder.field(Names.TIMESTAMP_FIELD,params("timestamp").toLong)
      
      /* site */
      builder.field(Names.SITE_FIELD,params("site"))
      
      /********** METRIC DATA **********/
	    	    
      /* total_orders */
	  builder.field("total_orders",x.total_orders)

	  /* total_amount */
	  builder.field("total_amount",x.total_amount)
	
	  /* total_avg_amount */
	  builder.field("total_avg_amount",x.total_avg_amount)

	  /* total_max_amount */
	  builder.field("total_max_amount",x.total_max_amount)

	  /* total_min_amount */
	  builder.field("total_min_amount",x.total_min_amount)
	
	  /* total_stdev_amount */
	  builder.field("total_stdev_amount",x.total_stdev_amount)

	  /* total_variance_amount */
	  builder.field("total_variance_amount",x.total_variance_amount)

	  /* total_avg_timespan */
	  builder.field("total_avg_timespan",x.total_avg_timespan)

	  /* total_max_timespan */
	  builder.field("total_max_timespan",x.total_max_timespan)

	  /* total_min_timespan */
	  builder.field("total_min_timespan",x.total_min_timespan)
	
	  /* total_stdev_timespan */
	  builder.field("total_stdev_timespan",x.total_stdev_timespan)

	  /* total_variance_timespan */
	  builder.field("total_variance_timespan",x.total_variance_timespan)

	  /* total_day_supp */
	  builder.startArray("total_day_supp")
	  for (rec <- x.total_day_supp) {
        
	    builder.startObject()

	    builder.field("day", rec._1)
        builder.field("supp",rec._2)
              
        builder.endObject()
    
	  }

      builder.endArray()

      /* total_time_supp */
	  builder.startArray("total_time_supp")
	  for (rec <- x.total_time_supp) {
      
	    builder.startObject()
              
        builder.field("time",rec._1)
        builder.field("supp",rec._2)
              
        builder.endObject()
    
	  }

      builder.endArray()   

	  /* total_item_supp */
	  builder.startArray("total_item_supp")
	  for (rec <- x.total_item_supp) {

	    builder.startObject()
          
	    builder.field("item",rec._1)
        builder.field("supp",rec._2)
              
        builder.endObject()
      
	  }

      builder.endArray()
	  
      /* total_items */
	  builder.field("total_items",x.total_items)
	  
      /* total_customers */
	  builder.field("total_customers",x.total_customers)
    
      builder.endObject()
      builder
      
    }).collect.toList
    
  }

}