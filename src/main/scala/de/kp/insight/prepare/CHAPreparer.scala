package de.kp.insight.prepare
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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.joda.time.DateTime

import de.kp.spark.core.Names

import de.kp.insight.model._

import de.kp.insight.preference.TFIDF
import de.kp.insight.RequestContext

class CHAPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
  
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)

    val customer = params("customer").toInt
    /*
     * STEP #1: Restrict the purchase orders to those items and attributes
     * that are relevant for the item segmentation task; this encloses a
     * filtering with respect to customer type, if different from '0'
     */
    val ctype = sc.broadcast(customer)

    val ds = orders.map(x => (x.site,x.user,new DateTime(x.timestamp).hourOfDay().get))
    val filteredDS = (if (customer == 0) {
      /*
       * This customer type indicates that ALL customer types
       * have to be taken into account when computing the item
       * segmentation 
       */
      ds
          
    } else {
      /*
       * Load the Parquet file that specifies the customer type specification 
       * and filter those customers that match the provided customer type
       */
      val parquetCST = readCST(uid).filter(x => x._2 == ctype.value)      
      ds.map(x => ((x._1,x._2),(x._3))).join(parquetCST).map(x => {
            
        val ((site,user),((day),rfm_type)) = x
        (site,user,day)
            
      })
    })     
    /*
     * STEP #2: Compute the customer day affinity (CDA) using the 
     * TDIDF algorithm from text analysis
     */
    val table = TFIDF.computeCDA(filteredDS,daytime="hour")
    /* 
     * The RDD is implicitly converted to a SchemaRDD by createSchemaRDD, 
     * allowing it to be stored using Parquet. 
     */
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    table.saveAsParquetFile(store)
  
  }

}