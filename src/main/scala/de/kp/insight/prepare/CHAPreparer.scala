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

import de.kp.spark.core.Names
import de.kp.insight.model._

import de.kp.insight.preference.TFIDF
import de.kp.insight.RequestContext

class CHAPreparer(ctx:RequestContext,params:Map[String,String],orders:RDD[InsightOrder]) extends BasePreparer(ctx,params) {
  
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)

    val customer = params("customer").toInt
    val filter = new PreparerFilter(ctx,orders)

    val filteredDS = filter.filterCHA(customer,uid)
    /*
     * Compute the customer day affinity (CDA) using the 
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