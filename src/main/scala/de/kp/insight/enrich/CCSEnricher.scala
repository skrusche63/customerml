package de.kp.insight.enrich
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

import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.parquet._

import de.kp.insight.util.MFUtil

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * The CCSEnricher is based on the results of the CPRLearner and precomputes
 * user user similarities for every single user; the similarity results are 
 * stored as Parquet file
 */
class CCSEnricher(ctx:RequestContext,params:Map[String,String]) extends BaseEnricher(ctx,params) {
        
  import sqlc.createSchemaRDD
  override def enrich {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
      
    val site = params(Names.REQ_SITE)
       
    /*
     * STEP #1: Retrieve matrix factorization and associated customer and product
     * lookup dictionaries from the file system; note, that these data have been
     * learned by the CPRLearner 
     */
    val store = String.format("""%s/%s/%s/2""",ctx.getBase,name,uid)         
    val (udict,idict,model) = new MFUtil(ctx.sparkContext).read(store)
        
    val total = params.get(Names.REQ_TOTAL) match {
      case None => 10
      case Some(value) => value.toInt
    }
    /*
     * STEP #2: Predict 'total' number of users that are most similar to a 
     * certain user and save these recomputed products as Parquet file; note, 
     * that the similarity matrix used here has ZERO diagonal values, i.e. we 
     * do not take the item itself into account
     */
    val similarities = model.computeUserSimilarities
    val ulookup = udict.map{case(user,ux) => (ux,user)}.toMap

    val tableCCS = ctx.sparkContext.parallelize(udict.flatMap{case (user,ux) => {
      
      val column = similarities.getRow(ux).elementsAsList()
      val highest = column.sortBy(x => -x).take(total)

      highest.map(x => {
      
        val other = ulookup(column.indexOf(x))
        val score = x
        
        ParquetCCS(site,user,other,score)
        
      }) 
          
    }}.toSeq)

    val storeCCS = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    tableCCS.saveAsParquetFile(storeCCS)

  }
  
}