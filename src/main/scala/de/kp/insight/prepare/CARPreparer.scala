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
import de.kp.insight.parquet._

import de.kp.insight.RequestContext
import de.kp.insight.geoip.{CountryUtil,LocationFinder}

import scala.collection.mutable.Buffer

case class BlockEntry(col:Long,cat:String,value:Double)

/**
 * CARPreparer has a focus on customers that refer to a certain 
 * customer type (from RFM Analysis); this approach ensures that
 * factorization models and similaity matrix are trained for a 
 * certain customer segment to avoid a mixing of e.g. bust buyers
 * with those that tend to fade off.
 * 
 * The results of the CPAPreparer are a prerequisite for the CARPreparer
 * as these results provide the target (rating) information for the
 * CARPreparer
 */
class CARPreparer(ctx:RequestContext,params:Map[String,String],orders:RDD[InsightOrder]) extends BasePreparer(ctx,params) {
  
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
      
    val customer = params("customer").toInt
    val filter = new PreparerFilter(ctx,orders)

    val filteredDS = filter.filterCAR(customer,uid)
    /*
     * Build the features associated with the Context-Aware recommendation 
     * approach from every customer's transaction data and join the result 
     * with the result of the CPAPreparer to add the respective rating
     */
    val ratings = filteredDS.groupBy(x => (x._1,x._2)).flatMap(p => {    

      val (site,user) = p._1
      val trans = p._2.map{case(site,user,ip_address,timestamp,items) => (ip_address,timestamp,items)}
      
      val geolookup = trans.map{case(ip_address,timestamp,items) => (ip_address,timestamp)}.map{case(ip_address,timestamp) => {
        
        val location = LocationFinder.locate(ip_address)
        
        val code = if (location.countrycode == "") "--" else location.countrycode
        val name = if (location.countryname == "") "N/A" else location.countryname
        
        (timestamp,CountryUtil.getIndex(code,name))
        
      }}.toMap
      
      /*
       * Compute time ordered list of purchased items and associated items 
       * (in the same transaction); for each item, we are interested in all
       * the items that have been purchased together 
       */
      val items = trans.flatMap{
        case(ip_address,timestamp,items) => {
          
          val iids = items.map(_.item)
          iids.map{case (iid) => (iid,timestamp,iids)}}
      
      }.groupBy{case(iid,timestamp,iids) => iid}
      /*
       * In order to gather contextual information for a certain (active) item,
       * we compare the two latest customer transactions, where the respective
       * item was bought
       */
      items.map(x => {
        
        /*
         * The active item is a categorical feature; this implies
         * that the respective CARLearner must set the value to 1
         */
        val active_item = x._1
        /*
         * Determine distint set of items that have been purchased
         * together with the active item; these data are used as a 
         * categorical set, and therefore the value must be set to 
         * 1 / N by the CARLearner
         */
        val other_items = x._2.flatMap(_._3).toSeq.distinct
        /*
         * Determine the recency of the last purchase of the active
         * item. The time elapsed from the last transaction since
         * now (today) is an indicator of the attractivity of the
         * active item. 
         * 
         * The recency is used as a numerical context variable
         */
        val timestamps = x._2.map(_._2).toSeq.sorted
       
        val latest = timestamps.last
        val today = new DateTime().getMillis()
        
        val recency = ((today - latest) / DAY).toDouble
        /*
         * From the timestamps, we are able to determine the 
         * geo location of the respective transaction; for 
         * the context, we determine the location with the
         * highest frequency with respect to customers country
         */       
        val location = timestamps.map(geolookup(_))
                         // Compute location count
                         .groupBy(v => v).map(v => (v._1,v._2.size))
                         // Determine location with highest cont
                         .toSeq.sortBy(_._2).last._1
        
        ((site,user,active_item),(today,other_items,recency,location))
        
      })
    
    }).join(readCPA(params)).map{
      case ((site,user,active_item),((today,other_items,recency,location),target)) => 
        (site,user,today,active_item,other_items,recency,location,target) 
    }
    
    /*
     * STEP #3. Store ratings as parquet file as these data are needed
     * for the post processing the Context-Aware Analysis results
     */
    val tableCAR = ratings.map{
           case (site,user,today,active_item,other_items,recency,location,target) => 
             ParquetCAR(site,user,today,active_item,other_items,recency,location,target) 
        }
    
    val store1 = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    tableCAR.saveAsParquetFile(store1)

       /*
     * STEP #4: Determine lookup data structure for users
     * and save as a parquet file
     */
    val users = ratings.groupBy(x => (x._1,x._2)).map(_._1)
    val uzipped = users.zipWithIndex
    
    val store2 = String.format("""%s/%s/%s/2""",ctx.getBase,name,uid)    
    
    val table2 = uzipped.map{case((site,user),col) => ParquetUCol(site,user,col)}
    table2.saveAsParquetFile(store2)
    
    val udict = ctx.sparkContext.broadcast(uzipped.collect.toMap)
    /*
     * STEP #5: Determine lookup data structure for items
     * and save as a parquet file
     */    
    val items = ratings.groupBy(x => x._4).map(_._1)
    val izipped = items.zipWithIndex
     
    val store3 = String.format("""%s/%s/%s/3""",ctx.getBase,name,uid)    
    
    val table3 = izipped.map{case(item,col) => ParquetICol(item,col)}
    table3.saveAsParquetFile(store3)
    
    val idict = ctx.sparkContext.broadcast(izipped.collect.toMap)
    
    /*
     * STEP #6: Build data structure that is compatible to the factorization
     * machine format (row,col,cat,val). The respective feature vector has the 
     * following architecture:
     * 
     * a) user block describing the active user
     * 
     * b) item block describing the active item
     * 
     * Every information in addition to the active user & item is considered
     * as context that has some influence onto the user-item rating
     * 
     * c) other items block (value = 1/N where N is the number of items rated so far)
     *         
     * Hypothesis: The items purchased so far by the active user do influence the rating
     * of the active item; this requires to have access onto historical purchase data
     * of the active user        
     *         
     * d) recency block of user-item-rating 
     * 
     * Hypothesis: The datetime of the respective user-item-rating influences the
     * rating of the active items
     *         
     * e) location block of user-item-rating (should be a single column)
     * 
     */
    val rows = ratings.groupBy(x => (x._1,x._2)).map(x => {
        
        val a_block_off = 0
        val b_block_off = a_block_off + udict.value.size
        
        val c_block_off = b_block_off + idict.value.size
        val d_block_off = c_block_off + 1
        
        val e_block_off = d_block_off + 1
        
        val (site,user) = x._1       
      
        val data = x._2.map{case(site,user,today,active_item,other_items,recency,location,target) => (active_item,other_items,recency,location,target)}
        data.flatMap{case(active_item,other_items,recency,location,target) => {
         /*
           * The subsequent process provides a single row of the feature matrix 
           * evaluated by the factorization machines
           */
          val features = Buffer.empty[BlockEntry]

          // a) USER BLOCK
          features += BlockEntry(a_block_off + udict.value((site,user)),"user",1)
          
          // b) ACTIVE ITEM BLOCK          
          features += BlockEntry(b_block_off + idict.value(active_item),"item",1)

          // c) OTHER ITEMS BLOCK
          val weight = other_items.size
          other_items.foreach(item => features += BlockEntry(c_block_off + idict.value(item),"context_categorical_set",weight))
       
          // d) RECENCY BLOCK          
          features += BlockEntry(d_block_off,"context_numerical",recency)
          
          // e) LOCATION BLOCK          
          features += BlockEntry(e_block_off,"context_numerical",location)
           
          // f) TARGET
          features += BlockEntry(e_block_off+1,"label",target)
       
          features
          
        }}.toSeq
        
      }).zipWithIndex
     
      val store4 = String.format("""%s/%s/%s/4""",ctx.getBase,name,uid)    
      
      val table4 = rows.flatMap{case(block,row) => block.map(e => ParquetDPT(row,e.col,e.cat,e.value))}
      table4.saveAsParquetFile(store4)

  }

  private def readCPA(params:Map[String,String]):RDD[((String,String,Int),Double)] = {
    
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME).replace("CAR","CPA")
    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)
    
    val parquetFile = sqlc.parquetFile(store)
    val schema = ctx.sparkContext.broadcast(parquetFile.schema)
    
    parquetFile.map(row => {

      val data = schema.value.fields.zip(row.toSeq).map{case(field,value) => (field.name,value)}.toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]

      val item = data("item").asInstanceOf[Int]
      val value = data("value").asInstanceOf[Double]
    
      ((site,user,item),value)
      
    })
  
  }

}