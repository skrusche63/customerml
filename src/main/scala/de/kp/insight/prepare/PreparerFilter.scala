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

import de.kp.insight._
import de.kp.insight.model._

class PreparerFilter(ctx:RequestContext,orders:RDD[InsightOrder]) {

  /**
   * Restrict the purchase orders to those items and attributes
   * that are relevant for the CAR preparation task; this encloses a
   * filtering with respect to customer type, if different from '0'
   */
  def filterCAR(customer:Int,uid:String):RDD[(String,String,String,Long,List[InsightOrderItem])] = {
    
    val ctype = ctx.sparkContext.broadcast(customer)

    val ds = orders.map(x => (x.site,x.user,x.ip_address,x.timestamp,x.items))
    (if (customer == 0) {
      /*
       * This customer type indicates that ALL customer types
       * have to be taken into account when computing the item
       * segmentation 
       */
      ds
          
    } else {
      /*
       * Load the Parquet file that specifies the customer type 
       * specification and filter those customers that match the 
       * provided customer type
       */
      val parquetCST = loadCustomerTypes(uid).filter(x => x._2 == ctype.value)      
      ds.map{
        
        case(site,user,ip_address,timestamp,items) => 
          ((site,user),(ip_address,timestamp,items))
      
      }.join(parquetCST).map{
        
        case((site,user),((ip_address,timestamp,items),rfm_type)) => 
          (site,user,ip_address,timestamp,items)
      }
      
    })     

  }
  /**
   * Restrict the purchase orders to those items and attributes that are 
   * relevant for the item segmentation task; this encloses a filtering 
   * with respect to customer type, if different from '0'
   */  
  def filterCDA(customer:Int,uid:String):RDD[(String,String,Int)] = {
    
    val ctype = ctx.sparkContext.broadcast(customer)

    val ds = orders.map(x => (x.site,x.user,new DateTime(x.timestamp).dayOfWeek().get))
    (if (customer == 0) {
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
      val parquetCST = loadCustomerTypes(uid).filter(x => x._2 == ctype.value)      
      ds.map(x => ((x._1,x._2),(x._3))).join(parquetCST).map(x => {
            
        val ((site,user),((day),rfm_type)) = x
        (site,user,day)
            
      })
    })     
    
  }
  /**
   * Restrict the purchase orders to those items and attributes that are 
   * relevant for the item segmentation task; this encloses a filtering 
   * with respect to customer type, if different from '0'
   */
  def filterCHA(customer:Int,uid:String):RDD[(String,String,Int)] = {
    
    val ctype = ctx.sparkContext.broadcast(customer)

    val ds = orders.map(x => (x.site,x.user,new DateTime(x.timestamp).hourOfDay().get))
    (if (customer == 0) {
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
      val parquetCST = loadCustomerTypes(uid).filter(x => x._2 == ctype.value)      
      ds.map(x => ((x._1,x._2),(x._3))).join(parquetCST).map(x => {
            
        val ((site,user),((hour),rfm_type)) = x
        (site,user,hour)
            
      })
    })     
    
  }
  /**
   * Restrict the purchase orders to those items and attributes that are
   * relevant for the item segmentation task; this encloses a filtering 
   * with respect to customer type, if different from '0'
   */
   def filterCPA(customer:Int,uid:String):RDD[(String,String,Int,Int,String)] = {
    
    val ctype = ctx.sparkContext.broadcast(customer)

    val ds = orders.flatMap(x => x.items.map(v => (x.site,x.user,v.item,v.quantity,v.category)))
    (if (customer == 0) {
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
    	val parquetCST = loadCustomerTypes(uid).filter(x => x._2 == ctype.value)      
    	ds.map(x => ((x._1,x._2),(x._3,x._4,x._5))).join(parquetCST).map(x => {

    	  val ((site,user),((item,quantity,category),rfm_type)) = x
    	  (site,user,item,quantity,category)

    	})
    })     
    
  }
  /**
   * Restrict the purchase orders to those attributes that are 
   * relevant for the state generation task; this encloses a
   * filtering with respect to customer type, if different 
   * from '0'
   */
  def filterCPS(customer:Int,uid:String):RDD[(String,String,Double,Long)] = {
    
    val ctype = ctx.sparkContext.broadcast(customer)

    val ds = orders.map(x => (x.site,x.user,x.amount,x.timestamp))
    (if (customer == 0) {
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
      val parquetCST = loadCustomerTypes(uid).filter(x => x._2 == ctype.value)      
      ds.map(x => ((x._1,x._2),(x._3,x._4))).join(parquetCST).map(x => {

    	val ((site,user),((amount,timestamp),rfm_type)) = x
    	(site,user,amount,timestamp)

  	  })
    })     
    
  }
  /**
   * Restrict the purchase orders to those items and attributes that are 
   * relevant for the item segmentation task; this encloses a filtering 
   * with respect to customer type, if different from '0'
   */
  def filterPPF(customer:Int,uid:String):RDD[(String,String,Int,Int)] =  {
    
    val ctype = ctx.sparkContext.broadcast(customer)

    val ds = orders.flatMap(x => x.items.map(v => (x.site,x.user,v.item,v.quantity)))
    (if (customer == 0) {
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
      val parquetCST = loadCustomerTypes(uid).filter(x => x._2 == ctype.value)      
      ds.map(x => ((x._1,x._2),(x._3,x._4))).join(parquetCST).map(x => {

    	val ((site,user),((item,quantity),rfm_type)) = x
    	(site,user,item,quantity)

      })
    })       
    
  }
  /**
   * This method loads the customer type description from the
   * Parquet file that has been created by the RFMPreparer.
   * 
   * The customer type specification is used to filter other
   * customer datasets by an RDD join mechanism to reduce to
   * those records that refer to a certain customer type
   */
  protected def loadCustomerTypes(uid:String):RDD[((String,String),Int)] = {

    val store = String.format("""%s/CST/%s""",ctx.getBase,uid)         
    
    val parquetFile = ctx.sqlCtx.parquetFile(store)
    val schema = ctx.sparkContext.broadcast(parquetFile.schema)
    
    parquetFile.map(row => {

      val data = schema.value.fields.zip(row.toSeq).map{case(field,value) => (field.name,value)}.toMap

      val site = data("site").asInstanceOf[String]
      val user = data("user").asInstanceOf[String]
      
      val rfm_type = data("rfm_type").asInstanceOf[Int]
      ((site,user),rfm_type)      
    
    })
    
  }

}