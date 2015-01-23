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

/**
 * DPS is short for D(iscount), P(rice) and S(hipping) and evalutes
 * the purchase history of a certain period of time with respect to
 * these attributes to gain insights into customers' price sensitivity.
 */
class DPSPreparer(ctx:RequestContext,orders:RDD[InsightOrder]) extends BasePreparer(ctx) {
        
  /*
   * The parameter K is used as an initialization 
   * parameter for the QTree semigroup
   */
  private val K = 6
  private val QUINTILES = List(0.20,0.40,0.60,0.80,1.00)
  
  import sqlc.createSchemaRDD
  override def prepare(params:Map[String,String]) {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)

    val customer = params("customer").toInt
    /*
     * STEP #1: Calculate mean amount spent per transaction and customer,
     * and also total discount & shipping ratio
     */
    val rawset = orders.groupBy(x => (x.site,x.user)).map(p => {

      val (site,user) = p._1  
      val data = p._2.map(v => (v.amount,v.discount,v.shipping))
      /*
       * Calculate the total discount and shipping ratio
       */
      val total_amount = data.map(_._1).sum
      val mean_amount = total_amount / data.size
      
      val discount_ratio = data.map(_._2).sum / total_amount
      val shipping_ratio = data.map(_._3).sum / total_amount
      
      (site,user,mean_amount,discount_ratio,shipping_ratio)
      
    })
    /*
     * STEP #2: Restrict the purchase orders to those orders that
     * refer to customers of a certain customer segment
     */
    val ctype = sc.broadcast(customer)

    val filteredDS = (if (customer == 0) {
      /*
       * This customer type indicates that ALL customer types
       * have to be taken into account when computing the item
       * segmentation 
       */
      rawset

    } else {
      /*
       * Load the Parquet file that specifies the customer type specification 
       * and filter those customers that match the provided customer type
       */
      val parquetCST = readCST(uid).filter(x => x._2 == ctype.value)      
      rawset.map(x => ((x._1,x._2),(x._3,x._4,x._5))).join(parquetCST).map(x => {

    	val ((site,user),((mean_amount,discount_ratio,shipping_ratio),rfm_type)) = x
    	(site,user,mean_amount,discount_ratio,shipping_ratio)

      })
    })       

    /*
     * STEP #2: Build quintiles from the user attributes extracted
     */
    val m_quintiles = sc.broadcast(quintiles(filteredDS.map(_._3).sortBy(x => x)))        

    val d_quintiles = sc.broadcast(quintiles(filteredDS.map(_._4).sortBy(x => x)))        
    val s_quintiles = sc.broadcast(quintiles(filteredDS.map(_._5).sortBy(x => x)))        
    
    val tableDPS = filteredDS.map(x => {
          
      val (site,user,mean_amount,discount_ratio,shipping_ratio) = x

      /********** MONETARY **********/

      val m_b1 = m_quintiles.value(0.20)
      val m_b2 = m_quintiles.value(0.40)
      val m_b3 = m_quintiles.value(0.60)
      val m_b4 = m_quintiles.value(0.80)

      val mval = (
        if (mean_amount < m_b1) 1
        else if (m_b1 <= mean_amount && mean_amount < m_b2) 2
        else if (m_b2 <= mean_amount && mean_amount < m_b3) 3
        else if (m_b3 <= mean_amount && mean_amount < m_b4) 4
        else if (m_b4 <= mean_amount) 5
        else 0  
      )

      /********** DISCOUNT ***********/

      val d_b1 = d_quintiles.value(0.20)
      val d_b2 = d_quintiles.value(0.40)
      val d_b3 = d_quintiles.value(0.60)
      val d_b4 = d_quintiles.value(0.80)
      /*
       * Customers with a small discount ratio are more valuable 
       * onces, than those that buy do to discount offerings
       */
      val dval = (
        if (discount_ratio < d_b1) 5
        else if (d_b1 <= discount_ratio && discount_ratio < d_b2) 4
        else if (d_b2 <= discount_ratio && discount_ratio < d_b3) 3
        else if (d_b3 <= discount_ratio && discount_ratio < d_b4) 2
        else if (d_b4 <= discount_ratio) 1
        else 0  
      )
                    
      /********** FREQUENCY *********/

      /*
       * Customers with a small shipping ratio are more valuable
       * onces, than those that are triggered by free shipping
       */
      val s_b1 = s_quintiles.value(0.20)
      val s_b2 = s_quintiles.value(0.40)
      val s_b3 = s_quintiles.value(0.60)
      val s_b4 = s_quintiles.value(0.80)

      val sval = (
        if (shipping_ratio < s_b1) 5
        else if (s_b1 <= shipping_ratio && shipping_ratio < s_b2) 4
        else if (s_b2 <= shipping_ratio && shipping_ratio < s_b3) 3
        else if (s_b3 <= shipping_ratio && shipping_ratio < s_b4) 2
        else if (s_b4 <= shipping_ratio) 1
        else 0  
      )

      ParquetDPS(site,user,mean_amount,mval,discount_ratio,dval,shipping_ratio,sval)
        
    })

    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)         
    tableDPS.saveAsParquetFile(store)
    
  }

}