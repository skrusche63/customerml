package de.kp.insight.profile
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
import de.kp.spark.core.model._

import de.kp.insight._
import de.kp.insight.parquet._

import de.kp.insight.model._

/**
 * CPPProfiler builds the customer persona profile
 */
class CPPProfiler(ctx:RequestContext,params:Map[String,String]) extends BaseActor(ctx) {
  
  import sqlc.createSchemaRDD

  override def receive = {
   
    case message:StartProfile => {
      
      val req_params = params
      
      val uid = req_params(Names.REQ_UID)
      val name = req_params(Names.REQ_NAME)
      
      val start = new java.util.Date().getTime.toString            
      ctx.putLog("info",String.format("""[UID: %s] %s profiling request received at %s.""",uid,name,start))
      
      try {
        
        profiling()
        
        val end = new java.util.Date().getTime.toString
        ctx.putLog("info",String.format("""[UID: %s] %s profiling finished at %s.""",uid,end))

        
      } catch {
        case e:Exception => {

          ctx.putLog("error",String.format("""[UID: %s] %s profiling failed due to an internal error.""",uid,name))
          
          val params = Map(Names.REQ_MESSAGE -> e.getMessage) ++ req_params

          context.parent ! ProfileFailed(params)            
          context.stop(self)
          
        }
      
      }
    
    }
    
  }

  private def profiling() {
 
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    /*
     * STEP #1: Retrieve result from CDA (customer day affinity) 
     * enricher, that describes the respective persona (cluster) 
     * and the distance of the customer from that persona
     */
    val nameCDA = name.replace("CTP","CDA")    
    val storeCDA = String.format("""%s/%s/%s/4""",ctx.getBase,nameCDA,uid)         
    
    val tableCDA = readCPD(storeCDA)
    /*
     * STEP #2: Retrieve result from CHA (customer hour affinity) 
     * enricher, that describes the respective persona (cluster) 
     * and the distance of the customer from that persona
     */
    val nameCHA = name.replace("CTP","CHA")    
    val storeCHA = String.format("""%s/%s/%s/4""",ctx.getBase,nameCHA,uid)         
    
    val tableCHA = readCPD(storeCHA)
    /*
     * STEP #3: Retrieve result from CSA (customer timespan affinity) 
     * enricher, that describes the respective persona (cluster) 
     * and the distance of the customer from that persona
     */
    val nameCSA = name.replace("CTP","CSA")    
    val storeCSA = String.format("""%s/%s/%s/4""",ctx.getBase,nameCSA,uid)         
    
    val tableCSA = readCPD(storeCSA)
    /*
     * STEP #3: Join the CDA, CHA & CSA table and build the temporal
     * or time based customer profile
     */    
    val tableCTP = tableCDA
                 .join(tableCHA)
                 .join(tableCSA)
                 .map{case ((site,user),(((d_persona,d_distance),(h_persona,h_distance)),(s_persona,s_distance))) => {
                   ((site,user),(d_persona,d_distance,h_persona,h_distance,s_persona,s_distance))
                 }}

    /*
     * STEP #4: Retrieve the result from the CPA (customer product affinity)
     * enricher, that describes the respective persona (cluster) 
     * and the distance of the customer from that persona
     */    
    val nameCPA = name.replace("CTP","CPA")    
    val storeCPA = String.format("""%s/%s/%s/4""",ctx.getBase,nameCPA,uid)         
    
    val tableCPA = readCPD(storeCPA)    
    /*
     * STEP #5: Retrieve the result from the RFM preparer that describes
     * the customer type segmentation
     */    
    val nameRFM = "RFM"    
    val storeRFM = String.format("""%s/%s/%s/1""",ctx.getBase,nameRFM,uid)         
    
    val tableRFM = readRFM(storeRFM)    
    /*
     * STEP #6: Retrieve the result from the CLS preparer that describes
     * the customer type segmentation
     */    
    val nameCLS = name.replace("CTP","CLS")    
    val storeCLS = String.format("""%s/%s/%s/1""",ctx.getBase,nameCLS,uid)         
    
    val tableCLS = readCLS(storeCLS)    
    
    val tableCPP = tableCTP.join(tableCPA).join(tableRFM).join(tableCLS)
                 .map{case ((site,user),((((d_type,d_dist,h_type,h_dist,r_type,r_dist),(p_persona,p_distance)),(recency,frequency,monetary,rval,fval,mval,rfm_type)),(lval))) =>
                   ParquetCPP(
                      site,
                      user,
                      recency,
                      frequency,
                      monetary,
                      rval,
                      fval,
                      mval,
                      lval,
                      rfm_type,
                      d_type,
                      d_dist,
                      h_type,
                      h_dist,
                      r_type,
                      r_dist,
                      p_persona,
                      p_distance)
                 }
    
    val storeCPP = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)  
    tableCPP.saveAsParquetFile(storeCPP)
  }
  
  private def readCLS(store:String):RDD[((String,String),(Int))] = {
    
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
      
      val amount = data("amount").asInstanceOf[Double]
      val timespan = data("timespan").asInstanceOf[Int]

      val loyalty = data("loyalty").asInstanceOf[Integer]
      val rfm_type = data("rfm_type").asInstanceOf[Int]

      ((site,user),(loyalty))
      
    })
    
  }

  /**
   * A helper method tor read the customer persona distance (CPD)
   * Parquet file from the file system; note, that this file has
   * been created by the SAEnricher (this leads to /4)
   */
  private def readCPD(store:String):RDD[((String,String),(Int,Double))] = {
   
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
      
      val persona = data("persona").asInstanceOf[Int]
      val distance = data("distance").asInstanceOf[Double]

      ((site,user),(persona,distance))
      
    })
    
  }
  
  private def readRFM(store:String):RDD[((String,String),(Int,Int,Double,Int,Int,Int,Int))] = {
    
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
      
      val today = data("today").asInstanceOf[Long]
      val recency = data("recency").asInstanceOf[Int]

      val frequency = data("frequency").asInstanceOf[Int]      
      val monetary = data("monetary").asInstanceOf[Double]

      val rval = data("rval").asInstanceOf[Int]
      val fval = data("fval").asInstanceOf[Int]
      
      val mval = data("mval").asInstanceOf[Int]
      val rfm_type = data("rfm_type").asInstanceOf[Int]

      ((site,user),(recency,frequency,monetary,rval,fval,mval,rfm_type))
      
    })
  }
}