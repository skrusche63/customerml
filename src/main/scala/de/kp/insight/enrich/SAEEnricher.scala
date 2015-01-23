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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.parquet._

class SAEEnricher(ctx:RequestContext,params:Map[String,String]) extends BaseEnricher(ctx,params) {
  
  import sqlc.createSchemaRDD
  override def enrich {
      
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    /*
     * STEP #1: Read customer dataset from Parquet file and join
     * with cluster reference Parquet file to assign the cluster
     * center as well as the respective distance
     */
    val ds = readDataset(params).join(readClustered(params)).map(x => {
          
      val (row,((site,user,item,col,label),(cluster,distance))) = x
      (site,user,cluster,distance,label,item,col)
          
    })
    /*
     * From the merged dataset, the customer persona distance (CPD) table
     * is derived and stored as a Parquet file
     */
    val storeCPD = String.format("""%s/%s/%s/4""",ctx.getBase,name,uid)                 
    val tableCPD = ds.map(x => ParquetCPD(x._1,x._2,x._3,x._4))
        
    tableCPD.saveAsParquetFile(storeCPD)

    /*
     * STEP #2: Dermine persona description from cluster center
     * specification and also the respective item label
     */
    val ds1 = ds.map(x => (x._5,x._6,x._7)).groupBy(x => x._3).map(x => {          
          
      val col = x._1.toInt
      val (label,item) = x._2.map(v => (v._1,v._2)).head   
          
      (col,(item,label))
          
    })
        
    val ds2 = readCentroids(params).flatMap(x => {
          
      val (cluster,features) = x
      features.zipWithIndex.map(v => {
            
        val (value,col) = v
        (col,(cluster,value))
            
      })
          
    })
    /*
     * From the result, the persona description (PSA) table
     * is derived and stored as a Parquet file.
     */
    val storePSA = String.format("""%s/%s/%s/5""",ctx.getBase,name,uid)         
    val tablePSA = ds1.join(ds2).map(x => {
          
      val (col, ((item,label), (cluster,value))) = x
      ParquetPSA(cluster,item,label,value)
          
    })
        
    tablePSA.saveAsParquetFile(storePSA)
    
  }

  private def readCentroids(params:Map[String,String]):RDD[(Int,Seq[Double])] = {
    
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/2""",ctx.getBase,name,uid)
    
    /* 
     * Read in the parquet file created above.  Parquet files are self-describing 
     * so the schema is preserved. The result of loading a Parquet file is also a 
     * SchemaRDD. 
     */
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(record => {

      val values = record.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val cluster = data("cluster").asInstanceOf[Int]      
      val features = data("features").asInstanceOf[Seq[Double]]
      
      (cluster,features)
      
    })
    
  }

  private def readClustered(params:Map[String,String]):RDD[(Long,(Int,Double))] = {
    
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/3""",ctx.getBase,name,uid)
    
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(record => {

      val values = record.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val cluster = data("cluster").asInstanceOf[Int]      
      
      val row = data("row").asInstanceOf[Long]
      val distance = data("distance").asInstanceOf[Double]
      
      (row,(cluster,distance))
      
    })
    
  }
  
  private def readDataset(params:Map[String,String]):RDD[(Long,(String,String,Int,Long,String))] ={
    
    val uid = params(Names.REQ_UID)
    val name = params(Names.REQ_NAME)
    
    val store = String.format("""%s/%s/%s/1""",ctx.getBase,name,uid)
    
    val parquetFile = sqlc.parquetFile(store)
    val metadata = parquetFile.schema.fields.zipWithIndex
    
    parquetFile.map(record => {

      val values = record.iterator.zipWithIndex.map(x => (x._2,x._1)).toMap
      
      val data = metadata.map(entry => {
      
        val (field,col) = entry
      
        val colname = field.name
        val colvalu = values(col)
      
        (colname,colvalu)
          
      }).toMap

      val site = data("site").asInstanceOf[String]     
      val user = data("user").asInstanceOf[String]
      
      val item = data("item").asInstanceOf[Int]
      
      val row = data("row").asInstanceOf[Long]      
      val col = data("col").asInstanceOf[Long]
      /*
       * After having clustered the original dataset, we are no longer interested 
       * into the affinity value a certain customer has for a specific product;
       * 
       * Still relevant is the label (or category) assigned to the data record
       */
      val label = data("label").asInstanceOf[String]      
      //val value = data("value").asInstanceOf[Double]

      (row,(site,user,item,col,label))
      
    })
    
  }
}
