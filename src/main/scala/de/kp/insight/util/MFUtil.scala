package de.kp.insight.util
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

import java.io.{ObjectInputStream,ObjectOutputStream} 

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,Path}

private class MFStruct(
    val rank:Int,
    val rmse:Double,
    val udict:Map[String,Int],
    val idict:Map[String,Int],
    val ufeat:Array[(Int,Array[Double])],
    val ifeat:Array[(Int,Array[Double])]
) extends Serializable {}

class MFUtil(@transient sc:SparkContext) extends Serializable {
  
  def read(store:String):(Map[String,Int],Map[String,Int],MFModel) = {
    
    val conf = new Configuration()
	val fs = FileSystem.get(conf)
    
    val ois = new ObjectInputStream(fs.open(new Path(store)))
    val struct = ois.readObject().asInstanceOf[MFStruct]
      
    ois.close()
    
    val rank = struct.rank
    val rmse = struct.rmse
    
    val udict = struct.udict
    val idict = struct.idict
    
    val ufeat = struct.ufeat
    val ifeat = struct.ifeat
    
    (udict,idict,new MFModel(rank,rmse,sc.parallelize(ufeat),sc.parallelize(ifeat)))
    
  }
  
  def write(store:String,udict:Map[String,Int],idict:Map[String,Int],model:MFModel) {
    /*
     * STEP #1: Convert model into MFStruct as RDDs cannot
     * be persisted directly
     */
    val struct = new MFStruct(
        model.rank,
        model.rmse,
        udict,
        idict,
        model.userFeatures.collect,
        model.productFeatures.collect
    )
    
    val conf = new Configuration()
	val fs = FileSystem.get(conf)

    val oos = new ObjectOutputStream(fs.create(new Path(store)))   
    oos.writeObject(struct)
    
    oos.close
    
  }

}
