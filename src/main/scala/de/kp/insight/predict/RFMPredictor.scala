package de.kp.insight.predict
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

import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.linalg.{Vector,Vectors}

import org.joda.time.DateTime

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.model._

import de.kp.insight.util.ElasticRDD

import org.elasticsearch.index.query._
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

class RFMPredictor(ctx:RequestContext,params:Map[String,String]) extends BasePredictor(ctx,params) {
  
  import sqlc.createSchemaRDD
  override def predict(params:Map[String,String]) {
    
    /*
     * STEP #1: Determine the content of the customers/segments index
     * of a certain customer type
     */
    val esConfig = ctx.getESConfig

    esConfig.set(Names.ES_RESOURCE,("customers/segments"))
    esConfig.set(Names.ES_QUERY,query(params))
      
    val elasticRDD = new ElasticRDD(ctx.sparkContext)         
    val rawset = elasticRDD.rfm(elasticRDD.read(esConfig))
    /*
     * STEP #2: We group the result by the timestamp parameter
     */
    val dataset = rawset.groupBy{case (site,user,amount,recency,frequency,timestamp) => timestamp}
    /*
     * Sum up the amounts spent by every individual customer of the 
     * selected customer type; this gives the total revenue from this 
     * customer segment for a certain timestamp
     */
    val monetary = dataset.map(x => (x._1,x._2.map(_._3).sum))    
    val mset = monetary.map{case (timestamp,label) => LabeledPoint(label,Vectors.dense(timestamp))}     
    /*
     * Build the mean recency for a certain customer type
     */
    val recency = dataset.map(x => (x._1, x._2.map(_._4).sum.toDouble / x._2.size))    
    val rset = recency.map{case (timestamp,label) => LabeledPoint(label,Vectors.dense(timestamp))}     
    
    /*
     * Sum up the purchase frequency of erevy individual customer of the 
     * selected customer type; this gives the total purchase frequency of 
     * this customer segment for a certain timestamp
     */
    val frequency = dataset.map(x => (x._1,x._2.map(_._5).sum))    
    val fset = frequency.map{case (timestamp,label) => LabeledPoint(label,Vectors.dense(timestamp))} 
    
    /*
     * STEP #3: Build models and use either request or default parameters
     * for the number of iterations and step size (lambda); note, that we
     * use linear regression with SGD
     */
    val iter = params.get("iter") match { 
      case None => 20
      case Some(value) => value.toInt
    }

    val lambda = params.get("lambda") match { 
      case None => 0.01
      case Some(value) => value.toDouble
    }
    /* Monetary model */
    val (mmodel,merror) = buildModel(mset,iter,lambda)
    /* Recency model */
    val (rmodel,rerror) = buildModel(rset,iter,lambda)
    /* Frequency model */
    val (fmodel,ferror) = buildModel(fset,iter,lambda)
    
    /*
     * STEP #4: Compute forecast values for recency, frequency and monetary 
     * for a certain period of time and 
     */
    val period = params("period")
    val total = params("total").toInt
    
    val now = new DateTime()
    val timestamps = period match {
      
      case "day" => (1 to total).map(i => now.plusDays(i).getMillis)
      
      case "week" => (1 to total).map(i => now.plusWeeks(i).getMillis)
      
      case "month" => (1 to total).map(i => now.plusMonths(i).getMillis)
      
      case "year" => (1 to total).map(i => now.plusYears(i).getMillis)
      
      case _ => throw new Exception("Parameter 'period' supports [day, week, month, year].")
      
    }
    
    val forecasts = timestamps.map(x => {
      
      val point = Vectors.dense(x)
      
      val mp = mmodel.predict(point)      
      val rp = rmodel.predict(point)
      
      val fp = rmodel.predict(point)
      
      (x, rp,fp, mp)
      
    })
        
    val sources = transform(params,forecasts)

    if (ctx.putSources("forecasts","rfm",sources) == false)
      throw new Exception("Loading process has been stopped due to an internal error.")
    
  }
  
  private def buildModel(trainset:RDD[LabeledPoint],iterations:Int,lambda:Double):(LinearRegressionModel,Double) = {

    val model = LinearRegressionWithSGD.train(trainset,iterations,lambda)
    val predictions = model.predict(trainset.map(_.features))   
    
    val loss = predictions.zip(trainset.map(_.label)).map{case(prediction,label) => {

      val err = (prediction - label)
      err*err
      
    }}.sum
    
    val rmse = math.sqrt(loss / trainset.count)
    
    (model,rmse)
    
  }
  
  private def query(params:Map[String,String]):String = {
    
    val customer = params("customer").toInt

    val fbuilder = FilterBuilders.termFilter("rfm_type",customer)
    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)

    qbuilder.toString
    
  }
  
  private def transform(params:Map[String,String],dataset:Seq[(Long,Double,Double,Double)]):List[XContentBuilder] = {
            
    dataset.map(x => {
      
      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
      
      /********** METADATA **********/
      
      /* uid */
      builder.field(Names.UID_FIELD,params(Names.REQ_UID))
      
      /* timestamp */
      builder.field(Names.TIMESTAMP_FIELD,params("timestamp").toLong)
 	  
	  /* site */
      builder.field(Names.SITE_FIELD,params(Names.REQ_SITE))
      
      /********** FORECAST DATA **********/
	  
	  /* recency */
      builder.field("recency",Math.round(x._2).toInt)

	  /* frequency */
	  builder.field("frequency",Math.round(x._3).toInt)

	  /* amount */
	  builder.field("amount",x._4)

	  /* timestamp */
	  builder.field("timestamp",x._1)

	  /* customer_type */
	  builder.field("customer_type",params("customer").toInt)
	  
	  builder.endObject()
	  
	  builder.endObject()
      builder
      
    }).toList
    
  }
  
}