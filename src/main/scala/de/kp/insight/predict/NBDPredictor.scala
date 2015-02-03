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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.insight.RequestContext
import de.kp.insight.model._

import de.kp.insight.math._
import de.kp.insight.util.ElasticRDD

import org.joda.time.DateTime

import org.elasticsearch.index.query._
import org.elasticsearch.common.xcontent.{XContentBuilder,XContentFactory}

import org.apache.commons.math3.analysis.MultivariateFunction

import org.apache.commons.math3.optim.InitialGuess
import org.apache.commons.math3.optim.MaxEval

import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.MultiDirectionalSimplex
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.PowellOptimizer

import org.apache.commons.math3.optim.nonlinear.scalar.GoalType
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction

import scala.collection.mutable.Buffer

class NBDPredictor(ctx:RequestContext,params:Map[String,String]) extends BasePredictor(ctx,params) {
        
  protected val DAY = 24 * 60 * 60 * 1000 // day in milliseconds
  
  import sqlc.createSchemaRDD
  override def predict(params:Map[String,String]) {

    /*
     * STEP #1: Retrieve orders from the orders/base index;
     * the respective query is either provided as an external
     * parameter or computed from the period of time provided
     * with this request
     */
    val esConfig = ctx.getESConfig
    esConfig.set(Names.ES_RESOURCE,("orders/base"))
      
    esConfig.set(Names.ES_QUERY,query(params))
    val elasticRDD = new ElasticRDD(ctx.sparkContext)
         
    val rawset = elasticRDD.read(esConfig)
    val orders = elasticRDD.orders(rawset)

    /*
     * STEP #2: Retrieve timestamps of orders received to
     * calculate [0,T]; note, that we do not describe the
     * time interval in terms of weeks (see Bruce Hardie)
     * but in days
     */
    val order_ts = orders.map(_.timestamp).collect().sorted
    
    val begin = order_ts.head
    val end   = order_ts.last
    
    val T = ((end - begin) / DAY).toDouble
    
    val bbegin = ctx.sparkContext.broadcast(begin)
    val dataset = orders.groupBy(x => (x.site,x.user)).map(p => {

      val (site,user) = p._1  
      
      /* 
       * Compute time ordered list of timestamps; take the
       * latest purchase timestamp and calculate difference
       * from the begin timestamp = Pareto/NDB recency 
       */
      val times = p._2.map(_.timestamp)
      val latest = times.toSeq.sorted.last
      
      val x = ((latest - bbegin.value) / DAY).toDouble
      val tx = p._2.size
          
      (site,user,x,tx)

    }).collect()
    
    def logLikelihood(r:Double,alpha:Double,s:Double,beta:Double):Double = {
      dataset.map(x => Math.log(ParetoNBDLikelihood(r,alpha,s,beta,x._3,x._4,T))).sum
    }

    val f = new MultivariateFunction() {
			  override def value(z:Array[Double]):Double = {			    
			    val Array(alpha,beta,r,s) = z
				logLikelihood(r,alpha,s,beta)
			}
		}
	
    val iterations = if (params.contains("iterations")) params("iterations").toInt else 20
    
    val eps = if (params.contains("eps")) params("eps").toDouble else 1e-2
    val max_eval = if (params.contains("max_eval")) params("max_eval").toInt else 2000
    
    /*
     * STEP #3: Determine Pareto/NDB factors alpha,beta,r,s
     */
    var initialGuess = new InitialGuess(Array[Double](1,1,0.5,0.5))
    (0 until iterations).foreach(i => {
			
      var optimizerMult = new PowellOptimizer(eps,eps)
	  var solutionMult = optimizerMult.optimize(
	      new MaxEval(max_eval), 
	      new ObjectiveFunction(f), 
	      GoalType.MAXIMIZE, 
	      initialGuess,
	      new MultiDirectionalSimplex(4)
	  )

	  initialGuess = new InitialGuess(solutionMult.getKey())
      
    })
    
    val Array(alpha,beta,r,s) = initialGuess.getInitialGuess()
    
    /*
     * STEP #4: Compute forecast values for recency, frequency and monetary 
     * for a certain period of time and 
     */
    val period = params("period")
    val total = params("total").toInt
    
    val now = new DateTime()
    val future_ts = period match {
      
      case "day" => (1 to total).map(i => now.plusDays(i).getMillis)
      
      case "week" => (1 to total).map(i => now.plusWeeks(i).getMillis)
      
      case "month" => (1 to total).map(i => now.plusMonths(i).getMillis)
      
      case "year" => (1 to total).map(i => now.plusYears(i).getMillis)
      
      case _ => throw new Exception("Parameter 'period' supports [day, week, month, year].")
      
    }
    
    val intervals = future_ts.map(ts => ((ts - end) / DAY).toDouble)
    val forecasts = dataset.flatMap{case (site,user,x,tx) => {
      
      future_ts.map(ts => {
        
        val t = ((ts - end) / DAY).toDouble       
        (site,user,ts,ParetoNBDExpectation(r,alpha,s,beta,x,tx,T,t))
      
      })
      
    }}.toSeq
     
    val sources = transform(params,forecasts)

    if (ctx.putSources("forecasts","nbd",sources) == false)
      throw new Exception("Loading process has been stopped due to an internal error.")
  
  }

  private def transform(params:Map[String,String],dataset:Seq[(String,String,Long,Double)]):List[XContentBuilder] = {
            
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
	  
	  /* user */
      builder.field("user",x._2)

      /* time */
	  builder.field("time",x._3)

	  /* frequency */
	  builder.field("frequency",x._4)

	  /* customer_type */
	  builder.field("customer_type",params("customer").toInt)
	  
	  builder.endObject()
	  
	  builder.endObject()
      builder
      
    }).toList
    
  }
 
  private def query(params:Map[String,String]):String = {
    
    val created_at_min = unformatted(params("created_at_min"))
    val created_at_max = unformatted(params("created_at_max"))
            
    val filters = Buffer.empty[FilterBuilder]
    filters += FilterBuilders.rangeFilter("time").from(created_at_min).to(created_at_max)
    
    filters.toList
            
    val fbuilder = FilterBuilders.boolFilter()
    fbuilder.must(filters:_*)
    
    val qbuilder = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),fbuilder)
    qbuilder.toString
    
  }
  
}