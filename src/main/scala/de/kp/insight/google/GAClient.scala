package de.kp.insight.google
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
import java.net.URL

import com.google.gdata.client.analytics.AnalyticsService
import com.google.gdata.client.analytics.DataQuery

import com.google.gdata.data.analytics.{DataEntry, DataFeed}

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

case class GARow(columns:Seq[GAColumn])
case class GAColumn(name:String,category:String,datatype:String,value:String)

class GAClient(params:Map[String,String]) {
  
  private val service = init
  private val (total,query) = buildQuery
  
  private def init:AnalyticsService = {
  
    val app_name = params("app_name")
    val analytics = new AnalyticsService(app_name)
   
    val user_name = params("user_name")
    val password = params("password")
    
    analytics.setUserCredentials(user_name,password)   
    analytics
   
  }
  
  def execute():Seq[GARow] = {
    
    val dataset = executeQuery()
    /*
     * The data feed returns data that is entirely dependent on the fields specified 
     * in the request using the dimensions and metrics parameters. 
     * 
     * The dimensions parameter defines the primary data keys for an Analytics report, 
     * such as ga:browser or ga:city. Dimensions are used to segment metrics.
     * 
     * For example, while one can ask for the total number of pageviews to a website, 
     * it might be more interesting to ask for the number of pageviews segmented by 
     * browser.
     * 
     * In this case, one sees the number of pageviews from Firefox, Internet Explorer, 
     * Chrome, and so forth. When the value of the dimension cannot be determined, 
     * Analytics uses the special string (not set). 
     * 
     */
    dataset.flatMap(batch => {
      
      batch.getEntries.map(entry => {

        val columns = Buffer.empty[GAColumn]

        /* DIMENSIONS */
        val dimensions = entry.getDimensions
        if (!dimensions.isEmpty) { 
          dimensions.map(dimension => GAColumn(dimension.getName,"dimension","string",dimension.getValue)) 
        }
        
        /* METRICS */
        val metrics = entry.getMetrics
        metrics.map(metric => GAColumn(metric.getName,"metric",metric.getType,metric.getValue))
      
        GARow(columns.toSeq)
 
      })

    })
    
  }
  
  private def buildQuery:(Int,DataQuery) = {
    
    val url = params("url")
    val query = new DataQuery(new URL(url))
    
    /*
     * All queries have the following 4 parameters 
     */
    val start_date = params("start_date")
    val end_date = params("end_date")
    
    /*
     * REQUIRED
     * 
     * The aggregated statistics for user activity in a view (profile), 
     * such as clicks or pageviews. When queried by alone, metrics provide 
     * the total values for the requested date range, such as overall pageviews 
     * or total bounces. 
     * 
     * However, when requested with dimensions, values are segmented by the dimension. 
     * For example, ga:pageviews requested with ga:country returns the total pageviews 
     * per country. 
     * 
     * When requesting metrics, keep in mind: All requests require at least one metric.
     * 
     * You can supply a maximum of 10 metrics for any query.Not all dimensions and metrics 
     * can be used together. Consult the Valid Combinations tool to see which combinations 
     * work together.
     * 
     */
    val metrics = params("metrics")
    query.setMetrics(metrics)
    /*
     * REQUIRED
     * 
     * The unique table ID used to retrieve the Analytics Report data.
     */
    val table_id = params("table_id")
    query.setIds(table_id)

    query.setStartDate(start_date)
    query.setEndDate(end_date)
    
    /*
     * Add optional parameters
     */    
    if (params.contains("dimensions")) {
      query.setDimensions(params("dimensions"))
    }

    if (params.contains("filters")) {
      query.setFilters(params("filters"))
    }

    if (params.contains("sort")) {
      query.setSort(params("sort"))
    }
    
    val max_results = if (params.contains("max_results")) {
      params("max_results").toInt
    } else 10000
    
    query.setMaxResults(max_results)
    (max_results,query)
     
  }
  
  /**
   *
   * Runs the query as many times as necessary to export all
   * the data. This is because Google Analytics sets a per-
   * query cap, typically of 10,000 rows per run.
   */
  protected def executeQuery(dataset:List[DataFeed] = Nil):List[DataFeed] = dataset match {

    /*
     * Specifies the starting point of the execution
     */
    case Nil => executeQuery(List(executeQueryRun(query)))

    /*
     * Specifies an intermediate point of the execution
     */
    case head :: _ => head.getEntries().size match {

      /* 
       * If we have retrieved the maximum possible results, run again
       */
      case `total` => {
        /* 
         * Increment query start point and rerun query
         */
        query.setStartIndex(query.getStartIndex + total)
        executeQuery(executeQueryRun(query) :: dataset)
      
      }
      /*
       * Execution is finished
       */
      case _ => dataset.reverse
      
    }
    
  }
 
  protected def executeQueryRun(query:DataQuery):DataFeed =
    service.getFeed(query.getUrl,classOf[DataFeed])

}