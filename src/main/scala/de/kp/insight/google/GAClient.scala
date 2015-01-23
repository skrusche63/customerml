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
  
  def execute() {
    
    val dataset = executeQuery()
    // TODO
    
  }
  
  private def buildQuery:(Int,DataQuery) = {
    
    val url = params("url")
    val query = new DataQuery(new URL(url))
    /*
     * All queries have the following 4 parameters 
     */
    val start_date = params("start_date")
    val end_date = params("end_date")
    
    val metrics = params("metrics")
    val table_id = params("table_id")

    query.setStartDate(start_date)
    query.setEndDate(end_date)
    
    query.setMetrics(metrics)
    query.setIds(table_id)
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