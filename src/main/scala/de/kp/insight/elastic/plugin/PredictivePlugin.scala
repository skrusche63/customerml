package de.kp.insight.elastic.plugin
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

import java.util.Collection

import org.elasticsearch.common.collect.Lists
import org.elasticsearch.common.component.LifecycleComponent
import org.elasticsearch.common.inject.Module
import org.elasticsearch.common.settings.Settings

import org.elasticsearch.plugins.AbstractPlugin

import org.elasticsearch.rest.RestModule
import org.elasticsearch.river.RiversModule

class PredictivePlugin(val settings:Settings) extends AbstractPlugin {
    
  override def name():String = {
    "shopify-insight"
  }

  override def description():String = {
    "Plugin that brings predictive analytics to Elasticsearch";
  }

 /**
  * REST API
  */
 def onModule(module:RestModule) {

    module.addRestAction(classOf[RecommendAction])
    module.addRestAction(classOf[TimeSeriesAction])
    
  }

  /**
   * River API - not supported
   */
  def onModule(module:RiversModule) {
  }

  /**
   * Module API
   */
  override def modules():Collection[Class[_ <: Module]] = {

    val modules:Collection[Class[_ <: Module]] = Lists.newArrayList()
  	/* The module is bound to the PredictiveService */
  	modules.add(classOf[PredictiveModule])
        
    modules
    
  }

  /**
   * Service API
   */
  override def services():Collection[Class[_ <: LifecycleComponent[_]]] = {
    	
    val services:Collection[Class[_ <: LifecycleComponent[_]]] = Lists.newArrayList()
    services.add(classOf[PredictiveService])        
      
    services
    
  }
	
}