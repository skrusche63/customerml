package de.kp.insight
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
import org.joda.time.format.DateTimeFormat
import de.kp.spark.core.actor.RootActor

abstract class BaseActor(ctx:RequestContext) extends RootActor(Configuration) {

  implicit val ec = context.dispatcher
  
  protected val sc = ctx.sparkContext
  protected val sqlc = ctx.sqlCtx
  
  protected def formatted(time:Long,category:String):String = DateUtil.formatted(time,category)
  
  protected def unformatted(date:String,category:String):Long = DateUtil.unformatted(date,category)
  
}