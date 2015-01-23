package de.kp.insight.geoip
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

import de.kp.insight._
import de.kp.insight.model._

import com.maxmind.geoip.{LookupService,regionName}

object LocationFinder extends Serializable {
  
  private val source = Configuration.geoip
  private val lookup = new LookupService(source, LookupService.GEOIP_MEMORY_CACHE);
  
  def locate(ip:String):Location = {

    if (ip == null || ip.isEmpty) {
      Location("","","","",0,0,0,"","",0,0)
    
    } else {
    
	  val locationInfo = lookup.getLocation(ip)

      val countrycode = if (locationInfo.countryCode == null) "" else locationInfo.countryCode
	  val countryname = if (locationInfo.countryName == null) "" else locationInfo.countryName

	  val region = if (locationInfo.region == null) "" else locationInfo.region
	  val regionname = if (regionName.regionNameByCode(countrycode,region) == null) "" else regionName.regionNameByCode(countrycode,region)
	   
	  val areacode = if (locationInfo.area_code.isNaN()) 0 else locationInfo.area_code
	  val dmacode = if (locationInfo.dma_code.isNaN()) 0 else locationInfo.dma_code

	  val metrocode = if (locationInfo.metro_code.isNaN()) 0 else locationInfo.metro_code

	  val city = if (locationInfo.city == null) "" else locationInfo.city
	  val postalcode = if (locationInfo.postalCode == null) "" else locationInfo.postalCode
	
	  val lat = if (locationInfo.latitude.isNaN()) 0 else locationInfo.latitude
	  val lon = if (locationInfo.longitude.isNaN()) 0 else locationInfo.longitude
	
	  Location(
	    countryname,
	    countrycode,
	    region,
	    regionname,
	    areacode,
	    dmacode,
	    metrocode,
	    city,
	    postalcode,
	    lat,
	    lon
	  )
	  
    }
  }

  def main(args:Array[String]) {
    
    val ip = "80.137.88.4"
    println(locate(ip))
  
  }
}