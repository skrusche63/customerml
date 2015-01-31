package de.kp.insight.mag
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

import java.io.{BufferedReader,BufferedWriter,File,IOException}
import java.nio.charset.Charset
import java.nio.file.Files

object FileUtil {

  val CHARSET = Charset.forName("UTF-8")

  def readFile(file:java.io.File):String = {
    
    val lines = scala.io.Source.fromFile(file).getLines()
    
    val sb = new StringBuilder()
    lines.foreach(line => sb.append(line))
    
    sb.toString()

  }

  def saveFile(content:String,file:java.io.File) {
    
    val writer = Files.newBufferedWriter(file.toPath(),CHARSET)

    writer.write(content,0,content.length())    
    writer.close()

  }

}