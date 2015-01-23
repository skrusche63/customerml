package de.kp.insight.redshift
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

import java.sql._

class RedshiftClient(
  val masterUserName:String,
  val masterUserPassword:String,
  val jdbcEndpoint:String
  ) {
  
  private val POSTGRES_DRIVER = "org.postgresql.Driver"  
  private val connection = createConnection
  
  def createConnection:Connection = {

    Class.forName(POSTGRES_DRIVER).newInstance()	
	/* Generate database connection */
	DriverManager.getConnection(jdbcEndpoint,masterUserName,masterUserPassword)
    
  }
  
  def shutdown {
    if (connection != null) connection.close
  }
  
  def executeBatch(sqlStmts:Seq[String]) {
    
    var stmt:Statement  = null
    try {

      connection.setAutoCommit(false)
      
      stmt = connection.createStatement()
      for (sqlStmt <- sqlStmts) {
        stmt.addBatch(sqlStmt)
      }
      stmt.executeBatch()
      connection.commit
      
    } catch {
      case t:Throwable => {
        
        if (connection != null) {
          try {
            connection.rollback
            
          } catch  {
            case t:Throwable => {/* do nothing */} 
            
          }
          
        }
        
      }
      
    } finally {

      try {
        if (stmt != null) stmt.close()
      
      } catch{
        case t:Throwable => {/* do nothing */}
      
      }
    
    }
    
  }
  
  def execute(sqlStatement:String) {
    
    var stmt:Statement  = null
    try {

      connection.setAutoCommit(true)

      stmt = connection.createStatement()
      stmt.execute(sqlStatement)
      
      
    } catch {
      case t:Throwable => {
        
      }
      
    } finally {

      try {
        if (stmt != null) stmt.close()
      
      } catch{
        case t:Throwable => {/* do nothing */}
      
      }
    
    }
  
  }


}