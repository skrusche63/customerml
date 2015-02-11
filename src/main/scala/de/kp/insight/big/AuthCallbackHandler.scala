package de.kp.insight.big

import javax.servlet.http.{HttpServlet,HttpServletRequest,HttpServletResponse}

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.ServletHandler

import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.server.ssl.SslSocketConnector

import org.eclipse.jetty.server.Connector
import scala.collection.JavaConversions._

object OAuthCBServer {

  private val KEYPASS = "bigcommerce"
  def main(args:Array[String]) {
        
    /* 
     * Create a basic jetty server object that will listen on port 8080.  
     * Note that if you set this to port 0 then a randomly available port 
     * will be assigned that you can either look in the logs for the port,
     * or programmatically obtain it for use in test cases.
     */   
    val server = new Server(8080)

    /* Encrypt the connection using a valid certificate/keystore */
    val sslContextFactory = new SslContextFactory("keystore.jks")
    
    sslContextFactory.setKeyStorePassword(KEYPASS)
    sslContextFactory.setKeyManagerPassword(KEYPASS)

    sslContextFactory.setTrustStore("keystore.jks")
    sslContextFactory.setTrustStorePassword(KEYPASS)
    
    
    /* Create new socket connector using the contextFactory */
    val sslConnector = new SslSocketConnector(sslContextFactory)
    sslConnector.setPort(8443)

    /* Add the SocketConnector to the server */
    server.setConnectors(Array[Connector](sslConnector))     
    /*
     * The ServletHandler is a dead simple way to create a context handler 
     * that is backed by an instance of a Servlet. This handler then needs 
     * to be registered with the Server object.
     */  
    val handler = new ServletHandler()
    server.setHandler(handler)
 
    /* 
     * Passing in the class for the servlet allows jetty to instantite an 
     * instance of that servlet and mount it on a given context path.
     *
     * This is a raw Servlet, not a servlet that has been configured through 
     * a web.xml or anything like that !!
     */
    handler.addServletWithMapping(classOf[CallbackServlet], "/oauth")
 
    /* 
     * Start things up! By using the server.join() the server thread will join 
     * with the current thread. 
     */
    server.start()
    server.join()
     
  }
  
  private class CallbackServlet extends HttpServlet {
    
    override def doGet(request:HttpServletRequest,response:HttpServletResponse) {
  
      println("GET REQUEST: " + request.getQueryString())
      val params = request.getParameterMap()

      val code = params("code")(0)
      val scope = params("scope")(0)
      
      val context = params("context")(0)
      
      AuthUtil.saveAuthInfo(code, scope, context)
      
      response.setContentType("application/json")
      response.setStatus(HttpServletResponse.SC_OK)
      
    }
    
  }

}