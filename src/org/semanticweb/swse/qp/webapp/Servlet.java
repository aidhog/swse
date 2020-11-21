package org.semanticweb.swse.qp.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.swse.qp.master.MasterQuery;
import org.semanticweb.swse.qp.utils.QueryProcessor;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Servlet.
 * 
 * @author aidhog
 */
public class Servlet extends HttpServlet  {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    /**
     * GET for asking queries.
     */
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {	
        ServletContext ctx = getServletContext();
        
        //OutputStreamWriter osw = new OutputStreamWriter(response.getOutputStream(), "UTF8");
        PrintWriter out = response.getWriter();

        // check for error during startup
        Exception ioex = (Exception)ctx.getAttribute(Listener.ERROR);
        if (ioex != null) {
        	throw new ServletException(ioex.getMessage());
        }
        
        String keywordq = request.getParameter("kwq");
        String focus = request.getParameter("focus");
        String lang = request.getParameter("lang");
        
        String charenc = request.getCharacterEncoding();
        
        if (charenc == null) {
        	charenc = "ISO-8859-1";
        }
        
        if(lang == null){
        	lang = QueryProcessor.DEFAULT_LANG;
        }
        
        if(keywordq!=null){
        	String f = request.getParameter("from");
            String t = request.getParameter("to");
            int from = 0;
            if(f!=null)
            	from = Integer.parseInt(f);
            int to = Integer.MAX_VALUE;
            if(t!=null)
            	to = Integer.parseInt(t);
            
            
            try {
            	keywordq = new String(keywordq.getBytes(charenc),"UTF8");
            } catch (UnsupportedEncodingException e1) {
            	// TODO Auto-generated catch block
            	e1.printStackTrace();
            }

            try {
	            MasterQuery mq = (MasterQuery)ctx.getAttribute(Listener.QUERYPROCESSOR);
	            Iterator<Node[]> results = mq.keywordQuery(keywordq, from, to, lang);
	            
	            response.setContentType("application/rdf+nq");
				while(results.hasNext()){
					out.println(Nodes.toN3(results.next()));
				}
            } catch (Exception e1) {
            	e1.printStackTrace();
            	throw new ServletException(e1);
            }

        } else if(focus!=null){
        	try {
            	focus = new String(focus.getBytes(charenc),"UTF8");
            } catch (UnsupportedEncodingException e1) {
            	// TODO Auto-generated catch block
            	e1.printStackTrace();
            }

            try {
	            MasterQuery mq = (MasterQuery)ctx.getAttribute(Listener.QUERYPROCESSOR);
	            Node n = NxParser.parseNode(focus);
	            Iterator<Node[]> results = mq.focus(n, lang);
	            
	            response.setContentType("application/rdf+nq");
				while(results.hasNext()){
					out.println(Nodes.toN3(results.next()));
				}
            } catch (Exception e1) {
            	e1.printStackTrace();
            	throw new ServletException(e1);
            }
        } else {
        	throw new ServletException("please specify parameter 'kwq' or 'focus'");
        }
        out.close();
    }
    

    /**
     * POST to register queries
     */
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    	throw new ServletException("post not supported, use get");
    }
    
    /**
     * PUT is for adding quads.
     */
    public void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {   	
    	throw new ServletException("put not supported, use get");
    }
    
    /**
     * DELETE for removing quads.
     */
    public void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {   	
    	throw new ServletException("delete not supported, use get");
    }
}