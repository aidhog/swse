//(c) 2004 Andreas Harth

package org.semanticweb.swse.qp.webapp;

import java.io.File;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.qp.RMIQueryConstants;
import org.semanticweb.swse.qp.master.MasterQuery;
import org.semanticweb.yars.nx.parser.NxParser;



/**
 * This is the main routine, the entry and exit point of the web
 * application.
 *
 * The web application's contextInitialized() and contextDestroyed() are
 * called when Tomcat/the servlet container starts or stops the web
 * application.
 */
public class Listener implements ServletContextListener {
	public final static String QUERYPROCESSOR = "mq";
	public final static String ERROR = "error";

	/**
	 * Servlet context is created.
	 */
	public void contextInitialized(ServletContextEvent event) {
		NxParser.DEFAULT_PARSE_DTS = false;
		ServletContext ctx = event.getServletContext();

		String servers = getFullPath(ctx, ctx.getInitParameter("servers"));
		String spoc = getFullPath(ctx, ctx.getInitParameter("spoc"));
		String sparse = getFullPath(ctx, ctx.getInitParameter("sparse"));
		String lucene = getFullPath(ctx, ctx.getInitParameter("lucene"));

		try{
			File f = new File(servers);
			RMIRegistries rs = new RMIRegistries(f, RMIQueryConstants.DEFAULT_RMI_PORT);
			MasterQuery mq = new MasterQuery(rs, lucene, spoc, sparse);
			ctx.setAttribute(QUERYPROCESSOR, mq);
		} catch (Exception e) {
			System.err.println("Error connecting to servers");
			e.printStackTrace();
			ctx.setAttribute(ERROR, e);
		}
	}

	private String getFullPath(ServletContext ctx, String rel){
		if (rel.startsWith(".")) {
			String realpath = ctx.getRealPath(rel);

			if (realpath != null) {
				return realpath;
			} else return rel;
		}
		return rel;
	}


	/**
	 * Servlet context is about to be shut down.
	 */
	public void contextDestroyed(ServletContextEvent event) {
		;
	}
}
