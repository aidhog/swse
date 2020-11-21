package org.semanticweb.swse.ldspider;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.semanticweb.swse.ldspider.remote.RemoteCrawlerFactory;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.util.Callbacks;
import org.semanticweb.yars2.rdfxml.RDFXMLParser;

import com.ontologycentral.ldspider.CrawlerConstants;
import com.ontologycentral.ldspider.hooks.content.CallbackDummy;
import com.ontologycentral.ldspider.http.ConnectionManager;
import com.ontologycentral.ldspider.http.Headers;

public class TimeOutTest {
//	public static String TIMEOUT_URL_1 = "http://johnbreslin.com/wiki/images/8/8e/20071227a.mp3";
	public static String TIMEOUT_URL = "http://www2.census.gov/acs2005_2007_3yr/pums/csv_pus.zip";
	
	static ConnectionManager _hclient = RemoteCrawlerFactory.getConnectionManager(32, 2);
	
	public static void main(String args[]) throws URISyntaxException{
		long time2 = System.currentTimeMillis();

		URI lu = new URI(TIMEOUT_URL);
		HttpGet hget = new HttpGet(new URI(TIMEOUT_URL));
		hget.setHeaders(CrawlerConstants.HEADERS);

		String type = null;
		try {
			HttpResponse hres = _hclient.connect(hget);

			HttpEntity hen = hres.getEntity();

			int status = hres.getStatusLine().getStatusCode();

			Header ct = hres.getFirstHeader("Content-Type");
			if (ct != null) {
				type = hres.getFirstHeader("Content-Type").getValue();
			}
			
			System.err.println("lookup on " + lu + " status " + status);

			// write headers in RDF
			Headers.processHeaders(lu, status, hres.getAllHeaders(), new Callbacks(new Callback[]{new CallbackDummy()}));

			if (status == HttpStatus.SC_OK) {				
				if (hen != null) {
					if (type.trim().toLowerCase().startsWith("application/rdf+xml")) {
						InputStream is = hen.getContent();

						new RDFXMLParser(is, true, true, lu.toString(),  new Callbacks(new Callback[]{new CallbackDummy()}), new Resource(lu.toString()));
						
						is.close();
					} else {
						System.err.println("not allowed " + lu);
					}
				} else {
					System.err.println("HttpEntity for " + lu + " is null");
				}
			} else if (status == HttpStatus.SC_MOVED_PERMANENTLY || status == HttpStatus.SC_MOVED_TEMPORARILY || status == HttpStatus.SC_SEE_OTHER) { 
				// treating all redirects the same but shouldn't: 301 -> rename context URI, 302 -> keep original context URI, 303 -> spec inconclusive
				Header[] loc = hres.getHeaders("location");
				System.err.println("redirecting (" + status + ") to " + loc[0].getValue());
				URI to = new URI(loc[0].getValue());

				// set redirect from original uri to new uri -> break loops by taking only one redirect into account
			}

			//not to be used : http://marc.info/?l=httpclient-users&m=120845193213798&w=2
//			if (hen != null) {
//				bytes = hen.getContentLength();
//				hen.consumeContent();
//			} else {
//				hget.abort();
//			}
			
			if(hen!=null){
				long bytes = hen.getContentLength();
			}
			hget.abort();
		} catch (Exception e) {
			hget.abort();
			System.err.println(e.getMessage());
		}
		
		long time3 = System.currentTimeMillis();
		
		
		System.err.println(lu + " "+(time3-time2) + " ms for lookup");
	}
	
}
