package org.semanticweb.swse.cli;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.HttpClientParams;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.params.ConnManagerParams;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import com.ontologycentral.ldspider.CrawlerConstants;
import com.ontologycentral.ldspider.http.internal.HttpRequestRetryHandler;
import com.ontologycentral.ldspider.http.internal.ResponseGzipUncompress;

/**
 * Main method to conduct distributed reasoning using remote reasoners 
 * controllable via RMI.
 * 
 * @author aidhog
 */
public class BenchmarkUI {
	private final static Logger _log = Logger.getLogger(BenchmarkUI.class.getSimpleName());

	private final static Node WARMUP = new Resource("http://sw.deri.org/~aidanh/foaf/foaf.rdf#Aidan_Hogan");

	private final static String KEYWORD_FORM = "list?keyword=";
	private final static String FOCUS_FORM = "detail?focus=";
	
	public static final String USERAGENT = "bench-ui";
	public static final Header[] HEADERS = {
		new BasicHeader("User-Agent", USERAGENT),
		new BasicHeader("Content-Type", "text/html")
		//		new BasicHeader("Connection", "close") //avoid CLOSE_WAIT?
	};

	public static void main(String args[]) throws Exception{
		Options options = new Options();

		Option uiO = new Option("ui", "file with UI base uris");
		uiO.setArgs(1);
		uiO.setRequired(true);
		options.addOption(uiO);

		Option kwO = new Option("kw", "line delimited keyword queries");
		kwO.setArgs(1);
		kwO.setRequired(true);
		options.addOption(kwO);

		Option focusO = new Option("f", "line delimited focus (n3 format node) queries");
		focusO.setArgs(1);
		focusO.setRequired(true);
		options.addOption(focusO);

		Option threadsO = new Option("t", "threads");
		threadsO.setArgs(1);
		threadsO.setRequired(true);
		options.addOption(threadsO);

		Option helpO = new Option("h", "print help");
		options.addOption(helpO);

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = null;

		try {
			cmd = parser.parse(options, args);
		} catch (org.apache.commons.cli.ParseException e) {
			System.err.println("***ERROR: " + e.getClass() + ": " + e.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}

		if (cmd.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("parameters:", options );
			return;
		}

		String kw = cmd.getOptionValue(kwO.getOpt());
		String f = cmd.getOptionValue(focusO.getOpt());
		String ui = cmd.getOptionValue(uiO.getOpt());
		int threads = Integer.parseInt(cmd.getOptionValue(threadsO.getOpt()));

		benchmarkUI(ui, kw, f, threads);
	}

	public static void benchmarkUI(String ui_file, String kw_file, String focus_file, int threads) throws Exception {
		ArrayList<String> uis = getEndpoints(ui_file);
		ArrayList<String> kws = BenchmarkLocalLucene.getKeywords(kw_file, false);
		ArrayList<Node> ns = getNodes(focus_file);
		
		_log.info("Benchmarking using "+uis.size()+" "+kws.size()+" keywords and "+ns.size()+" nodes.");
		_log.info("Using "+threads+" threads");

		_log.info("First, warmup with a quick kw/focus query...");
		ArrayList<URI> warmup = new ArrayList<URI>();
		for(String ui:uis){
			warmup.add(getKeywordQuery(ui, BenchmarkLocalLucene.WARMUP));
			warmup.add(getFocusQuery(ui, WARMUP));
		}
		
		ConnectionManager cm = getConnectionManager(threads);
		
		ConcurrentLinkedQueue<URI> warmupq = new ConcurrentLinkedQueue<URI>(warmup);
		
		LookupThread lt = new LookupThread(cm, warmupq, true);
		lt.start();
		lt.join();
		_log.info("...okay, warmed up...");
		
		
		_log.info("Running eval proper...");
		ArrayList<URI> kwqueries = getKeywordQueries(uis, kws);
		ArrayList<URI> bqueries = getFocusQueries(uis, ns);
		
		ArrayList<URI> mix = new ArrayList<URI>();
		mix.addAll(kwqueries);
		mix.addAll(bqueries);

		Collections.shuffle(mix);

		ConcurrentLinkedQueue<URI> q = new ConcurrentLinkedQueue<URI>(mix);

		

		LookupThread[] lts = new LookupThread[threads];
		for(int i=0; i<threads; i++){
			lts[i] = new LookupThread(cm, q);
		}
		
		long b4 = System.currentTimeMillis();
		for(int i=0; i<threads; i++){
			lts[i].start();
		}
		for(int i=0; i<threads; i++){
			lts[i].join();
		}
		long after = System.currentTimeMillis();

		_log.info("UI eval done in "+(after-b4)+" ms");
	}

	public static ArrayList<URI> getKeywordQueries(ArrayList<String> uis, ArrayList<String> kws) throws Exception{
		ArrayList<URI> kwqueries = new ArrayList<URI>();
		Random r = new Random();
		for(String kw:kws){
			if(!kw.equals(BenchmarkLocalLucene.WARMUP))
				kwqueries.add(getKeywordQuery(uis.get(r.nextInt(uis.size())), kw));
		}
		return kwqueries;
	}
	
	public static URI getKeywordQuery(String ui, String kw) throws UnsupportedEncodingException, URISyntaxException{
		return new URI(ui+KEYWORD_FORM+URLEncoder.encode(kw, "utf-8"));
	}

	public static ArrayList<URI> getFocusQueries(ArrayList<String> uis, ArrayList<Node> ns) throws Exception{
		ArrayList<URI> fqueries = new ArrayList<URI>();
		Random r = new Random();
		for(Node n:ns){
			fqueries.add(getFocusQuery(uis.get(r.nextInt(uis.size())), n));
		}
		return fqueries;
	}
	
	public static URI getFocusQuery(String ui, Node n) throws UnsupportedEncodingException, URISyntaxException{
		String nS = null;
		if(n instanceof Resource) nS = URLEncoder.encode(n.toString(), "utf-8");
		else nS = n.toN3();
		
		URI u = new URI(ui+FOCUS_FORM+nS);
		return u; 
	}

	public static ArrayList<Node> getNodes(String f) throws ParseException, IOException{
		BufferedReader br = new BufferedReader(new FileReader(f));
		ArrayList<Node> ns = new ArrayList<Node>();
		String line = null;
		while((line = br.readLine())!=null){
			line = line.trim();
			if(!line.isEmpty()){
				ns.add(NxParser.parseNode(line));
			}
		}
		return ns;
	}
	
	public static ArrayList<String> getEndpoints(String uis) throws ParseException, IOException{
		BufferedReader br = new BufferedReader(new FileReader(uis));
		ArrayList<String> es = new ArrayList<String>();
		String line = null;
		while((line = br.readLine())!=null){
			line = line.trim();
			if(!line.isEmpty() && !line.startsWith("#")){
				es.add(line);
			}
		}
		return es;
	}

	public static ConnectionManager getConnectionManager(int threads){
		String phost = null;
		int pport = 0;		
		String puser = null;
		String ppassword = null;

		if (System.getProperties().get("http.proxyHost") != null) {
			phost = System.getProperties().get("http.proxyHost").toString();
		}
		if (System.getProperties().get("http.proxyPort") != null) {
			pport = Integer.parseInt(System.getProperties().get("http.proxyPort").toString());
		}

		if (System.getProperties().get("http.proxyUser") != null) {
			puser = System.getProperties().get("http.proxyUser").toString();
		}
		if (System.getProperties().get("http.proxyPassword") != null) {
			ppassword = System.getProperties().get("http.proxyPassword").toString();
		}

		ConnectionManager cm = new ConnectionManager(phost, pport, puser, ppassword, threads*CrawlerConstants.MAX_CONNECTIONS_PER_THREAD);
		cm.setRetries(CrawlerConstants.RETRIES);

		return cm;
	}

	public static class LookupThread extends Thread {
		Logger _log = Logger.getLogger(this.getClass().getSimpleName());

		//		Sitemaps _sitemaps;

		final ConnectionManager _hclient;
		final Queue<URI> _q;
		
		final boolean _warmup;
		
		public LookupThread(ConnectionManager hc, Queue<URI> q, boolean warmup) {
			_hclient = hc;
			_q = q;
			_warmup = warmup;
		}

		public LookupThread(ConnectionManager hc, Queue<URI> q) {
			this(hc, q, false);
		}

		public void run() {
			_log.info("starting thread ...");

			int i = 0;

			URI lu;
			while((lu = _q.poll())!=null){
				_log.fine("looking up " + lu);

				i++;
				long b4 = System.currentTimeMillis();
				int status = 0;

				//				List<URI> li = _sitemaps.getSitemapUris(lu);
				//				if (li != null && li.size() > 0) {
				//					_log.info("sitemap surprisingly actually has uris " + li);
				//				}

				HttpGet hget = new HttpGet(lu);
				hget.setHeaders(HEADERS);
				
				int read = 0, total = 0;

				try {
					HttpResponse hres = _hclient.connect(hget);

					HttpEntity hen = hres.getEntity();

					status = hres.getStatusLine().getStatusCode();

					_log.info("lookup on " + lu + " status " + status);

					if (status == HttpStatus.SC_OK) {				
						if (hen != null) {
							InputStream is = hen.getContent();
							byte[] buf = new byte[1024];
							
							while((read = is.read(buf))!=-1){
								total+=read;
							}
							read = total;
							is.close();
						} else {
							throw new RuntimeException("HttpEntity for " + lu + " is null");
						}
					} else{ 
						hget.abort();
						throw new RuntimeException("Got status "+status);
					}

					hget.abort();
				} catch (Throwable e) {
					hget.abort();
					throw new RuntimeException(e);
				}

				long after = System.currentTimeMillis();
				if(!_warmup)
					_log.info("Result (kw time bytes) :\t"+lu+"\t"+(after-b4)+"\t"+total);
			}
		}
	}

	public static class ConnectionManager {

		private DefaultHttpClient _client;


		public ConnectionManager(String proxyHost, int proxyPort, String puser, String ppassword, int connections) {
			// general setup
			SchemeRegistry supportedSchemes = new SchemeRegistry();

			// Register the "http" and "https" protocol schemes, they are
			// required by the default operator to look up socket factories.
			supportedSchemes.register(new Scheme("http", 
					PlainSocketFactory.getSocketFactory(), 80));
			supportedSchemes.register(new Scheme("https", 
					SSLSocketFactory.getSocketFactory(), 443));

			// prepare parameters
			HttpParams params = new BasicHttpParams();
			HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
			HttpProtocolParams.setContentCharset(params, "UTF-8");
			HttpProtocolParams.setUseExpectContinue(params, true);

			// we deal with redirects ourselves
			HttpClientParams.setRedirecting(params, false);

			//connection params 
			params.setParameter(CoreConnectionPNames.SO_TIMEOUT, Integer.MAX_VALUE);
			params.setParameter(CoreConnectionPNames.TCP_NODELAY, true);
			params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, Integer.MAX_VALUE);

			ConnManagerParams.setMaxTotalConnections(params, connections);

			ClientConnectionManager cm = new ThreadSafeClientConnManager(params, supportedSchemes);

			_client = new DefaultHttpClient(cm, params);
			_client.addResponseInterceptor(new ResponseGzipUncompress());

			// check if we have a proxy
			if (proxyHost != null) {
				HttpHost proxy = new HttpHost(proxyHost, proxyPort, "http");
				_client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
			}	
		}

		public void setRetries(int no) {
			//set the retry handler
			if (no > 0) {
				HttpRequestRetryHandler retryHandler = new HttpRequestRetryHandler(no);
				_client.setHttpRequestRetryHandler(retryHandler);
			}
		}

		public void shutdown() {
			_client.getConnectionManager().shutdown();

		}

		public void closeIdleConnections(long ms) {
			_client.getConnectionManager().closeIdleConnections(ms, TimeUnit.MILLISECONDS);

		}

		public HttpResponse connect(HttpGet get) throws ClientProtocolException, IOException {
			return _client.execute(get);
		}
	}
}

