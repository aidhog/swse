package org.semanticweb.swse.ldspider.remote;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ldspider.remote.queue.DiskQueue;
import org.semanticweb.swse.ldspider.remote.utils.ErrorHandlerCounter;
import org.semanticweb.swse.ldspider.remote.utils.PersistentRedirects;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.ontologycentral.ldspider.CrawlerConstants;
import com.ontologycentral.ldspider.hooks.error.ErrorHandler;
import com.ontologycentral.ldspider.hooks.error.ErrorHandlerLogger;
import com.ontologycentral.ldspider.hooks.fetch.FetchFilterRdfXml;
import com.ontologycentral.ldspider.http.LookupThread;
import com.ontologycentral.ldspider.tld.TldManager;

/**
 * Serialisable setup for parameters to build a remote crawler.
 * 
 * @author aidhog
 */
public class RemoteCrawlerSetup implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6318965800189451392L;
	
	public static final String QUEUE_DIR = "q/";
	public static final String ACCESS_FILE = "access.log";
	public static final String REDIRECTS_FILE = "redirects.nx";
	public static final String DATA_FILE = "data.nq";
	public static final String GZ_SUFFIX = ".gz";
	
	private static final int NO_VALUE = -1;
	
	private int _threads = NO_VALUE;
	private int _retries = NO_VALUE;
	private boolean _headers  = false;
	
	private String _outdir = null;
	
//	private String _qdir = null;
	
//	private String _log = null;
	private boolean _gzlog = false;
	
//	private String _out = null;
	private boolean _gzout = false;
	
	private boolean _gzred = false;
	
	private long _mindelay = CrawlerConstants.DEFAULT_MIN_DELAY;
	
	private int _maxplduris = CrawlerConstants.DEFAULT_MAX_PLD_ROUND;
	private int _minplduris = CrawlerConstants.DEFAULT_MIN_PLD_ROUND;
	
	private boolean _score = CrawlerConstants.DEFAULT_SCORE_STRUCTURED;
	
	private ArrayList<OutputStream> _streams = new ArrayList<OutputStream>();
	
	//set after crawler construction
	private PersistentRedirects _pr = null;
	
	public RemoteCrawlerSetup(String outdir){
		_outdir = outdir;
	}
	
	public PersistentRedirects getRedirects(){
		return _pr;
	}
	
	public void setGzLog(boolean gz){
		_gzlog = gz;
	}
	
	public void setGzData(boolean gz){
		_gzout = gz;
	}
	
	public void setGzRedirects(boolean gz){
		_gzred = gz;
	}
	
	public void setThreads(int threads){
		_threads = threads;
	}
	
	public void setRetries(int retries){
		_retries = retries;
	}
	
	public void setExtractHeaders(boolean headers){
		_headers = headers;
	}
	
//	public void setQueueDirectory(String qdir){
//		_qdir = qdir;
//	}
	
//	public void setLog(String log, boolean gzip){
//		_log = log;
//		_gzlog = gzip;
//	}
	
//	public void setOutput(String out, boolean gzip){
//		_out = out;
//		_gzout = gzip;
//	}
	
	public void setMinDelay(long mindelay){
		_mindelay = mindelay;
	}
	
	public void setMaxPldURIs(int maxplduris){
		_maxplduris = maxplduris;
	}
	
	public void setMinPldURIs(int minplduris){
		_minplduris = minplduris;
	}
	
	public void setScore(boolean score){
		_score = score;
	}
	
	/**
	 * Ensures cleanup of streams created for crawler
	 * @throws IOException 
	 */
	public void close() throws IOException{
		for(OutputStream os:_streams){
			if(os!=null)
				os.close();
		}
	}
	
	public RemoteCrawler createCrawler() throws URISyntaxException, IOException{
		LookupThread._convertHeaders = _headers;
		
		File f = new File(_outdir);
		f.mkdirs();
		
		RemoteCrawlerFactory rcf = new RemoteCrawlerFactory();
		
		OutputStream log = null;
		if(_gzlog){
			log = new FileOutputStream(_outdir+"/"+ACCESS_FILE+GZ_SUFFIX);
			log = new GZIPOutputStream(log);
		} else{
			log = new FileOutputStream(_outdir+"/"+ACCESS_FILE);
		}
		ErrorHandler eh = new ErrorHandlerLogger(new PrintStream(log));
		_streams.add(log);
		
		String qdir = _outdir+"/"+QUEUE_DIR;
		
		RMIUtils.mkdirs(qdir);
		
		OutputStream out = null;
		if (_gzout) {
			out = new FileOutputStream(_outdir+"/"+DATA_FILE+GZ_SUFFIX);
			out = new GZIPOutputStream(out);
		} else{
			out = new FileOutputStream(_outdir+"/"+DATA_FILE);
		}
		rcf.setCallback(new CallbackNxOutputStream(out));
		_streams.add(out);
		
		OutputStream redir = null;
		if (_gzred) {
			redir = new FileOutputStream(_outdir+"/"+REDIRECTS_FILE+GZ_SUFFIX);
			redir = new GZIPOutputStream(redir);
		} else{
			redir = new FileOutputStream(_outdir+"/"+REDIRECTS_FILE);
		}
		_streams.add(redir);
		
		_pr = new PersistentRedirects(new PrintStream(redir));
		
		TldManager tldm = RemoteCrawlerFactory.getTopLevelDomainManager(
				rcf.getConnectionManager()
		 );
		
		ErrorHandlerCounter ehc = new ErrorHandlerCounter(eh, tldm);
		rcf.setErrorHandlerCounter(ehc);
		
		if(_threads!=NO_VALUE)
			rcf.setThreads(_threads);
		
		if(_retries!=NO_VALUE)
			rcf.setRetries(_retries);
		
		rcf.setMaxPldURIs(_maxplduris);
		rcf.setMinPldURIs(_minplduris);
		
		rcf.setScoring(_score);
		
		
		
//		rcf.setLinkFilter(new LinkFilter(eh));
		rcf.setFetchFilter(new FetchFilterRdfXml(ehc));
		
//		if(_qdir!=null){
//			ConnectionManager cm = rcf.getConnectionManager(_retries, _threads);
//			TldManager tld = 
			rcf.setQueue(new DiskQueue(tldm, _pr, _mindelay, _score, qdir, ehc));
		
		return rcf.createCrawler();
	}
}
