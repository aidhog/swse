package org.semanticweb.swse.ldspider.remote;


import java.io.IOException;
import java.util.logging.Logger;

import org.semanticweb.swse.ldspider.remote.queue.DiskQueue;
import org.semanticweb.swse.ldspider.remote.utils.ErrorHandlerCounter;
import org.semanticweb.yars.nx.parser.Callback;

import com.ontologycentral.ldspider.CrawlerConstants;
import com.ontologycentral.ldspider.hooks.content.CallbackDummy;
import com.ontologycentral.ldspider.hooks.error.ErrorHandlerDummy;
import com.ontologycentral.ldspider.hooks.fetch.FetchFilter;
import com.ontologycentral.ldspider.hooks.fetch.FetchFilterAllow;
import com.ontologycentral.ldspider.http.ConnectionManager;
import com.ontologycentral.ldspider.http.robot.Robots;
import com.ontologycentral.ldspider.tld.TldManager;

/**
 * Factory for a remote crawler.
 * @author aidhog
 *
 */
public class RemoteCrawlerFactory {
	private static Logger _log = Logger.getLogger(RemoteCrawlerFactory.class.getName());
	
	private Callback _c = new CallbackDummy();
	
	private int _maxplduris = CrawlerConstants.DEFAULT_MAX_PLD_ROUND;
	
	private int _minplduris = CrawlerConstants.DEFAULT_MIN_PLD_ROUND;
	
	private int _threads = CrawlerConstants.DEFAULT_NB_THREADS;
	
	private int _retries = CrawlerConstants.RETRIES;
	
	private FetchFilter _ff = new FetchFilterAllow();
	
	private DiskQueue _q = null;
	
	private ConnectionManager _cm = null;
	
	private TldManager _tldm = null;
	
	private Robots _r = null;
	
	private ErrorHandlerCounter _eh = null;
	
	private boolean _score = false;
	
	public static ConnectionManager getConnectionMananger(){
		return getConnectionManager(CrawlerConstants.RETRIES, CrawlerConstants.DEFAULT_NB_THREADS);
	}
	
	public static ConnectionManager getConnectionMananger(int threads){
		return getConnectionManager(CrawlerConstants.RETRIES, threads);
	}
	
	public static ConnectionManager getConnectionManager(int threads, int retries){
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
		cm.setRetries(retries);
		return cm;
	}
	
	public ConnectionManager getConnectionManager(){
		return getConnectionManager(_threads, _retries);
	}
	
	public static TldManager getTopLevelDomainManager(ConnectionManager cm){
		TldManager tldm = null;
		 try {
			 tldm = new TldManager(cm);
		 } catch (Exception e) {
			 System.err.println("cannot get tld file online " + e.getMessage());
			 try {
				 tldm = new TldManager();
			 } catch (IOException e1) {
				 _log.info("cannot get tld file locally " + e.getMessage());
				 throw new RuntimeException("cannot get tld file locally " + e.getMessage());
			 }
		 }
		 return tldm;
	}
	
	public RemoteCrawlerFactory(){
		;
	}
	
	public void setErrorHandlerCounter(ErrorHandlerCounter eh){
		_eh = eh;
	}
	
	public void setCallback(Callback c){
		_c = c;
	}
	
	public void setScoring(boolean score){
		_score = score;
	}
	
	public void setQueue(DiskQueue q){
		_q = q;
	}

	public void setMaxPldURIs(int maxplduris){
		_maxplduris = maxplduris;
	}
	
	public void setMinPldURIs(int minplduris){
		_minplduris = minplduris;
	}
	
	public void setThreads(int threads){
		_threads = threads;
	}
	
	public void setRetries(int retries){
		_retries = retries;
	}
	
	public void setFetchFilter(FetchFilter ff){
		_ff = ff;
	}
	
	public void setConnectionManager(ConnectionManager cm){
		_cm = cm;
	}
	
	public void setTldManager(TldManager tldm){
		_tldm = tldm;
	}
	
	public void setRobots(Robots r){
		_r = r;
	}
	
	/**
	 * @todo fix queue setting
	 * @return
	 */
	public RemoteCrawler createCrawler(){
		if(_cm==null){
			_cm = getConnectionManager();
		}
		if(_tldm==null){
			_tldm = getTopLevelDomainManager(_cm);
		}
		if(_r==null){
			_r= new Robots(_cm, _eh);
		}
		if(_eh==null){
			_eh = new ErrorHandlerCounter(new ErrorHandlerDummy(), _tldm);
		}
		if(_q==null){
			;
		}
		

		return new RemoteCrawler(_threads, _maxplduris, _minplduris, _cm,
				_eh, _r, _tldm, _q, _ff, _c, _score);
		
	}
}
