package org.semanticweb.swse.ldspider.remote;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.semanticweb.swse.ldspider.remote.queue.DiskQueue;
import org.semanticweb.swse.ldspider.remote.utils.ErrorHandlerCounter;
import org.semanticweb.swse.ldspider.remote.utils.LinkFilter;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.util.Callbacks;

import com.ontologycentral.ldspider.hooks.fetch.FetchFilter;
import com.ontologycentral.ldspider.hooks.fetch.FetchFilterRdfXml;
import com.ontologycentral.ldspider.http.ConnectionManager;
import com.ontologycentral.ldspider.http.LookupThread;
import com.ontologycentral.ldspider.http.robot.Robots;
import com.ontologycentral.ldspider.queue.memory.Redirects;
import com.ontologycentral.ldspider.tld.TldManager;

/**
 * Crawler desgined to be run remotely over RMI, with co-ordination
 * between rounds.
 * 
 * @author aidhog
 */
public class RemoteCrawler {
	Logger _log = Logger.getLogger(this.getClass().getName());

	Callback _output;
	LinkFilter _lf;
	ErrorHandlerCounter _eh;
	FetchFilter _ff;
	ConnectionManager _cm;
	
	Map<String,Integer> _leftover = null;
	
	PldManager _pldm = null;
	
	PldManager _pldmRound = null;
	
	DiskQueue _q;
	
	Redirects _r;
	
	Robots _robots;
	TldManager _tldm;
	
	List<Thread> _ts = null;
	
	//UriSrc _urisrc;
	
	int _threads;
	
	int _maxuris;
	
	int _minuris;
	
	int _currentRound = 0;
	
	boolean _score;
	
	/**
	 * Constructor for RemoteCrawlerFactory
	 * @param maxuris
	 * @param threads
	 * @param q
	 */
	RemoteCrawler(int threads, int maxuris, int minuris, ConnectionManager cm,
			ErrorHandlerCounter eh, Robots r, TldManager tldm, DiskQueue q,
			 FetchFilter ff, Callback output, boolean score) {
		_threads = threads;
		_maxuris = maxuris;
		_minuris = minuris;
		
		_cm = cm;
		_eh = eh;
		_robots = r;
		_tldm = tldm;
		_q = q;
		_ff = ff;
		_output = output;
		
		_pldm = new PldManager();
		_score = score;
		
		_r = q.getKnownRedirects();
	}
	
	public void addAllFrontier(Collection<String> uris) throws FileNotFoundException, IOException{
		for(String uri:uris){
			URI u;
			try{
				u = new URI(uri);
			}catch(URISyntaxException e){
				_log.info("cannot add to frontier -- invalid uri "+uri);
				continue;
			}
			
			_q.addFrontier(u);
		}
		
	}
	
	public void addAllFrontier(Map<String, Integer> uris){
		if(_leftover == null){
			_leftover = new HashMap<String,Integer>();
		}
		
		for(Map.Entry<String,Integer> uri:uris.entrySet()){
			//forward counts for redirects
			try{
				URI u = new URI(uri.getKey());
				URI r = _r.getRedirect(u);
				if(r.equals(u)){
					_q.addFrontier(new URI(uri.getKey()), uri.getValue());
				} else{
					Integer i = _leftover.get(r.toString());
					if(i==null){
						_leftover.put(r.toString(), uri.getValue());
					} else{
						_leftover.put(r.toString(), i+uri.getValue());
					}
				}
			}catch(Exception e){
				_log.info("cannot add to frontier -- invalid uri "+uri);
			}
		}
	}
	
	public int runRound(int targeturis) throws IOException, ParseException{
		_currentRound++;
		_ts = new ArrayList<Thread>();
		List<LookupThread> lts = new ArrayList<LookupThread>();
		
		_pldm.newRound();
		_q.setPldManager(_pldm);
		
		PldManager pldmRound = new PldManager();
		
		_lf = new LinkFilter(_eh);
		if(_leftover!=null){
			for(Map.Entry<String, Integer> l:_leftover.entrySet()){
				_lf.addLink(l.getKey(), l.getValue());
			}
		}
		_leftover = null;
		
		Callbacks cbs = new Callbacks(new Callback[] { _output, _lf } );
		FetchFilterRdfXml ff = new FetchFilterRdfXml(_eh);
		_eh.setPldManager(pldmRound);
		_q.setPldManagerForRound(pldmRound);
		_q.setLinkFilter(_lf);
		
		_q.schedule(_maxuris, _minuris, targeturis);
		
		for (int j = 0; j < _threads; j++) {
			LookupThread lt = new LookupThread(_cm, _q, cbs, _robots, _eh, ff);
			lts.add(lt);
			_ts.add(new Thread(lt,"LookupThread-"+j));
		}

		_log.info("Starting threads round " + _currentRound + " with " + _q.size() + " uris");
//		_log.info(_q.toString());
		
		for (Thread t : _ts) {
			t.start();
		}

		for (Thread t : _ts) {
			try {
				t.join();
			} catch (InterruptedException e1) {
				_log.warning(e1.getMessage());
			}
		}
		
		long maxtime = 0;
		for (LookupThread lt: lts){
			long time = lt.getTotalTime();
			if(time>maxtime)
				maxtime = time;
		}
		
		int i = 0;
		for (LookupThread lt: lts){
			_log.info("Thread "+i+" idle at end of round for "+(maxtime-lt.getTotalTime())+" ms");
			i++;
		}
		
		_cm.closeIdleConnections(10*1000);
		_pldmRound = pldmRound;
//		_pldm.logStats();
		
		return _q.crawled();
	}
	
	public int endRound(boolean join){
		_q.setFinished();
		if (join) for (Thread t : _ts) {
			try {
				t.join();
			} catch (InterruptedException e1) {
				_log.warning(e1.getMessage());
			}
		}
		return _q.crawled();
	}
	
	public PldManager getNewPldsStats(){
		return _pldmRound;
	}
	
	public void addRemoteStats(PldManager pldm){
		if(_pldm==null){
			_pldm = pldm;
		} else{
			_pldm.addAll(pldm);
		}
	}
	
	public Map<String, Integer> getNewURIs(){
		_lf.considerRedirects(_q.getKnownRedirects());
		return _lf.getLinks();
	}
	
	public void close() throws IOException, ParseException {
		_cm.shutdown();
		_q.close();
		_output.endDocument();
		_pldm.logStats();
		_eh.close();
	}
	
	public long getLookups(){
		return _eh.lookups();
	}
}
