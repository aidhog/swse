package org.semanticweb.swse.ldspider.remote.utils;

import java.net.URI;
import java.util.logging.Logger;

import org.apache.http.HttpStatus;

import com.ontologycentral.ldspider.hooks.error.ErrorHandler;
import com.ontologycentral.ldspider.tld.TldManager;

public class ErrorHandlerCounter implements ErrorHandler {

	private static Logger _log = Logger.getLogger(ErrorHandlerCounter.class.getName());
	
	int _lookups = 0;
	int _robots = 0;
	int _rdf = 0;
	
	PldManager _pldm;
	TldManager _tldm;
	
	ErrorHandler _eh;
	
	public ErrorHandlerCounter(ErrorHandler eh, TldManager tldm) {
		_tldm = tldm;
		_eh = eh;
	}
	
	public void handleStatus(URI u, int status, String type, long duration, long contentLength) {
		if (type != null && type.indexOf(';') > 0) {
			type = type.substring(0, type.indexOf(';')).toLowerCase().trim();
		}
		
		if(u!=null && u.toString().endsWith("robots.txt")){
			_robots++;
		} else{
			_lookups++;
		
			if(type!=null && type.equals("application/rdf+xml") && status==HttpStatus.SC_OK){
				_rdf++;
				String pld = _tldm.getPLD(u);
				if(pld!=null)
					_pldm.incrementUseful(pld);
			} else if (status == HttpStatus.SC_MOVED_PERMANENTLY || status == HttpStatus.SC_MOVED_TEMPORARILY || status == HttpStatus.SC_SEE_OTHER) {
				;
			} else{
				String pld = _tldm.getPLD(u);
				if(pld!=null)
					_pldm.incrementUseless(pld);
			}
		}
		
		_eh.handleStatus(u, status, type, duration, contentLength);
	}
	
	public void setPldManager(PldManager pldm){
		_pldm = pldm;
	}
	
	public int getLookups(){
		return _lookups;
	}
	
	public int getRobots(){
		return _robots;
	}
	
	public int getRDF(){
		return _rdf;
	}
	
	public void close(){
		_log.info("Lookups: "+_lookups+" RDF: "+_rdf+" Robots: "+_robots);
		_eh.close();
	}

	public void handleError(URI u, Throwable e) {
		_eh.handleError(u, e);
	}

	public long lookups() {
		return _lookups;
	}
}
