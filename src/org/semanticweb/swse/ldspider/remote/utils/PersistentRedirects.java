package org.semanticweb.swse.ldspider.remote.utils;

import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;

import com.ontologycentral.ldspider.queue.memory.Redirects;

public class PersistentRedirects extends Redirects{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7489517852694558442L;
	transient PrintStream _ps;
	
	private Redirects _round;
	
	static Logger _log = Logger.getLogger(PersistentRedirects.class.getName());
	
	public PersistentRedirects(PrintStream ps) {
		super();
		_ps = ps;
		_round = new Redirects();
	}
	
	public boolean put(URI from, URI to) {
		boolean unique = super.put(from, to);
		
		if(unique){
			if (from.getFragment() != null) {
				try {
					from = removeFragment(from);
				} catch (URISyntaxException e) {
					_log.info(e.getMessage() + " " + from);
				}			
			}
			
			_round.put(from, to);
		
			_ps.println(new Nodes(new Resource(from.toASCIIString()), new Resource(to.toASCIIString())).toN3());
		}
		return unique;
	}
	
	public void close(){
		_ps.close();
	}
	
	public void clearRound(){
		_round = new Redirects();
	}
	
	public Redirects getRoundRedirects(){
		return _round;
	}
}
