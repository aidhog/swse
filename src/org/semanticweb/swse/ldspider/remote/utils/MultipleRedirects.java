package org.semanticweb.swse.ldspider.remote.utils;

import java.io.PrintStream;
import java.net.URI;

import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;

import com.ontologycentral.ldspider.queue.memory.Redirects;

public class MultipleRedirects extends Redirects{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7489517852694558442L;
	transient PrintStream _ps;
	
	public MultipleRedirects(PrintStream ps) {
		super();
		_ps = ps;
	}
	
	public boolean put(URI from, URI to) {
		boolean unique = super.put(from, to);
		if(unique){
			_ps.println(new Nodes(new Resource(from.toASCIIString()), new Resource(to.toASCIIString())).toN3());
		}
		return unique;
	}
	
	public void close(){
		_ps.close();
	}
}
