package org.semanticweb.swse.cons2.utils;


import java.util.Iterator;

import org.semanticweb.saorr.auth.AuthorityInspector;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RDFS;

/**
 * Pre-process authority for reasoning
 * 
 * @author Aidan Hogan
 * @date 2010-03-25
 */
public class PreprocessAuthority implements Iterator<Node[]>{
	private Node[] _current;
	private Node[] _current2 = null;
	private Iterator<Node[]> _in;
	private AuthorityInspector _ai;
	
	public PreprocessAuthority(Iterator<Node[]> in, AuthorityInspector ai){
		_in = in;
		_ai = ai;
		getNext();
	}
	
	private void getNext(){
		_current = null;
		
		if(_current2!=null){
			_current = _current2;
			_current2 = null;
			
			return;
		}
		
		while(_in.hasNext() && _current==null){
			Node[] next = _in.next();
			
			if(next[1].equals(RDF.TYPE)){
				if(_ai.checkAuthority(next[0], next[3])){
					_current = next;
				} 
			} else if(next[1].equals(RDFS.SUBPROPERTYOF)){
				if(_ai.checkAuthority(next[0], next[3])){
					_current = next;
				}
			} else if(next[1].equals(OWL.INVERSEOF)){
				if(_ai.checkAuthority(next[0], next[3])){
					_current = next;
				} 
				if(_ai.checkAuthority(next[2], next[3])){
					if(_current!=null)
						_current2 = new Node[]{next[2], next[1], next[0], new Resource(Rule.CONTEXT_PREFIX+"cons-inv-sym")};
					_current = new Node[]{next[2], next[1], next[0], new Resource(Rule.CONTEXT_PREFIX+"cons-inv-sym")};
				}
			} else if(next[1].equals(OWL.EQUIVALENTPROPERTY)){
				if(_ai.checkAuthority(next[0], next[3])){
					_current = new Node[]{next[0], RDFS.SUBPROPERTYOF, next[2], new Resource(Rule.CONTEXT_PREFIX+"cons-ep-sp0")};
				} 
				if(_ai.checkAuthority(next[2], next[3])){
					if(_current!=null)
						_current2 = new Node[]{next[2], RDFS.SUBPROPERTYOF, next[0], new Resource(Rule.CONTEXT_PREFIX+"cons-ep-sp1")};
					_current = new Node[]{next[2], RDFS.SUBPROPERTYOF, next[0], new Resource(Rule.CONTEXT_PREFIX+"cons-ep-sp1")};
				}
			}
		}
	}
	
	public boolean hasNext() {
		return _current!=null;
	}

	public Node[] next() {
		Node[] next = _current;
		getNext();
		return next;
	}

	public void remove() {
		_in.remove();
	}
}
