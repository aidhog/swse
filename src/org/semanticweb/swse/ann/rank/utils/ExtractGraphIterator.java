package org.semanticweb.swse.ann.rank.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import org.deri.idrank.namingauthority.Redirects;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.RDF;

public class ExtractGraphIterator implements Iterator<Node[]>{
	TreeSet<Node[]> _currentLinks = new TreeSet<Node[]>(NodeComparator.NC);
	Iterator<Node[]> _currentIter = null;
	Node[] _last = null;
	final Iterator<Node[]> _in;
	final boolean _tbox;
	
	public ExtractGraphIterator(Iterator<Node[]> in, boolean tbox){
		_in = in;
		_tbox = tbox;
		
		if(_in.hasNext()){
			_last = in.next();
			loadNext();
		}
	}
	
	private void loadNext(){
		_currentLinks = new TreeSet<Node[]>(NodeComparator.NC);
		if(!_in.hasNext()){
			if(_last == null){
				_currentLinks = null;
				_currentIter = null;
			} else{
				extractLinks(_last);
				_last = null;
				if(_currentLinks.isEmpty()){
					_currentLinks = null;
				} else{
					_currentIter  = _currentLinks.iterator();
				}
			}
		} else{
			boolean sameCon = true;
			while(_in.hasNext()){
				extractLinks(_last);
				Node[] next = _in.next();
				
				if(!next[3].equals(_last[3]) && !_currentLinks.isEmpty()){
					_last = next;
					sameCon = false;
					break;
				}
				 
				_last = next;
			}
	
			if(sameCon && !_in.hasNext()){
				extractLinks(_last);
				_last = null;
			}
			if(!_currentLinks.isEmpty())
				_currentIter = _currentLinks.iterator();
			else{
				_currentIter = null;
				_currentLinks = null;
			}
		}
	}
	
	private void extractLinks(Node[] stmt){
		boolean rdfType = false;
		for(int i=0; i<3; i++){
			if(stmt[i] instanceof Resource){
				if(i==1 && !_tbox && stmt[i].equals(RDF.TYPE)){
					rdfType = true;
				}
				if(_tbox || i==0 || (i==2 &&!rdfType)){
					Node normalised = normalise((Resource)stmt[i]);
					if(!normalised.equals(stmt[3]))
						_currentLinks.add(new Node[]{stmt[3], normalised});
				}
			}
		}
	}
	
	private static Resource normalise(Resource in){
		return new Resource(Redirects.removeFragment(in.toString()));
	}

	public boolean hasNext() {
		return _currentIter!=null;
	}

	public Node[] next() {
		if(!hasNext())
			throw new NoSuchElementException();
		Node[] next = _currentIter.next();
		if(!_currentIter.hasNext()){
			loadNext();
		}
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
