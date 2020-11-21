package org.semanticweb.swse.econs.incon.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.semanticweb.yars.nx.Node;

public class PeekAheadIterator implements Iterator<Node[]> {
	Node[] _next = null;
	Iterator<Node[]> _iter = null;
	
	public PeekAheadIterator(Iterator<Node[]> iter){
		_iter = iter;
		loadNext();
	}

	private void loadNext() {
		_next = null;
		if(_iter.hasNext()){
			_next = _iter.next();
		}
	}

	public boolean hasNext() {
		return _next!=null;
	}

	public Node[] next() {
		if(!hasNext()) throw new NoSuchElementException();
		
		Node[] next = _next;
		loadNext();
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	public Node[] peek(){
		return _next;
	}
}
