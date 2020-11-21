package org.semanticweb.swse.ann.incons.utils;

import org.semanticweb.saorr.index.StatementStore;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.util.ResetableIterator;

public class ClearStatementStoreIterator implements ResetableIterator<Node[]>{
	ResetableIterator<Node[]> _in;
	StatementStore _ss;
	Node _old = null;
	
	public ClearStatementStoreIterator(ResetableIterator<Node[]> in, StatementStore ss){
		_in = in;
		_ss = ss;
	}

	public void reset() {
		_in.reset();
	}

	public boolean hasNext() {
		return _in.hasNext();
	}

	public Node[] next() {
		Node[] next = _in.next();
		if(_old==null)
			_old = next[0];
		
		if(!_old.equals(next[0])){
			_ss.clear();
			_old = next[0];
		}
		return next;
	}

	public void remove() {
		_in.remove();
	}
}
