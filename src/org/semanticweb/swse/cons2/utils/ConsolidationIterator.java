package org.semanticweb.swse.cons2.utils;

import java.util.Iterator;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.namespace.OWL;

/**
 * Provides an iterator which rewrites statements according to provided Iterator
 * of reasoned owl:sameAs statements.
 * 
 * @author Aidan Hogan
 */
public class ConsolidationIterator implements Iterator<Node[]>{
	
	private Iterator<Node[]> _sai;
	private Iterator<Node[]> _iter;
	private Node _oldin = null;

	private int _comp = 0;
	
	private int _rewrittenStmts = 0;
	private int _rewrittenIDs = 0;
	private int _filtered = 0;
	private int _count = 0;
	
	private HandleSameAs _hsa;
	private Node[] _current = null;
	
	private int _pos;
	
	public static enum HandleSameAs{
		REWRITE, FILTER, BUFFER
	}
	
	private static final int P = 1;
	
	private Node[] _nextP = null;

	public ConsolidationIterator(Iterator<Node[]> iter, Iterator<Node[]> sai){
		this(iter, sai, HandleSameAs.REWRITE, 0);
	}
	
	public ConsolidationIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, HandleSameAs hsa){
		this(iter, sai, hsa, 0);
	}
	
	public ConsolidationIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, HandleSameAs hsa, int pos){
		_iter = iter;
		_sai = sai;
		_hsa = hsa;
		_pos = pos;
		setNextPivot();
		setNext();
	}
	
	public boolean hasNext() {
		return _current != null;
	}
	
	public int rewrittenStmts(){
		return _rewrittenStmts;
	}
	
	public int rewrittenIDs(){
		return _rewrittenIDs;
	}
	
	public int count(){
		return _count;
	}
	
	public int filtered(){
		return _filtered;
	}

	public Node[] next(){
		Node[] next = _current;
		setNext();
		_count++;
		return next;
	}
	
	private void setNext(){
		_current = null;
		if(!_iter.hasNext())
			return;
		
		Node[] next = _iter.next();
		Node[] rewritten = new Node[next.length];

		System.arraycopy(next, 0, rewritten, 0, next.length);
		
		if(_nextP==null){
			_current = next;
			return;
		}
		
		if(_oldin == null || !_oldin.equals(next[_pos])){
			_oldin = next[_pos];
			_comp = _nextP[0].compareTo(next[_pos]);
			while(_comp<0){
				setNextPivot();
				if(_nextP==null){
					_current = next;
					return;
				}
				_comp = _nextP[0].compareTo(next[_pos]);
			}
			
			if(_comp==0){
				_rewrittenIDs++;
			}
		}
		
		if(_comp==0){ 
			if(next[P].equals(OWL.SAMEAS)){
				if(_hsa==HandleSameAs.BUFFER){
					_current = next;
					return;
				} else if(_hsa==HandleSameAs.FILTER){
					_filtered++;
					setNext();
				}
			}
				
			rewritten[_pos] = _nextP[1];
			_rewrittenStmts++;
		}
		
		_current = rewritten;
	}
	
	private void setNextPivot(){
		Node[] line = null;
		Node old = null;
		if(_nextP!=null){
			old = _nextP[0];
		}
		
		_nextP = null;
		while(_sai.hasNext()){
			line = _sai.next();
//			if(line[0].compareTo(line[2])<=0)
//				continue;
			if(old == null || !line[0].equals(old)){
				_nextP = new Node[]{line[0], line[2]};
				return;
			}
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
