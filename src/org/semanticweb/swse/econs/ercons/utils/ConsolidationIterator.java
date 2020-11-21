package org.semanticweb.swse.econs.ercons.utils;

import java.util.Iterator;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;

/**
 * Provides an iterator which rewrites statements according to provided Iterator
 * of reasoned owl:sameAs statements.
 * 
 * @author Aidan Hogan
 */
public class ConsolidationIterator implements Iterator<Node[]>{
	public static final Node NO_REWRITE = new Literal("n");


	private Iterator<Node[]> _sai;
	private Iterator<Node[]> _iter;
	private Node _oldin = null;

	private int _comp = 0;

	private int _rewrittenStmts = 0;
	private int _rewrittenIDs = 0;
	private int _filtered = 0;
	private int _count = 0;

	private boolean _track = false;

	private HandleNode _hsa;
	private HandleNode _hrt;
	private Node[] _current = null;

	private int _pos;

	public static enum HandleNode{
		REWRITE, FILTER, BUFFER
	}
	
	private static final int P = 1;

	private Node[] _nextP = null;

	public ConsolidationIterator(Iterator<Node[]> iter, Iterator<Node[]> sai){
		this(iter, sai, HandleNode.REWRITE, HandleNode.REWRITE, 0, false);
	}

	public ConsolidationIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, HandleNode hsa){
		this(iter, sai, hsa, HandleNode.REWRITE, 0, false);
	}

	public ConsolidationIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, HandleNode hsa, int pos){
		this(iter, sai, hsa, HandleNode.REWRITE, 0, false);
	}

	public ConsolidationIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, HandleNode hsa, HandleNode hrt, int pos, boolean track){
		_iter = iter;
		_sai = sai;
		_hsa = hsa;
		_hrt = hrt;
		_pos = pos;
		_track = track;
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
		boolean done = false;
		while(!done){
			_current = null;
			if(!_iter.hasNext())
				return;

			Node[] next = _iter.next();
			Node[] rewritten = new Node[next.length];
			Node suffix = NO_REWRITE;
			
			if(next[P].equals(OWL.SAMEAS)){
				if(_hsa==HandleNode.FILTER){
					_filtered++;
					//had recursion, but was giving a stack overflow
					continue;	
				} else if(_hsa==HandleNode.BUFFER){
					if(_track){
						_current = append(next, NO_REWRITE);
					} else _current = next;
					return;
				}
			} else if(next[P].equals(RDF.TYPE)){
				if(_hrt==HandleNode.FILTER){
					_filtered++;
					//had recursion, but was giving a stack overflow
					continue;	
				} else if(_hrt==HandleNode.BUFFER){
					if(_track){
						_current = append(next, NO_REWRITE);
					} else _current = next;
					return;
				}
			}

			System.arraycopy(next, 0, rewritten, 0, next.length);

			if(_nextP==null){
				if(_track){
					_current = append(next, NO_REWRITE);
				} else _current = next;
				return;
			}

			if(_oldin == null || !_oldin.equals(next[_pos])){
				_oldin = next[_pos];
				_comp = _nextP[0].compareTo(next[_pos]);
				while(_comp<0){
					setNextPivot();
					if(_nextP==null){
						if(_track){
							_current = append(next, NO_REWRITE);
						} else _current = next;
						return;
					}
					_comp = _nextP[0].compareTo(next[_pos]);
				}

				if(_comp==0){
					_rewrittenIDs++;
				}
			}

			

			if(_comp==0){ 
				suffix = rewritten[_pos];
				rewritten[_pos] = _nextP[1];
				_rewrittenStmts++;
			}

			if(_track){
				_current = append(rewritten, suffix);
			} else _current = rewritten;

			done = true;
		}
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
			if(line[0].compareTo(line[2])<=0)
				continue;
			if(old == null || !line[0].equals(old)){
				_nextP = new Node[]{line[0], line[2]};
				return;
			}
		}
	}

	private static Node[] append(Node[] raw, Node suffix){
		Node[] extra = new Node[raw.length+1];
		System.arraycopy(raw, 0, extra, 0, raw.length);
		extra[raw.length] = suffix;
		return extra;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
