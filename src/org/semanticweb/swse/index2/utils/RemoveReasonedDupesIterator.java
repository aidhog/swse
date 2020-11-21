package org.semanticweb.swse.index2.utils;

import java.util.ArrayList;
import java.util.Iterator;

import org.semanticweb.saorr.rules.DefaultRule;
import org.semanticweb.yars.nx.Node;

/**
 * Removes duplicate reasoned triples from the iterator
 * @author aidhog
 *
 */
public class RemoveReasonedDupesIterator implements Iterator<Node[]>{
	Iterator<Node[]> _in;
	Iterator<Node[]> _current;
	
	long _inC;
	long _outC;
	
	private static final String N3_CONTEXT_PREFIX = "<"+DefaultRule.CONTEXT_PREFIX;
	
	Node[] _next = null;
	
	public RemoveReasonedDupesIterator(Iterator<Node[]> in){
		_in = in;
		loadNext();
	}
	
	private void loadNext(){
		if(_next==null){
			if(_in.hasNext()){
				_next = _in.next();
				_inC++;
			}
			else return;
		}
			
		Node[] old;
		
		Node[] reasoned = null;
		ArrayList<Node[]> asserted = new ArrayList<Node[]>();
		
		if(_next[3].toN3().startsWith(N3_CONTEXT_PREFIX))
			reasoned = _next;
		else
			asserted.add(_next);
		
		if(_in.hasNext()) while(_in.hasNext()){
			old = _next;
			_next = _in.next();
			_inC++;
			
			if(!tripleEquals(_next,old)){
				break;
			}
			
			if(_next[3].toN3().startsWith(N3_CONTEXT_PREFIX)){
				if(reasoned==null)
					reasoned = _next;
			} else{
				asserted.add(_next);
			}

//				else
//					System.err.println("Removing dupe "+Nodes.toN3(_next)+" "+Nodes.toN3(reasoned));
			
		} else{
			_next = null;
		}
		
		if(asserted.isEmpty())
			asserted.add(reasoned);
		_current = asserted.iterator();
	}
	
	/**
	 * More efficient in reverse for a sorted set
	 */
	private static boolean tripleEquals(Node[] a, Node[] b){
		return b[2].equals(a[2]) && b[1].equals(a[1]) && b[0].equals(a[0]);
	}
	
	public boolean hasNext() {
		return _current!=null && _current.hasNext();
	}

	public Node[] next() {
		Node[] next = _current.next();
		if(!_current.hasNext())
			loadNext();
		_outC++;
		return next;
	}
	
	public long duplicatesRemoved(){
		return _inC-_outC;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
