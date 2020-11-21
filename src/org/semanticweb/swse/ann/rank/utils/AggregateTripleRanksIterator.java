package org.semanticweb.swse.ann.rank.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;

public class AggregateTripleRanksIterator implements Iterator<Node[]>{
	static Logger _log = Logger.getLogger(AggregateTripleRanksIterator.class.getName()); 

	final Iterator<Node[]> _iter;

	TreeSet<Node[]> _currentData = null;
	Iterator<Node[]> _currentIter = null;
	Node[] _next = null;
	Node[] _nextTriple = null;

	double _nextRank = 0;

	public AggregateTripleRanksIterator(Iterator<Node[]> in){
		_iter = in;
		if(_iter.hasNext()){
			Node[] first = _iter.next();
			_nextTriple = triple(first);
			_next = first;
			_nextRank = Double.parseDouble(first[first.length-1].toString());
			loadNext();
		}
	}

	private void loadNext(){
		if(_next==null){
			_currentData = null;
			_currentIter = null;
		} else{
			TreeSet<Node[]> tempData = new TreeSet<Node[]>(NodeComparator.NC);
			tempData.add(_next);
			if(_iter==null || !_iter.hasNext()){
				_currentData = tempData;
				_currentIter = _currentData.iterator();
				_next = null;
				_nextTriple = null;
			} else{
				Node[] next = null;
				Node[] triple = null;
				boolean newTriple = false;
				while(_iter.hasNext()){
					next = _iter.next();
					triple = triple(next);
					if(NodeComparator.NC.equals(_nextTriple,triple)){
						if(tempData.add(next))
							_nextRank+=Double.parseDouble(next[next.length-1].toString());
					} else{
						newTriple = true;
						break;
					}
				}
				
				if(tempData.size()==1){
					_currentData = tempData;
				} else{
					_currentData = new TreeSet<Node[]>(NodeComparator.NC);
					Literal newrank = new Literal(Double.toString(_nextRank));
					for(Node[] na:tempData){
						_currentData.add(changeRank(na, newrank));
					}
				}
				
				_currentIter = _currentData.iterator();

				if(newTriple){
					_next = next;
					_nextTriple = triple(next);
					_nextRank =Double.parseDouble(next[next.length-1].toString());
				} else{
					_next = null;
					_nextTriple = null;
				}
			}
		}
	}

	protected static Node[] triple(Node[] na){
		if(na==null || na.length==0)
			return na;
		Node[] triple = new Node[3];
		System.arraycopy(na, 0, triple, 0, 3);

		return triple;
	}

	private static Node[] changeRank(Node[] na, Literal rank){
		Node[] ranked = new Node[na.length];
		System.arraycopy(na, 0, ranked, 0, na.length);
		ranked[ranked.length-1] = rank;
		return ranked;
	}

	public boolean hasNext() {
		return _currentIter!=null;
	}

	public Node[] next() {
		if(!hasNext())
			throw new NoSuchElementException();
		Node[] next = _currentIter.next();
		if(!_currentIter.hasNext())
			loadNext();
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
