package org.semanticweb.swse.ann.agg.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import org.semanticweb.saorr.rules.DefaultRule;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;

public class AggregateTripleMaxRanksIterator implements Iterator<Node[]>{
	static Logger _log = Logger.getLogger(AggregateTripleMaxRanksIterator.class.getName()); 

	final Iterator<Node[]> _iter;

	Node[] _currentData = null;
	Node[] _next = null;
	Node[] _nextTriple = null;

	double _nextRank = 0;
	
	long dominatedReasoned = 0, dominatedAsserted = 0, sameann = 0;
	long scanned = 0, returned = 0;

	public AggregateTripleMaxRanksIterator(Iterator<Node[]> in){
		_iter = in;
		if(_iter.hasNext()){
			Node[] first = _iter.next();
			scanned++;
			_nextTriple = triple(first);
			_next = first;
			_nextRank = Double.parseDouble(first[first.length-1].toString());
			loadNext();
		}
	}

	private void loadNext(){
		if(_next==null){
			_currentData = null;
		} else{
			if(_iter==null || !_iter.hasNext()){
				_currentData = appendRank(_nextTriple, _nextRank);
				_next = null;
				_nextTriple = null;
			} else{
				Node[] next = null;
				Node[] triple = null;
				Node maxContext = _next[3];
				boolean newTriple = false;
				while(_iter.hasNext()){
					next = _iter.next();
					scanned++;
					triple = triple(next);
					if(NodeComparator.NC.equals(_nextTriple,triple)){
						double rank = Double.parseDouble(next[next.length-1].toString());;
						if(rank>_nextRank){
							_nextRank=rank;
							if(DefaultRule.isReasonedContext(maxContext)){
								dominatedReasoned++;
							} else{
								dominatedAsserted++;
							}
							maxContext = next[3];
						} else if(_nextRank==rank){
							sameann++;
						} else{
							if(DefaultRule.isReasonedContext(next[3])){
								dominatedReasoned++;
							} else{
								dominatedAsserted++;
							}
						}
					} else{
						newTriple = true;
						break;
					}
				}
				
				_currentData = appendRank(_nextTriple, _nextRank);
				
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
	
	private static Node[] appendRank(Node[] na, double rank){
		return appendRank(na, new Literal(Double.toString(rank)));
	}

	private static Node[] appendRank(Node[] na, Literal rank){
		Node[] ranked = new Node[na.length+1];
		System.arraycopy(na, 0, ranked, 0, na.length);
		ranked[ranked.length-1] = rank;
		return ranked;
	}

	public boolean hasNext() {
		return _currentData!=null;
	}

	public Node[] next() {
		if(!hasNext())
			throw new NoSuchElementException();
		Node[] next = _currentData;
		returned++;
		loadNext();
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	public void logStats(){
		System.err.println("Scanned "+scanned+". Returned "+returned+". Same-annotation "+sameann+". Reason-dominated "+dominatedReasoned+". Asserted-dominated "+dominatedAsserted);
	}
}
