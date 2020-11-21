package org.semanticweb.swse.ann.rank.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import org.semanticweb.swse.ann.rank.RMIAnnRankingServer;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.reorder.ReorderIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.util.SniffIterator;

public class RankTriplesIterator implements Iterator<Node[]>{
	public static final double NO_RANK = 0d;
	
	static Logger _log = Logger.getLogger(RankTriplesIterator.class.getName()); 
	
	final Iterator<Node[]> _iter;
	final int[] _invOrder;
	
	double _currentRank;
	Node _currentCon;
	
	Node[] _last = null;
	Node[] _current = null;
	
	/**
	 * Rank triples given by iterator in using ranks in iterator rank
	 * @param in input triples sorted by c
	 * @param ranks input source ranks sorted by c
	 */
	public RankTriplesIterator(Iterator<Node[]> in, Iterator<Node[]> ranks){
		SniffIterator si = new SniffIterator(in);
		if(si.hasNext()){
			short len = si.nxLength();
			int[] order = RMIAnnRankingServer.CONTEXT_SORT_ORDER;
			if(order.length<len){
				//pad
				int[] temp = new int[len];
				System.arraycopy(order, 0, temp, 0, order.length);
				for(int i=order.length; i<len; i++){
					temp[i] = i;
				}
				order = temp;
			}
			ReorderIterator ri = new ReorderIterator(si, order);
			_invOrder = ReorderIterator.getInvOrder(order);
			_iter = new MergeSortIterator(ri, ranks);
			loadNext();
		} else{
			_invOrder = null;
			_iter = null;
		}
	}
	
	private void loadNext(){
		_current = null;
		if(_iter.hasNext()){
			Node[] next = _iter.next();
			while(next.length==2){
				_currentRank = Double.parseDouble(next[1].toString());
				_currentCon = next[0];
				if(!_iter.hasNext())
					return;
				else next = _iter.next();
			}
			Node[] temp = ReorderIterator.reorder(next, _invOrder);
			_current = new Node[temp.length+1];
			System.arraycopy(temp, 0, _current, 0, temp.length);
			if(next[0].equals(_currentCon)){
				_current[temp.length] = new Literal(Double.toString(_currentRank));
			} else{
				_current[temp.length] = new Literal(Double.toString(NO_RANK));
				_log.severe("Could not find rank for "+temp[3]);
			}
		}
	}
	
	public boolean hasNext() {
		return _current!=null;
	}

	public Node[] next() {
		if(!hasNext())
			throw new NoSuchElementException();
		Node[] next = _current;
		loadNext();
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
