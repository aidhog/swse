package org.semanticweb.swse.ann.repair.utils;

import java.util.Map;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.util.FlyweightNodeIterator;
import org.semanticweb.yars.util.ResetableIterator;

public class ResetableFlyweightNodeIterator extends FlyweightNodeIterator implements ResetableIterator<Node[]>{
	ResetableIterator<Node[]> _in;
	
	public ResetableFlyweightNodeIterator(int flyweightCacheSize, ResetableIterator<Node[]> in) {
		super(flyweightCacheSize, in);
		_in = in;
	}
	
	public ResetableFlyweightNodeIterator(Map<Node,Node> flyweightCache, ResetableIterator<Node[]> in) {
		super(flyweightCache, in);
		_in = in;
	}

	public void reset() {
		_in.reset();
	}
	
}
