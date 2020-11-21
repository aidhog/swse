package org.semanticweb.swse.rank.utils;

import java.util.logging.Logger;

import org.deri.idrank.pagerank.PageRankInfo;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;

public class CallbackPageRank implements Callback{
	PageRankInfo _ranks;
	Logger _log = Logger.getLogger(CallbackPageRank.class.getName());
	
	public CallbackPageRank(){
		_ranks = new PageRankInfo();
	}
	
	public void endDocument() {
		;
	}

	public void processStatement(Node[] nx) {
		_ranks.add(nx);
	}

	public void startDocument() {
		;
	}
	
	public PageRankInfo getMap(){
		return _ranks;
	}

}
