package org.semanticweb.swse.qp.master.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.lucene.search.ScoreDoc;

public class Slicer {
	TreeSet<RemoteHit> _hits;
	
	public Slicer(ArrayList<ScoreDoc[]> hits){
		_hits = new TreeSet<RemoteHit>();
		
		for(int i=0; i<hits.size(); i++){
			ScoreDoc[] hs = hits.get(i);
			for(int j=0; j<hs.length; j++){
				_hits.add(new RemoteHit(i, hs[j]));
			}
		}
	}
	
	/**
	 * 
	 * @param from - inclusive
	 * @param to - inclusive
	 * @return
	 */
	public ScoreDoc[][] getSlice(int from, int to, int servers){
		if(from>0){
			from -= 1;
		}
		
		Iterator<RemoteHit> iter = _hits.iterator();
		
		for(int i=0; i<from; i++){
			if(iter.hasNext())
				iter.next();
			else break;
		}
		
		TreeSet<RemoteHit> slice = new TreeSet<RemoteHit>();
		for(int i=0; i<(to-from); i++){
			if(iter.hasNext()){
				slice.add(iter.next());
			} else break;
		}
		
		return toServerHits(slice, servers);
	}
	
	private static ScoreDoc[][] toServerHits(TreeSet<RemoteHit> slice, int servers){
		ScoreDoc[][] hits = new ScoreDoc[servers][];
		
		ArrayList<ScoreDoc>[] srvhits = new ArrayList[servers];
		
		for(int i=0; i<srvhits.length; i++){
			srvhits[i] = new ArrayList<ScoreDoc>();
		}
		
		for(RemoteHit rh:slice){
			srvhits[rh._server].add(rh._sd);
		}
		
		for(int i=0; i<srvhits.length; i++){
			hits[i] = new ScoreDoc[srvhits[i].size()];
			for(int j=0; j<srvhits[i].size(); j++){
				hits[i][j] = srvhits[i].get(j);
			}
		}
		
		return hits;
	}
	
	public static class RemoteHit implements Comparable<RemoteHit>{
		private int _server;
		private ScoreDoc _sd;
		
		public RemoteHit(int server, ScoreDoc sd){
			_server = server;
			_sd = sd;
		}
		
		public boolean equals(Object o){
			if(this==o)
				return true;
			else if(o instanceof RemoteHit){
				RemoteHit rt = (RemoteHit)o;
				return rt._sd.doc==_sd.doc && rt._sd.score==_sd.score && rt._server==_server;
			}
			return false;
		}
		
		public int compareTo(RemoteHit arg0) {
			if(_sd.score<arg0._sd.score){
				return 1;
			} else if(_sd.score>arg0._sd.score){
				return -1;
			} else{
				int c = _sd.doc - arg0._sd.doc;
				if(c!=0){
					return c;
				}
				return _server - arg0._server;
			}
		}
		
		public String toString(){
			return _sd.toString()+" srv="+_server;
		}
	}
}
