package org.semanticweb.swse.qp.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.apache.lucene.search.ScoreDoc;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.qp.RMIQueryConstants;
import org.semanticweb.swse.qp.RMIQueryInterface;
import org.semanticweb.swse.qp.master.utils.Cache;
import org.semanticweb.swse.qp.master.utils.ResultMergeIterator;
import org.semanticweb.swse.qp.master.utils.Slicer;
import org.semanticweb.swse.qp.utils.QueryProcessor;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.util.Array;

import com.healthmarketscience.rmiio.RemoteIterator;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterQuery {
	private final static Logger _log = Logger.getLogger(MasterQuery.class.getSimpleName());

	private RMIRegistries _servers;
	private RMIClient<RMIQueryInterface> _rmic;

	//TODO cache for individual keyword results (which will
	//also give reuse of Node[] references in keyword cache)
	private Cache<Pair<Node,String>, Collection<Node[]>> _focus = null;
	private Cache<Pair<Node,String>, Collection<Node[]>> _entity = null;
	private Cache<Pair<String,String>, Collection<Node[]>> _keyword = null;
	
	private static final int DEFAULT_FOCUS_CACHE_SIZE = 100;
	private static final int DEFAULT_ENTITY_CACHE_SIZE = 1000;
	private static final int DEFAULT_KEYWORD_CACHE_SIZE = 100;
	
	public MasterQuery(RMIRegistries servers, String lucene, String spoc, String sparse) throws Exception{
		this(servers, lucene, spoc, sparse, DEFAULT_FOCUS_CACHE_SIZE, DEFAULT_ENTITY_CACHE_SIZE, DEFAULT_KEYWORD_CACHE_SIZE);
	}
	
	public MasterQuery(RMIRegistries servers, String lucene, String spoc, String sparse, int focusCacheSize, int entityCacheSize, int kwCacheSize) throws Exception{
		_servers = servers;
		_rmic = new RMIClient<RMIQueryInterface>(servers, RMIQueryConstants.DEFAULT_STUB_NAME);
		
		if(focusCacheSize>0)
			_focus = new Cache<Pair<Node,String>, Collection<Node[]>>(focusCacheSize);
		if(entityCacheSize>0)
			_entity = new Cache<Pair<Node,String>, Collection<Node[]>>(entityCacheSize);
		if(kwCacheSize>0)
			_keyword = new Cache<Pair<String,String>, Collection<Node[]>>(kwCacheSize);
		
		_log.info("Initting servers...");
		Collection<RMIQueryInterface> stubs = _rmic.getAllStubs();
		VoidRMIThread[] init = new VoidRMIThread[stubs.size()];
		
		Iterator<RMIQueryInterface> stubIter = stubs.iterator();
		for(int i=0; i<init.length; i++){
			_log.info("...server "+i+"...");
			init[i] = new RemoteQueryInitThread(stubIter.next(), i, servers, RMIUtils.getLocalName(lucene,i), RMIUtils.getLocalName(spoc,i), RMIUtils.getLocalName(sparse,i));
			init[i].start();
		}

		for(int i=0; i<init.length; i++){
			init[i].join();
			if(!init[i].successful()){
				throw init[i].getException();
			}
			_log.info("...server "+i+" ready...");
		}
		_log.info("...initialised.");
	}

	public Iterator<Node[]> keywordQuery(String keywordQ, int from, int to, String langPrefix) throws Exception{
		long b4 = System.currentTimeMillis();
		String key = "\""+keywordQ+"\" "+from+":"+to;
		_log.info("Keyword : "+key);
		
		if(langPrefix==null)
			langPrefix = QueryProcessor.NULL_LANG;
		
		Pair<String,String> kwLang = new Pair<String,String>(key, langPrefix);
		
		if(_keyword!=null){
			Collection<Node[]> data = _keyword.get(kwLang);
			if(data!=null){
				_log.info("Keyword : "+key+" found in cache!");
				_log.info("Keyword : "+key+" (cached) finished in "+(System.currentTimeMillis()-b4)+" ms!");
				return data.iterator();
			}
		}
		
		
		Collection<RMIQueryInterface> stubs = _rmic.getAllStubs();
		RemoteSimpleQueryHitsThread[] hitsT = new RemoteSimpleQueryHitsThread[stubs.size()];
		
		Iterator<RMIQueryInterface> stubIter = stubs.iterator();
		for(int i=0; i<hitsT.length; i++){
			hitsT[i] = new RemoteSimpleQueryHitsThread(stubIter.next(), i, keywordQ, 0, to, langPrefix);
			hitsT[i].start();
		}

		ArrayList<ScoreDoc[]> hits = new ArrayList<ScoreDoc[]>();
		
		for(int i=0; i<hitsT.length; i++){
			hitsT[i].join();
			if(!hitsT[i].successful()){
				throw hitsT[i].getException();
			}
			hits.add(hitsT[i].getResult());
		}
		
		Slicer s = new Slicer(hits);
		
		ScoreDoc[][] slices = s.getSlice(from, to, _servers.getServerCount());
		
		RemoteRetrieveHitsThread[] rhitsT = new RemoteRetrieveHitsThread[stubs.size()];
		
		stubIter = stubs.iterator();
		for(int i=0; i<rhitsT.length; i++){
			rhitsT[i] = new RemoteRetrieveHitsThread(stubIter.next(), i, slices[i], langPrefix);
			rhitsT[i].start();
		}

		ArrayList<RemoteIterator<Node[]>> iters = new ArrayList<RemoteIterator<Node[]>>();
		
		for(int i=0; i<hitsT.length; i++){
			rhitsT[i].join();
			if(!rhitsT[i].successful()){
				throw rhitsT[i].getException();
			}
			iters.add(rhitsT[i].getResult());
		}
		
		ResultMergeIterator rmi = new ResultMergeIterator(iters);
		
		ArrayList<Node[]> results = new ArrayList<Node[]>();
		while(rmi.hasNext()){
			results.add(rmi.next());
		}
		
		if(_keyword!=null){
			_keyword.put(kwLang, results);
		}
		
		_log.info("Keyword : "+key+" (not-cached) finised in "+(System.currentTimeMillis()-b4)+" ms!");
		
		return results.iterator();
	}
	
	public Iterator<Node[]> focus(Node n, String langPrefix) throws Exception{
		long b4 = System.currentTimeMillis();
		_log.info("Focus : "+n.toN3()+"");
		
		if(langPrefix==null)
			langPrefix = QueryProcessor.NULL_LANG;
		
		Pair<Node, String> nl = new Pair<Node, String>(n, langPrefix);
		if(_focus!=null){
			Collection<Node[]> data = _focus.get(nl);
			if(data!=null){
				_log.info("Focus : "+n.toN3()+" found in cache");
				_log.info("Focus : "+n.toN3()+" (cached) finished in "+(System.currentTimeMillis()-b4)+" ms!");
				return data.iterator();
			}
		}
		
		ArrayList<RMIQueryInterface> stubs = _rmic.getAllStubs();
		int index = _servers.getServerNo(n);
		RemoteFocusThread hitsT = new RemoteFocusThread(stubs.get(index), index, n);
		hitsT.start();

		RemoteIterator<Node[]> iter = null;
		
		hitsT.join();
		if(!hitsT.successful()){
			throw hitsT.getException();
		}
		iter = hitsT.getResult();
		
		if(iter==null){
			return null;
		}
		
		ArrayList<Node[]> result = new ArrayList<Node[]>();
		TreeSet<Node>[] entities = new TreeSet[_servers.getServerCount()];
		
		for(int i=0; i<entities.length; i++){
			entities[i] = new TreeSet<Node>();
		}
		
		while(iter.hasNext()){
			Node[] na = iter.next();
			entities[_servers.getServerNo(na[1])].add(na[1]);
			if(!(na[2] instanceof Literal)){
				entities[_servers.getServerNo(na[2])].add(na[2]);
			}
			result.add(na);
		}
		
		getEntities(entities, result, langPrefix);
		
		
		
		if(_focus!=null){
			_focus.put(nl, result);
		}
		_log.info("Focus : "+n.toN3()+" (not cached) finished in "+(System.currentTimeMillis()-b4)+" ms!");
		
		return result.iterator();
	}
	
	private void getEntities(TreeSet<Node>[] entities, ArrayList<Node[]> allResults, String langPrefix) throws Exception{
		ArrayList<RMIQueryInterface> stubs = _rmic.getAllStubs();
		ArrayList<RemoteEntitiesThread> hitsT = new ArrayList<RemoteEntitiesThread>();
		
		if(langPrefix==null)
			langPrefix = QueryProcessor.NULL_LANG;
		
		for(int i=0; i<entities.length; i++){
			TreeSet<Node> notCached = new TreeSet<Node>();
			for(Node n:entities[i]){
				Pair<Node, String> nl = new Pair<Node, String>(n, langPrefix);
				Collection<Node[]> cached = null;
				if(_entity!=null){
					cached = _entity.get(nl);
				}
				if(cached==null){
					notCached.add(n);
				} else{
					allResults.addAll(cached);
				}
				
			}
			if(!notCached.isEmpty()){
				RemoteEntitiesThread hT = new RemoteEntitiesThread(stubs.get(i), i, notCached, langPrefix);
				hT.start();
				hitsT.add(hT);
			}
		}
		
		for(RemoteEntitiesThread hT:hitsT){
			hT.join();
			if(!hT.successful()){
				throw hT.getException();
			}
			
			RemoteIterator<Node[]> remoteResult = hT.getResult();
			Node olds = null;
			ArrayList<Node[]> entityResult = new ArrayList<Node[]>();
			if(remoteResult!=null) while(remoteResult.hasNext()){
				Node[] na = remoteResult.next();
				if(olds==null){
					olds = na[0];
				} else if(!na[0].equals(olds)){
					Pair<Node, String> nl = new Pair<Node, String>(olds, langPrefix);
					if(_entity!=null)
						_entity.put(nl, entityResult);
					allResults.addAll(entityResult);
					entityResult = new ArrayList<Node[]>();
				}
				entityResult.add(na);
			}
			if(olds!=null){
				Pair<Node, String> nl = new Pair<Node, String>(olds, langPrefix);
				if(_entity!=null)
					_entity.put(nl, entityResult);
				allResults.addAll(entityResult);
			}
		}
	}
	
	public static class Pair<E,F>{
		E _a;
		F _b;
		int _hc;
		
		public Pair(E a, F b){
			_a = a;
			_b = b;
			_hc = getHashCode();
		}
		
		public E getA(){
			return _a;
		}
		
		public F getB(){
			return _b;
		}
		
		private int getHashCode(){
			return Array.hashCode(_a, _b);
		}
		
		public int hashCode(){
			return _hc;
		}
		
	    /**
	     * Equality check
	     * 
	     * @param
	     * @return
	     */
	    public boolean equals(Pair<?,?> o) {
	    	if(o._a.equals(_a) && o._b.equals(_b))
	    		return true;
	    	return false;
	    }
	    
	    /**
	     * Equality check
	     * 
	     * @param
	     * @return
	     */
	    public boolean equals(Object o) {
	    	if (o instanceof Pair<?,?>){
	    		return equals((Pair<?,?>)o);
	    	} return false;
	    		
	    }
	}
}
