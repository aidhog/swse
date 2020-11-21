package org.semanticweb.swse.qp.master.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class Cache<E,F> {
	private static final float LOAD_FACTOR = 0.75f;

	private final LinkedHashMap<E, F> _cache;
	private int _size;
	
	public Cache(int size){
		_size = size;
		int hashTableCapacity = (int)Math.ceil(size / LOAD_FACTOR) + 1;
		_cache = new LinkedHashMap<E,F>(hashTableCapacity, LOAD_FACTOR, true) {
		      // (an anonymous inner class)
		      private static final long serialVersionUID = 1;
		     
		      @Override protected boolean removeEldestEntry (Map.Entry<E,F> eldest) {
		         return size() > _size; 
		      }
		};
	}
	
	public synchronized F get(E k){
		return _cache.get(k);
	}
	
	public synchronized void clear(){
		_cache.clear();
	}
	
	public synchronized void put(E k, F data){
		_cache.put(k, data);
	}
}
