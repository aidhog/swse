package org.semanticweb.swse.econs.sim.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.stats.Count;

public class Stats<E extends Number> implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static Logger _log = Logger.getLogger(Stats.class.getName());
	
	private double _sum = 0;
	private long _n = 0;
	private Count<E> _count;
	private int _top;
	private int _rand;
	
	private PriorityQueue<Nodes> _topPQ;
	private PriorityQueue<Nodes> _randPQ;

	public Stats(int top, int rand) {
		_count = new Count<E>();
		_top = top;
		_rand = rand;
		if(_top>0)
			_topPQ = new PriorityQueue<Nodes>(_top, ConfidenceComparator.INSTANCE);
		if(_rand>0)
			_randPQ = new PriorityQueue<Nodes>(_rand, RandomComparator.INSTANCE);
	}
	
	public void addAll(Stats<E> s){
		_count.addAll(s.getDistMap());
		_sum+=s._sum;
		_n+=s._n;
		
		if(_top>0 && s._topPQ!=null && !s._topPQ.isEmpty()){
			for(Nodes t:s._topPQ){
				if(_topPQ.size()<_top){
					_topPQ.add(t);
				} else if(ConfidenceComparator.INSTANCE.compare(_topPQ.peek(), t)<0){
					_topPQ.poll();
					_topPQ.add(t);
				}
			}
		}
		
		if(_rand>0 && s._randPQ!=null && !s._randPQ.isEmpty()){
			for(Nodes t:s._randPQ){
				if(_randPQ.size()<_rand){
					_randPQ.add(t);
				} else if(RandomComparator.INSTANCE.compare(_randPQ.peek(), t)<0){
					_randPQ.poll();
					_randPQ.add(t);
				}
			}
		}
	}

	public void addValue(E value, Nodes e1e2c) {
		_sum+=value.doubleValue();
		_n ++;
		_count.add(value);
		
		if(_top>0){
			if(_topPQ.size()<_top){
				_topPQ.add(e1e2c);
			} else if(ConfidenceComparator.INSTANCE.compare(_topPQ.peek(), e1e2c)<0){
				_topPQ.poll();
				_topPQ.add(e1e2c);
			}
		}
		
		if(_rand>0){
			if(_randPQ.size()<_rand){
				_randPQ.add(e1e2c);
			} else if(RandomComparator.INSTANCE.compare(_randPQ.peek(), e1e2c)<0){
				_randPQ.poll();
				_randPQ.add(e1e2c);
			}
		}
	}

	public Double getSum() {
		return _sum;
	}

	public Double getMean() {
		return _sum/(double)_n;
	}

	public Long getN() {
		return _n;
	}

	public void clear() {
		_count = new Count<E>();
		System.gc();
	}

	public Count<E> getDistMap() {
		return _count;
	}

	public static class ConfidenceComparator implements Comparator<Nodes>, Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static final ConfidenceComparator INSTANCE = new ConfidenceComparator();
		public int compare(Nodes arg0, Nodes arg1) {
			if(arg0==arg1)
				return 0;
			int comp = Double.compare(Double.parseDouble(arg0.getNodes()[2].toString()), Double.parseDouble(arg1.getNodes()[2].toString()));
			if(comp==0)
				return arg0.compareTo(arg1);
			return comp;
		}
		
	}
	
	public static class RandomComparator implements Comparator<Nodes>, Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public static final RandomComparator INSTANCE = new RandomComparator();
		public int compare(Nodes arg0, Nodes arg1) {
			if(arg0==arg1)
				return 0;
			int comp = arg0.hashCode()-arg1.hashCode();
			if(comp==0)
				return arg0.compareTo(arg1);
			return comp;
		}
	}
	
	public void logShortStats(){
		_log.info("Similarities count "+_n);
		_log.info("Similarities sum "+_sum);
		_log.info("Similarities mean "+getMean());
		_log.info("Distrib size "+_count.size());
	}
	
	public void logStats(){
		_log.info("Similarities count "+_n);
		_log.info("Similarities sum "+_sum);
		_log.info("Similarities mean "+getMean());
		
		ArrayList<Nodes> buffer = new ArrayList<Nodes>();
		if(_top>0){
			_log.info("===Top "+_top+"===");
			Nodes ns = null;
			while((ns = _topPQ.poll())!=null){
				_log.info(ns.toN3());
				buffer.add(ns);
			}
		}
		
		for(Nodes ns:buffer){
			_topPQ.add(ns);
		}
		buffer.clear();
		
		if(_rand>0){
			_log.info("===Random "+_rand+"===");
			Nodes ns = null;
			while((ns = _randPQ.poll())!=null){
				_log.info(ns.toN3());
				buffer.add(ns);
			}
		}
		
		for(Nodes ns:buffer){
			_randPQ.add(ns);
		}
			
		_log.info("===Distrib===");
		_count.printOrderedStats(_log, Level.INFO);
	}

}

