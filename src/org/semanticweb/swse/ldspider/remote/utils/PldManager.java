package org.semanticweb.swse.ldspider.remote.utils;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.logging.Logger;

public class PldManager implements Serializable{
	private static Logger _log = Logger.getLogger(PldManager.class.getName());
	/**
	 * Stores information relating to a PLD, including inlinks
	 * and ratio of useful documents.
	 */
	private static final long serialVersionUID = -1900546973054878089L;
	
	public static final double MIN = 0.1;

	private Hashtable <String, PldStats> _stats;
	
//	private transient double _maxScore = -1;
	
//	private transient double _averageScore = -1;
	
	public PldManager(){
		_stats = new Hashtable<String, PldStats>();
	}
	
	protected Hashtable <String, PldStats> getStats(){
		return _stats;
	}
	
	public int incrementUseful(String pld){
		return incrementUseful(pld, 1);
	}
	
	public int incrementUseful(String pld, int by){
		PldStats plds = getOrCreatePldStats(pld);
		return plds.incrementUseful(by);
	}
	
	public int incrementUseless(String pld){
		return incrementUseless(pld, 1);
	}
	
	public int incrementUseless(String pld, int by){
		PldStats plds = getOrCreatePldStats(pld);
		return plds.incrementUseless(by);
	}
	
	public int incrementPolled(String pld){
		return incrementPolled(pld, 1);
	}
	
	public int incrementPolled(String pld, int by){
		PldStats plds = getOrCreatePldStats(pld);
		return plds.incrementPolled(by);
	}
	
	public int incrementSkipped(String pld){
		return incrementSkipped(pld, 1);
	}
	
	public int incrementSkipped(String pld, int by){
		PldStats plds = getOrCreatePldStats(pld);
		return plds.incrementSkipped(by);
	}
	
//	public int incrementInlinks(String pld){
//		return incrementInlinks(pld, 1);
//	}
//	
//	public int incrementInlinks(String pld, int by){
//		PldStats plds = getOrCreatePldStats(pld);
//		int inlinks = plds.incrementInlinks(by);
//		return inlinks;
//	}
	
	public void addAll(PldManager pldm){
		Hashtable <String, PldStats> stats = pldm.getStats();
		for(Entry<String, PldStats> e:stats.entrySet()){
			PldStats plds = _stats.get(e.getKey());
			if(plds==null){
				_stats.put(e.getKey(), e.getValue());
			} else{
				plds.addAll(e.getValue());
			}
		}
	}
	
	public void newRound(){
//		if(!_stats.isEmpty()){
//			double sumRatioUseful = 0;
//			for(Entry<String,PldStats> e:_stats.entrySet()){
//				sumRatioUseful += e.getValue().getPercentageUseful();
//			}
//			
//			double averageRatioUseful = (double)sumRatioUseful/(double)_stats.size();
//			PldStats.setMeanUseful(averageRatioUseful);
//		}
	}
	
//	public void setAllActive(){
//		setActive(_stats.keySet());
//	}
	
//	public void setActive(Collection<String> activePlds){
//		_activePldCount = activePlds.size();
//		
//		if(_activePldCount==0)
//			return;
//		
//		double score = 0;
//		double sumScore = 0;
//		for(String pld:activePlds){
//			PldStats pldstats = getOrCreatePldStats(pld);
//			score = pldstats.getScore();
//			sumScore += score;
//			if(score>_maxScore){
//				_maxScore = score;
//			}
//		}
//		
//		_averageScore = sumScore/(double)_activePldCount;
//		System.err.println("MAX SCORE: "+_maxScore);
//	}

	public void logStats(){
		_log.info("======================");
		_log.info("PldStats");
		_log.info("Mean useful "+PldStats._meanUseful);
		_log.info("Size "+_stats.size());
		_log.info("======================");
		for(Entry<String,PldStats> e:_stats.entrySet()){
			_log.info("PLD "+e.getKey()+" "+e.getValue());
		}
		_log.info("======================");
	}
	
	public int size(){
		return _stats.size();
	}
	
	/**
	 * @todo it
	 * @param pld
	 * @param pldCount
	 * @return
	 */
	public double getScore(String pld){
		PldStats plds = getOrCreatePldStats(pld);
//		double normalisedScore = plds.getScore()/_maxScore;
//		double rand = (Math.random()*randLimit*(_averageScore/_maxScore));
//		return normalisedScore + ((1d-normalisedScore)*rand);
		return plds.getScoreUseful();
	}
	
	private PldStats getOrCreatePldStats(String pld){
		PldStats plds = _stats.get(pld);
		if(plds==null){
			plds = new PldStats();
			_stats.put(pld, plds);
		}
		return plds;
	}
	
	public static class PldStats implements Serializable{
			/**
		 * 
		 */
		public static final int MIN_CREDIBLE = 10;
		private static final long serialVersionUID = 1315747002934925046L;
		
		private static double _meanUseful = 1; 
		
			private int _useful = 0;
			private int _useless = 0;
			
//			private int _inlinks = 0;
			
			private int _polled = 0;
			private int _skipped = 0;
			
			public PldStats(){
				;
			}
			
//			public static void setMeanUseful(double meanUseful){
//				_meanUseful = meanUseful;
//			}
			
			public void addAll(PldStats plds){
//				incrementInlinks(plds.getInlinks());
				incrementUseful(plds.getUseful());
				incrementUseless(plds.getUseless());
				incrementPolled(plds.getPolled());
				incrementSkipped(plds.getSkipped());
			}
			
			public int incrementUseful(int by){
				_useful+=by;
				return _useful;
			}
			
			public int incrementUseful(){
				return incrementUseful(1);
			}
			
			public int incrementPolled(){
				return incrementPolled(1);
			}
			
			public int incrementPolled(int by){
				_polled+=by;
				return _polled;
			}
			
			public int incrementSkipped(){
				return incrementSkipped(1);
			}
			
			public int incrementSkipped(int by){
				_skipped+=by;
				return _skipped;
			}
			
			public int incrementUseless(int by){
				_useless+=by;
				return _useless;
			}
			
			public int incrementUseless(){
				return incrementUseless(1);
			}
			
			public int getUseful(){
				return _useful;
			}
			
			public int getUseless(){
				return _useless;
			}
			
			public int getPolled(){
				return _polled;
			}
			
			public int getSkipped(){
				return _skipped;
			}
			
			//credibility formula
			private double getScoreUseful(){
				double structuredScore = _useful;
				structuredScore+=(MIN_CREDIBLE)*_meanUseful;
				
				structuredScore/=(double)(_useful+_useless+MIN_CREDIBLE);
				return structuredScore;
			}
			
//			public double getScore(){
//				double structuredScore = getScoreUseful();
//				return structuredScore * (double)_inlinks;
//			}
			
			public double getPercentagePolled(){
				int total = _polled + _skipped;
				//benefit of the doubt algorithm
				if(total == 0){
					return 1;
				}
				return (double)_polled/(double)total;
			}
			
			public double getPercentageSkipped(){
				int total = _polled + _skipped;
				//benefit of the doubt algorithm
				if(total == 0){
					return 1;
				}
				return (double)_skipped/(double)total;
			}
			
			public double getPercentageUseful(){
				int total = _useless + _useful;
				//benefit of the doubt algorithm
				if(total == 0){
					return _meanUseful;
				}
				return (double)_useful/(double)total;
			}
			
			public double getPercentageUseless(){
				int total = _useless + _useful;
				//benefit of the doubt algorithm
				if(total == 0){
					return 1-_meanUseful;
				}
				return (double)_useless/(double)total;
			}
			
//			public int incrementInlinks(int by){
//				_inlinks+=by;
//				return _inlinks;
//			}
//			
//			public int incrementInlinks(){
//				return incrementInlinks(1);
//			}
//			
//			public int getInlinks(){
//				return _inlinks;
//			}
//			
//			public double getNormalisedInlinks(int maxInlinks){
//				return (double)_inlinks/(double)maxInlinks;
//			}
			
			public String toString(){
				return "useful:"+_useful+" useless:"+_useless+" ratioUseful:"+getPercentageUseful()+
				" usefulScore:"+getScoreUseful()+/*" inlinks:"+_inlinks+*/" polled:"+_polled+" skipped:"+_skipped+
				" percentPolled:"+getPercentagePolled()/*+" score:"+getScore()*/;
			}
	}
}
