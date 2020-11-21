package org.semanticweb.swse.econs.incon.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;

/**
 * Concurrence (very-)lite.
 * 
 * @author	Aidan Hogan
 * @since	1.6
 */
public class ConcurrenceLite {

	static Logger _log = Logger.getLogger(ConcurrenceLite.class.getName());
	
	/**
	 * Generate pair-wise concurrence for the given set of instances,
	 * attribute values, and distribution of string values.
	 * 
	 * @param insts	instances to match
	 * @param stats	statistics about numeric attributes
	 * @param str_stats	distribution of string values for each attribute
	 * @return square matrix presenting the concurrence scores following the indexes of the list of instances given in the call
	 */
	public static double concur(HashSet<Nodes> ina, HashSet<Nodes> inb, 
			HashSet<Nodes> outa, HashSet<Nodes> outb, Map<Node,Double> inpstats, Map<Node,Double> outpstats){
		ArrayList<Double> scores = new ArrayList<Double>();
		
		HashSet<Nodes> interIn = getIntersection(ina, inb);
		
		for(Nodes n:interIn){
			Double val = inpstats.get(n.getNodes()[0]);
			if(val!=null){
				scores.add(val);
			}
//			else{ //owlSameAs, IFP or FP
//				_log.severe("No score for "+n.getNodes()[0]);
//			}
		}
		
		HashSet<Nodes> interOut = getIntersection(outa, outb);
		
		for(Nodes n:interOut){
			Double val = outpstats.get(n.getNodes()[0]);
			if(val!=null){
				scores.add(val);
			} 
//			else{ //owlSameAs, IFP or FP
//				_log.severe("No score for "+n.getNodes()[0]);
//			}
		}
		
		return aggregateConcurrenceScores(scores, 1d);
	}
	
	/**
	 * Aggregate a list of concurrence scores into a final score
	 * 
	 * @param confs	list of concurrence scores
	 * @param max	maximum value the score can take
	 * @return aggregated concurrence score
	 */
	private static double aggregateConcurrenceScores(ArrayList<Double> confs, double max){
		double agg = 0;

		for(double d:confs){
			double r = 1 - agg;
			double c = r * d;
			agg+=c;
		}
		return agg * max;
	}
	
	public static <E> HashSet<E> getIntersection(Set<E> a, Set<E> b){
		HashSet<E> inter = new HashSet<E>();
		
		if(a==null || b==null || a.isEmpty() || b.isEmpty())
			return inter;
		
		for(E na:a){
			if(b.contains(na)){
				inter.add(na);
			}
		}
		
		return inter;
	}
}
