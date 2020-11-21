package org.semanticweb.swse.econs.incon.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

import org.semanticweb.saorr.Statement;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;

public class IncompatibleIdentifierGraph {
	static Logger _log = Logger.getLogger(IncompatibleIdentifierGraph.class.getName());
	
	private HashMap<Node, HashSet<Node>> _graph;
	
	public IncompatibleIdentifierGraph(){
		_graph = new HashMap<Node, HashSet<Node>>();
	}
	
	/**
	 * 
	 * @param incon
	 * @param mip
	 * @return true if inconsistency is added, false if not due to consolidation, null if not unique wrt incompatible ids
	 * @throws UnsupportedOperationException
	 */
	public Boolean addInconsistency(Set<Statement> incon, MoreInfoProcessor mip) throws UnsupportedOperationException {
		if(incon == null)
			return false;
		
		if(incon.size()>2){
			throw new UnsupportedOperationException("Does not support inconsistencies with more than two triples. "+incon);
		}
		
		if(incon.size()==0){
			return false;
		} else if(incon.size()==1){
			Statement s = incon.iterator().next();
			if(s.subject.equals(s.object)){
				Set<Nodes> refIds = mip.getReflexiveInfo(s);
				if(refIds==null || refIds.isEmpty()){
					return false; //cannot be caused by consolidation
				} else{
					boolean done = false;
					for(Nodes ns:refIds){
						done |= loadInconsistentNodes(ns);
					}
					return done;
				}
			} else{
				return false;  //cannot be caused by consolidation
			}
		} else{
			Iterator<Statement> iter = incon.iterator();
			
			Statement s1 = iter.next();
			Statement s2 = iter.next();
			
			Set<Node> mi1 = new HashSet<Node>();
			mi1.addAll(mip.getInfo(s1));
			Set<Node> mi2 = new HashSet<Node>();
			mi2.addAll(mip.getInfo(s2));
			
			if(mi1==null){
				_log.severe("Info null for inconsistent statement "+s1);
				return false;
			}
			if(mi2==null){
				_log.severe("Info null for inconsistent statement "+s2);
				return false;
			}
			
			if(s1.subject.equals(s1.object)){
				Set<Nodes> refIds = mip.getReflexiveInfo(s1);
				if(refIds!=null){
					for(Nodes ns: refIds){
						for(Node n:ns.getNodes()){
							mi1.add(n);//conservative workround
							//should be a disjunction
						}
					}
				} 
			}
			
			if(s2.subject.equals(s2.object)){
				Set<Nodes> refIds = mip.getReflexiveInfo(s2);
				if(refIds!=null){
					for(Nodes ns: refIds){
						for(Node n:ns.getNodes()){
							mi2.add(n);//conservative workround
							//should be a disjunction
						}
					}
				} 
			}
			
			removeIntersection(mi1, mi2);
			
			if(mi1.isEmpty() || mi2.isEmpty())
				return false; //not due to consolidation
			
			
			if(loadPairwiseInconsistentSets(mi1, mi2))
				return true; //unique inconsitency due to consolidation
			return null; //inconsistency due to consolidation, but not unique wrt. ids
		}
	}
	
	private void removeIntersection(Set<Node> a, Set<Node> b){
		HashSet<Node> inter = getIntersection(a, b);
		for(Node n:inter){
			a.remove(n);
			b.remove(n);
		}
	}
	
	private HashSet<Node> getIntersection(Set<Node> a, Set<Node> b){
		HashSet<Node> inter = new HashSet<Node>();
		
		for(Node na:a){
			if(b.contains(na)){
				inter.add(na);
			}
		}
		
		return inter;
	}
	
	public boolean isStronglyConnected(){
		boolean sg = true;
		int eCount = idCount() - 1;
		for(HashSet<Node> edges:_graph.values()){
			sg &= edges.size() == eCount;
		}
		return sg;
	}
	
	public boolean loadPairwiseInconsistentSets(Set<Node> inconsa, Set<Node> inconsb){
		if(inconsa==null || inconsa.isEmpty() || inconsb==null || inconsb.isEmpty())
			return false;
		
		boolean changed = false;
		for(Node a:inconsa){
			HashSet<Node> ica = getOrCreateInconsistentIds(a);
			for(Node b:inconsb){
				if(!a.equals(b)){
					changed |= ica.add(b);
				}
			}
		}
		
		for(Node b:inconsb){
			HashSet<Node> icb = getOrCreateInconsistentIds(b);
			for(Node a:inconsa){
				if(!a.equals(b)){
					changed |= icb.add(a);
				}
			}
		}
		
		return changed;
	}
	
	public boolean loadPairwiseInconsistentSet(Set<Node> incons){
		if(incons==null || incons.size()<2)
			return false;
		
		boolean changed = false;
		for(Node a:incons){
			HashSet<Node> ica = getOrCreateInconsistentIds(a);
			for(Node b:incons){
				if(!a.equals(b)){
					changed |= ica.add(b);
				}
			}
		}
		
		return changed;
	}
	
	public boolean loadInconsistentNodes(Nodes ns){
		Node[] na = ns.getNodes();
		
		if(na.length<2){
			return false;
		} else if(na.length>2){
			HashSet<Node> incons = new HashSet<Node>();
			for(Node n:na)
				incons.add(n);
			return loadPairwiseInconsistentSet(incons);
		} else{
			if(!na[0].equals(na[1])){
				addAsymmetricPair(na[0], na[1]);
				addAsymmetricPair(na[1], na[0]);
				return true;
			}
			return false;
		}
	}
	
	private boolean addAsymmetricPair(Node a, Node b){
		HashSet<Node> ai = getOrCreateInconsistentIds(a);
		return ai.add(b);
	}
	
	public HashSet<Node> getInconsistentIds(Node n){
		return _graph.get(n);
	}
	
	private HashSet<Node> getOrCreateInconsistentIds(Node n){
		HashSet<Node> inc = _graph.get(n);
		if(inc==null){
			inc = new HashSet<Node>();
			_graph.put(n, inc);
		}
		return inc;
	}
	
	public String toString(){
		return _graph.toString();
	}
	
	public HashMap<Node, HashSet<Node>> getGraph(){
		return _graph;
	}
	
	public int idCount(){
		return _graph.size();
	}
}
