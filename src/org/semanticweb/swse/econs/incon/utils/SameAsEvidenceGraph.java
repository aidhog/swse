package org.semanticweb.swse.econs.incon.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import org.semanticweb.saorr.Statement;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.swse.cons.utils.SameAsIndex.SameAsList;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.namespace.OWL;

public class SameAsEvidenceGraph {
	static Logger _log = Logger.getLogger(SameAsEvidenceGraph.class.getName());
	
	
	
	public static Partition partition(MoreInfoProcessor mip, IncompatibleIdentifierGraph iig){
		HashMap<Node,Instance> map = new HashMap<Node,Instance>();
		
		for(Map.Entry<Node, HashSet<Nodes>> ine: mip._inlinks.entrySet()){
			Instance i = new Instance(ine.getKey());
			i._in = ine.getValue();
			map.put(ine.getKey(), i);
		}
		
		for(Map.Entry<Node, HashSet<Nodes>> ine: mip._outlinks.entrySet()){
			Instance i = map.get(ine.getKey()); 
			if(i==null){
				i = new Instance(ine.getKey());
				map.put(ine.getKey(), i);
			}
			i._out = ine.getValue();
		}
		
		for(Statement ifp: mip._ifpExpls.keySet()){
			Instance i = map.get(ifp.subject); 
			if(i==null){
				_log.severe("Could not find info for "+ifp.subject+" subject of IFP statement "+ifp);
			} else{
				if(i._ifps == null) i._ifps = new HashSet<Nodes>();
				i._ifps.add(new Nodes(ifp.predicate, ifp.object));
			}
		}
		
		for(Statement fp: mip._fpExpls.keySet()){
			Instance i = map.get(fp.object); 
			if(i==null){
				_log.severe("Could not find info for "+fp.object+" object of FP statement "+fp);
			} else{
				if(i._fps == null) i._fps = new HashSet<Nodes>();
				i._fps.add(new Nodes(fp.predicate, fp.subject));
			}
		}
		
		Partition p = new Partition(map, iig.getGraph());
		
		boolean done = false;
		while(!done){
			Instance[] insts = p.getPivotalInstances();
			TreeSet<SameAsLink> links = new TreeSet<SameAsLink>();
			
			for(int i=0; i<insts.length; i++){
				Instance ii = insts[i];
				Node ni = ii._n;
				HashSet<Node> incom = p._incom.get(ni);
				HashSet<Nodes> i_inlinks = mip._inlinks.get(ni);
				HashSet<Nodes> i_outlinks = mip._outlinks.get(ni);
				for(int j=i+1; j<insts.length; j++){
					if(incom==null || !incom.contains(insts[j]._n)){
						Instance ij = insts[j];
						Node nj = insts[j]._n;
						Nodes sameAs = new Nodes(OWL.SAMEAS, nj);
						
						boolean insa = i_inlinks!=null && i_inlinks.contains(sameAs);
						boolean outsa = i_outlinks!=null && i_outlinks.contains(sameAs);
						
						HashSet<Nodes> j_inlinks = mip._inlinks.get(nj);
						HashSet<Nodes> j_outlinks = mip._outlinks.get(nj);
						
						HashSet<Node> ifpFpVals = new HashSet<Node>();
						HashSet<Nodes> inter = ConcurrenceLite.getIntersection(ii._ifps, ij._ifps);
						inter.addAll( ConcurrenceLite.getIntersection(ii._fps, ij._fps));
						
						if(!inter.isEmpty()){
							for(Nodes ns:inter){
								ifpFpVals.add(ns.getNodes()[1]);
							}
						}
						
						double concur = ConcurrenceLite.concur(i_inlinks, j_inlinks, i_outlinks, j_outlinks, mip._inpstats, mip._outpstats);
						
						SameAsLink sal = new SameAsLink(ni, nj, insa, outsa, ifpFpVals, concur);
						links.add(sal);
					}
				}
			}
			
			if(links.isEmpty())
				done = true;
			else for(SameAsLink sal:links){
				boolean added = p.merge(sal._a, sal._b);
				System.err.println(added+"\t"+sal);
			}
		}
		
		return p;
	}
	
	
	public static class Instance {
		Node _n;
		
		//asserted in
		HashSet<Nodes> _in;
		//asserted out
		HashSet<Nodes> _out;
		
		//ifps (possibly inferred)
		HashSet<Nodes> _ifps;
		//fps (possibly inferred)
		HashSet<Nodes> _fps;
		
		public Instance(Node n){
			_n = n;
		}
		
		boolean addAll(Instance i){
			boolean ch = false;
			if(i._in!=null){
				if(_in == null) _in = new HashSet<Nodes>();
				ch |= _in.addAll(i._in);
			}
			if(i._out!=null){
				if(_out == null) _out = new HashSet<Nodes>();
				ch |= _out.addAll(i._out);
			}
			if(i._ifps!=null){
				if(_ifps == null) _ifps = new HashSet<Nodes>();
				ch |= _ifps.addAll(i._ifps);
			}
			if(i._fps!=null){
				if(_fps == null) _fps = new HashSet<Nodes>();
				ch |= _fps.addAll(i._fps);
			}
			return ch;
		}
	}
	
	
	/**
	 * Evidence for a single pair of nodes
	 * 
	 * @author aidhog
	 */
	public static class SameAsLink implements Comparable<SameAsLink>{
		Node _a, _b;
		
		boolean _inSA = false;		
		boolean _outSA = false;
		
		//sets ifp/fp values 
		//which give the sameas
		Set<Node> _ifpFpVals;
		
		double _concur;
		
		SameAsLink(Node a, Node b, boolean inSA, boolean outSA, Set<Node> ifpFpVals, double concur){
			_ifpFpVals = ifpFpVals;
			_inSA = inSA;
			_outSA = outSA;
			_concur = concur;
			_a = a;
			_b = b;
		}
		
		int getRank(){
			int rank = 0;
			if(_inSA)
				rank++;
			if(_outSA)
				rank++;
			rank += _ifpFpVals.size();
			
			return rank;
		}
		
		public int compareTo(SameAsLink arg0) {
			if(arg0==this)
				return 0;
			
			int comp = arg0.getRank() - getRank();
			if(comp != 0)
				return comp;
			
			comp = Double.compare(arg0._concur, _concur);
			if(comp != 0)
				return comp;
			
			comp = _a.compareTo(arg0._a);
			if(comp!=0)
				return comp;
			
			return _b.compareTo(arg0._b);
		}
		
		public String toString(){
			return _a.toN3()+" "+_b.toN3()+" rank:"+getRank()+" concur:"+_concur+" inSA:"+_inSA+" outSA:"+_outSA+" ifpFpVals:"+_ifpFpVals;
		}
	}
	
	public Nodes pair(Node a, Node b){
		if(a.compareTo(b)<=0){
			return new Nodes(a,b);
		}
		return new Nodes(b,a);
	}
	
	public static class Partition{
		Map<Node,SameAsList> _partition;
		Map<Node,Instance> _inst;
		Map<Node,HashSet<Node>> _incom;
		
		int partitions = 0;
		
		public Partition(Map<Node,Instance> inst, Map<Node,HashSet<Node>> incom){
			_partition = new HashMap<Node,SameAsList>();
			_inst = inst;
			_incom = incom;
		}
		
		public Instance[] getPivotalInstances(){
			ArrayList<Instance> insts = new ArrayList<Instance>();
			for(Map.Entry<Node, Instance> i:_inst.entrySet()){
				if(i.getKey().equals(i.getValue()._n)){
					insts.add(i.getValue());
				}
			}
			Instance[] ia = new Instance[insts.size()];
			insts.toArray(ia);
			return ia;
		}
		
		public boolean merge(Node a, Node b){
			if(a.equals(b)){
				return false;
			}
			
			HashSet<Node> ica = _incom.get(a);
			HashSet<Node> icb = _incom.get(b);
			
			if(ica==null){
				ica = new HashSet<Node>();
				_incom.put(a, ica);
			} else if(ica.contains(b)) return false;
			
			if(icb==null){
				icb = new HashSet<Node>();
				_incom.put(b, icb);
			} else if(icb.contains(a)) return false;

			SameAsList sala = _partition.get(a);
			SameAsList salb = _partition.get(b);
			
			Instance ia = _inst.get(a);
			Instance ib = _inst.get(b);
			
			for(Node n:ica){
				HashSet<Node> incn = _incom.get(n);
				if(incn == null){
					incn = new HashSet<Node>();
					_incom.put(n, incn);
				}
				incn.add(b);
				if(salb!=null)
					incn.addAll(salb);
			}
			
			for(Node n:icb){
				HashSet<Node> incn = _incom.get(n);
				if(incn == null){
					incn = new HashSet<Node>();
					_incom.put(n, incn);
				}
				incn.add(a);
				if(sala!=null)
					incn.addAll(sala);
			}
			
			if(sala==null && salb==null){
				SameAsList sal = new SameAsList();
				sal.add(a);
				sal.add(b);
				_partition.put(a, sal);
				_partition.put(b, sal);
				
				ia.addAll(ib);
				_inst.put(b, ia);
				ia._n = sal.getPivot();
				
				ica.addAll(icb);
				_incom.put(b, ica);
				partitions++;
			} else if(sala==null){
				salb.add(a);
				_partition.put(a, salb);
				
				ib.addAll(ia);
				_inst.put(a, ib);
				ib._n = salb.getPivot();
				
				icb.addAll(ica);
				_incom.put(a, icb);
			} else if(salb==null){
				sala.add(b);
				_partition.put(b, sala);
				
				ia.addAll(ib);
				_inst.put(b, ia);
				ia._n = sala.getPivot();
				
				ica.addAll(icb);
				_incom.put(b, ica);
			} else if(sala!=salb){
				partitions--;
				sala.addAll(salb);
				for(Node n:salb){
					_partition.put(n, sala);
					
					Instance in = _inst.get(n);
					ia.addAll(in);
					_inst.put(n, ia);
					
					HashSet<Node> icn = _incom.get(n);
					if(icn!=null)
						ica.addAll(icn);
					_incom.put(n, ica);
				}
				ia._n = sala.getPivot();
			} else{
				return false;
			}
			
			return true;
		}
		
	}
}
