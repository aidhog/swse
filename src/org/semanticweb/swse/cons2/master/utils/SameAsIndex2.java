package org.semanticweb.swse.cons2.master.utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.stats.Count;

/**
 * Class for keeping same-as chains in memory
 * @author aidhog
 */
public class SameAsIndex2 implements Serializable{
	/**
	 * 
	 */
	public static final int CACHE_SIZE = 100;
	
	private static final long serialVersionUID = 1L;
	private Hashtable<Node, SameAsList> _index;
	
	protected boolean _empty = true;
	protected boolean _changed = false;
	protected OnDiskReasoner[] _odrs;
	
	private int _added;
	private int _limit;
	
	
	public SameAsIndex2(){
		this(-1);
	}
	
	public SameAsIndex2(int limit){
		_index = new Hashtable<Node, SameAsList>();
		_limit = limit;
	}

	public int size(){
		return _index.size();
	}

	//can be made protected, public for debugging
	public SameAsList handleSameAsPair(Node a, Node b) throws IndexFullException {
		_empty = false;
		SameAsList sala = _index.get(a);
		SameAsList salb = _index.get(b);
		
		SameAsList newsal = new SameAsList();
		
		if(sala==null || salb==null || sala!=salb){
			_changed = true;
			if(sala !=null && salb!=null && sala!=salb){
				Node pa = sala.getPivot();
				Node pb = salb.getPivot();
				int comp = pa.compareTo(pb);
				SameAsList pl = null;
				SameAsList nl = null;
				if(comp<=0){
					pl = sala;
					nl = salb;
				}
				else if(comp>0){
					pl = salb;
					nl = sala;
				}
				
				pl.addAll(nl);
				
				
				for(Node n:nl){
					_index.put(n, pl);
				}
				
				newsal.addAll(nl);
				newsal.add(pl.first());
			} else if(sala==null && salb==null){
				_added+=2;
				sala = new SameAsList();
				sala.add(a);
				sala.add(b);
				_index.put(a, sala);
				_index.put(b, sala);
				
				newsal.addAll(sala);
			} else if(sala!=null){
				_added++;
				sala.add(b);
				_index.put(b, sala);
				
				newsal.add(sala.first());
				newsal.add(b);
			} else{
				_added++;
				salb.add(a);
				_index.put(a, salb);
				
				newsal.add(a);
				newsal.add(salb.first());
			}
		} else return null;
		
		if(_limit!=-1 && _added>_limit){
			//hack to avoid further IFEs after adding last element
			_limit = _added;
			throw new IndexFullException();
		}
		return newsal;
	}
	
	public Node getPivot(Node n){
		SameAsList sal = _index.get(n);
		if(sal==null){
			return n;
		}
		Node p = sal.getPivot();
		if(p.equals(n))
			return n;
		return p;
	}
	
	protected void forceNewPivot(Node remove, Node neu){
		SameAsList sai = _index.get(remove);
		if(sai==null)
			return;
		sai.remove(remove);
		_index.remove(remove);
		sai.add(neu);
		_index.put(neu, sai);
	}
	
	protected SameAsList handleSameAsList(TreeSet<Node> sameAs) throws IndexFullException{
		if(sameAs.size()<2)
			return null;
		_empty = false;
		
		Iterator<Node> iter = sameAs.iterator();
		Node first = iter.next();
		SameAsList newsal = new SameAsList();
		newsal.addAll(sameAs);
		while(iter.hasNext()){
			Node next = iter.next();
			newsal.addAll(handleSameAsPair(first, next));
		}
		
		return newsal;
	}

	public int writeToFile(Callback out, Resource context) throws IOException{
		Set<Entry<Node, SameAsList>> entries = _index.entrySet();
		Node[] sa = { null, OWL.SAMEAS, null, context };
		int i=0;
		for(Entry<Node, SameAsList> entry:entries){
			//ensure duplicate elements are not sent to be printed
			//(very clever this :) )
			SameAsList sal = entry.getValue();
			if(sal.first().equals(entry.getKey())){
				Node pivot = sal.getPivot();
				for(Node n:sal){
					if(!pivot.equals(n)){
						sa[0] = n;
						sa[2] = pivot;
						out.processStatement(sa);
						i++;
						
						sa[0] = pivot;
						sa[2] = n;
						out.processStatement(sa);
						i++;
					}
				}
				
			}
		}
		
		return i;
	}
	
	public void writeFullChainsToFile(Callback out, Resource context) throws IOException{
		Set<Entry<Node, SameAsList>> entries = _index.entrySet();
		for(Entry<Node, SameAsList> entry:entries){
			//ensure duplicate elements are not sent to be printed
			//(very clever this :) )
			if(entry.getValue().first().equals(entry.getKey())){
				writeSameAsChain(entry.getValue(), out, context);
			}
		}
	}
	
	public boolean isEmpty(){
		return _empty;
	}
	
	public boolean hasChanged(){
		return _changed;
	}
	
	public boolean resetChangedFlag(){
		boolean old = _changed;
		_changed = false;
		return old;
	}
	
	public SameAsList getEquivalents(Node n){
		return _index.get(n);
	}
	
	public Count<Integer> getCrappyDistribution(){
		Count<Integer> count = new Count<Integer>();
		for(SameAsList sal:_index.values()){
			count.add(sal.size());
		}
		return count;
	}
	
	protected static void writeSameAsChain(SameAsList sameas, Callback out, Resource context) throws IOException{
		Node[] sa = { null, OWL.SAMEAS, null, context };
		if(sameas.size()<2)
			return;
		
		Iterator<Node> iter = sameas.iterator();
		Node pivot = sameas.getPivot();
		if(pivot==null)
			pivot = iter.next();
		
		while(iter.hasNext()){
			Node next = iter.next();
			if(next == pivot){
				continue;
			}
				
			sa[0] = pivot;
			sa[2] = next;

			out.processStatement(sa);

			sa[0] = next;
			sa[2] = pivot;

			out.processStatement(sa);
		}
	}
	
	public static class SameAsList extends TreeSet<Node>{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public SameAsList(){
			super();
		}
		
		public Node getPivot(){
			return first();
		}
	}
	
	public static class IndexFullException extends Exception{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public IndexFullException(){
			super();
		}
		
		public IndexFullException(String msg){
			super(msg);
		}
	}
}


