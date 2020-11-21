package ie.deri.urq.cons_eval;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;

public class SameAsIndex implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7558342206327360421L;
	private HashMap<Node, SameAsList> _sal;
	private int _pairs = 0, _uniquepairs = 0;
	
	public static final Resource CONTEXT = new Resource("http://sw.deri.org/2010/01/rmi/cons/#sameas");
	
	public SameAsIndex(){
		_sal = new HashMap<Node, SameAsList>();
	}
	
	public Map<Node, SameAsList> getIndex(){
		return _sal;
	}
	public int size(){
//		int size = 0;
//		for(SameAsList l : _sal.values()){
//			size+=_sal.size();
//		}
		return _sal.size();
	}
	
	public boolean addSameAs(Node a, Node b){
		if(a.equals(b)){
			return false;
		}
		
		SameAsList sala = _sal.get(a);
		SameAsList salb = _sal.get(b);
		
		_pairs++;
		if(sala==null && salb==null){
			SameAsList sal = new SameAsList();
			sal.add(a);
			sal.add(b);
			_sal.put(a, sal);
			_sal.put(b, sal);
		} else if(sala==null){
			salb.add(a);
			_sal.put(a, salb);
		} else if(salb==null){
			sala.add(b);
			_sal.put(b, sala);
		} else if(sala!=salb){
			sala.addAll(salb);
			for(Node n:salb){
				_sal.put(n, sala);
			}
		} else{
			return false;
		}
		_uniquepairs++;
		return true;
	}
	
	public SameAsList getSameAsList(Node n){
		return _sal.get(n);
	}
	
	public Node getPivot(Node n){
		SameAsList sal = getSameAsList(n);
		
		if(sal==null){
			return n;
		} 
		Node pivot = sal.getPivot();
		
		if(pivot==null){
			return n;
		} 
		return pivot;
	}
	
	public void logStats(){
		TreeSet<SameAsList> ts = new TreeSet<SameAsList>();
		ts.addAll(_sal.values());
		
		int i  = 0, size = -1;
		
//		_log.info("SAMEAS INDEX SIZE "+_sal.size());
		for(SameAsList sal:ts){
//			_log.info(sal.toString());
			if(size==-1){
				size = sal.size();
			} else if(sal.size()!=size){
//				_log.info("DISTRIB "+size+" "+i);
				size = sal.size();
				i=0;
			}
			i++;
		}
//		_log.info("DISTRIB "+size+" "+i);
	}
	
	
	public static class SameAsList extends TreeSet<Node> implements Comparable<SameAsList>, Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = -5975665868041226497L;
		Node _pivot = null;
		
		public SameAsList(){
			super();
		}
		
		public void setPivot(Node n){
			_pivot = n;
		}
		
		
		public Node getPivot(){
			if(_pivot==null){
				return first();
			}
			return _pivot;
		}

		public int compareTo(SameAsList arg0) {
			if(this==arg0){
				return 0;
			} else if(arg0.size()>size()){
				return 1;
			} else if(arg0.size()==size()){
				Node n1 = arg0.first();
				Node n2 = first();
				return n2.compareTo(n1);
			} else{
				return -1;
			}
		}
		
		public String toString(){
			StringBuffer buf = new StringBuffer();
			for(Node n:this){
				buf.append(n.toN3()+" ");
			}
			return buf.toString();
		}
	}
}
