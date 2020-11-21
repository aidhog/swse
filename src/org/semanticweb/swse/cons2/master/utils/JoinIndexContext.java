package org.semanticweb.swse.cons2.master.utils;

import org.semanticweb.yars.nx.Resource;

public class JoinIndexContext extends Resource{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public static final String ROUND = "round#";
	public static final String ITER = "iter#";
	public static final String ORDER = "order#";
	
	private int _round;
	private int _iter = -1;
	private int[] _order = new int[]{0, 1, 2, 3};
	
	public JoinIndexContext(String root, int round, int iter, int[] order){
		super(root+ROUND+round+ITER+iter+ORDER+toString(order));
		_round = round;
		_order = order;
		_iter = iter;
	}
	
	public JoinIndexContext(String root, int round, int iter){
		super(root+ROUND+round+ITER+iter);
		_round = round;
		_iter = iter;
	}
	
	public JoinIndexContext(String root, int round, int[] order){
		super(root+ROUND+round+ORDER+toString(order));
		_round = round;
		_order = order;
	}
	
	public JoinIndexContext(String root, int round){
		super(root+ROUND+round);
		_round = round;
	}
	
	public static String toString(int[] order){
		StringBuffer buf = new StringBuffer();
		for(int e:order){
			buf.append(e);
		}
		return buf.toString();
	}
	
	public static int[] fromString(String order){
		int[] ord = new int[order.length()];
		for(int i=0; i<order.length(); i++){
			ord[i] = Integer.parseInt(new String(new char[]{order.charAt(i)}));
		}
		return ord;
	}
	
	public static JoinIndexContext parse(Resource r){
//		if(r instanceof JoinIndexContext)
//			return (JoinIndexContext)r;
		
		int round, iter;
		int[] order;
		int indexr = r.toString().indexOf(ROUND);
		
		if(indexr<0)
			return null;
		
		int indexi = r.toString().indexOf(ITER);
		int indexo = r.toString().indexOf(ORDER);
		
		if(indexi>0 && indexo>0){
			round = Integer.parseInt(r.toString().substring(indexr+ROUND.length(), indexi));
			iter = Integer.parseInt(r.toString().substring(indexi+ITER.length(), indexo));
			order = fromString(r.toString().substring(indexo+ORDER.length()));
			
			return new JoinIndexContext(r.toString().substring(0, indexr), round, iter, order);
		} else if(indexi>0){
			round = Integer.parseInt(r.toString().substring(indexr+ROUND.length(), indexi));
			iter = Integer.parseInt(r.toString().substring(indexi+ITER.length()));
			
			return new JoinIndexContext(r.toString().substring(0, indexr), round, iter);
		} else if(indexo>0){
			round = Integer.parseInt(r.toString().substring(indexr+ROUND.length(), indexo));
			order = fromString(  r.toString().substring(indexo+ORDER.length()));
			
			return new JoinIndexContext(r.toString().substring(0, indexr), round, order);
		} else {
			round = Integer.parseInt(r.toString().substring(indexr+ROUND.length()));
			return new JoinIndexContext(r.toString().substring(0, indexr), round);
		}
	}
	
	public int getRound(){
		return _round;
	}
	
	public int getRootlength(){
		return _data.indexOf(toString().indexOf(ROUND));
	}
	
	public String getRoot(){
		return toString().substring(0, toString().indexOf(ROUND));
	}
	
	public int getIteration(){
		return _iter;
	}
	
	public int[] getOrder(){
		return _order;
	}
}
