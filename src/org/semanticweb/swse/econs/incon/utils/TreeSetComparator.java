package org.semanticweb.swse.econs.incon.utils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class TreeSetComparator<E extends Comparable<E>> implements Comparator<TreeSet<E>> {
	
	public int compare(TreeSet<E> arg0, TreeSet<E> arg1) {
		return _compare(arg0, arg1);
	}

	/**
	 * Assumes TreeSets have same Comparator
	 * @param <E>
	 * @param a
	 * @param b
	 * @return
	 */
	public static <E extends Comparable<E>> int _compare(TreeSet<E> a, TreeSet<E> b){
		int comp = a.size() - b.size();
		if(comp!=0) return comp;
		Iterator<E> itera = a.iterator();
		Iterator<E> iterb = b.iterator();
		
		while(itera.hasNext()){
			comp = itera.next().compareTo(iterb.next());
			if(comp!=0) return comp;
		}
		
		return comp;
	}
}
