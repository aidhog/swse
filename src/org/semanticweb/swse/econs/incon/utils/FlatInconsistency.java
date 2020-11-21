package org.semanticweb.swse.econs.incon.utils;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.rules.TemplateRule;

public class FlatInconsistency implements Comparable<FlatInconsistency> {
	private final TreeSet<Statement> _data;
	private final String _ruleId;
	
	public FlatInconsistency(Collection<Statement> data, String id){
		_ruleId = id;
		
		_data = new TreeSet<Statement>();
		_data.addAll(data);
	}
	
	public TreeSet<Statement> getData(){
		return _data;
	}
	
	public String getRuleId(){
		return _ruleId;
	}
	
	public static TreeSet<FlatInconsistency> flatten(InconsistencyException ie){
		TreeSet<FlatInconsistency> ts = new TreeSet<FlatInconsistency>();
		String id = TemplateRule.getOriginalRuleId(ie.getDetectionRule().getID());
		for(Set<Statement> ss:ie.getGuiltyData()){
			ts.add(new FlatInconsistency(ss, id));
		}
		return ts;
	}

	public int compareTo(FlatInconsistency arg0) {
		if(this==arg0) return 0;
		int comp = _ruleId.compareTo(arg0._ruleId);
		if(comp!=0) return comp;
		return TreeSetComparator._compare(_data, arg0._data);
	}
	
	public boolean equals(Object o){
		if(this==o) return true;
		if(o instanceof FlatInconsistency){
			FlatInconsistency fi = (FlatInconsistency)o;
			boolean equals = fi._ruleId.equals(_ruleId);
			if(!equals) return false;
			return _data.equals(fi._data);
		}
		return false;
	}
}
