package org.semanticweb.swse.econs.incon.utils;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.rules.TemplateRule;

public class InconsistencyExceptionComparator  implements Comparator<InconsistencyException>{
	public final static InconsistencyExceptionComparator INSTANCE = new InconsistencyExceptionComparator();


	public int compare(InconsistencyException arg0,
			InconsistencyException arg1) {
		int comp = TemplateRule.parseId(arg0.getDetectionRule().getID())[0].compareTo(TemplateRule.parseId(arg0.getDetectionRule().getID())[1]);
		if(comp!=0){
			return comp;
		}
		
		Collection<Set<Statement>> data1 = arg0.getGuiltyData();
		Collection<Set<Statement>> data2 = arg1.getGuiltyData();
		
		comp = data1.size() - data2.size();
		if(comp!=0){
			return comp;
		}
		
		if(data1.size()!=1){
			return -1;
		}
		
		Set<Statement> s1 = data1.iterator().next();
		Set<Statement> s2 = data2.iterator().next();
		
		if(s1.equals(s2)){
			return 0;
		}
		return -1;
	}
}
