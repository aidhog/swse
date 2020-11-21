package org.semanticweb.swse.econs.incon.utils;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.engine.ih.InconsistencyHandler;
import org.semanticweb.saorr.rules.TemplateRule;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.stats.Count;

public class InconLogger implements InconsistencyHandler {
	static Logger _log = Logger.getLogger(InconLogger.class.getName());
	MoreInfoProcessor _mii;
	PrintWriter _pw;

	int _incon, _sincon, _sconic, _suconic, _snconic;

	HashMap<Set<Statement>,Set<String>> _ies = new HashMap<Set<Statement>,Set<String>>();
	Set<String> _ids = new HashSet<String>();
//	TreeSet<FlatInconsistency> _ies = new TreeSet<FlatInconsistency>();
	
	Count<String> _rules = new Count<String>();
	Count<String> _prefixes = new Count<String>();
	Count<String> _arules = new Count<String>();
	Count<String> _aprefixes = new Count<String>();
	
	boolean _todo = false;
	
	public InconLogger(MoreInfoProcessor mii, PrintWriter pw){
		_mii = mii;
		_pw = pw;
	}

	public void handleInconsistency(InconsistencyException arg0) {
		String id = arg0.getDetectionRule().getID();
		for(Set<Statement> ss:arg0.getGuiltyData()){
			HSTreeSet<Statement> hsts = new HSTreeSet<Statement>(ss);
			Set<String> ids = _ies.get(hsts);
			if(ids==null){
				ids = new TreeSet<String>();
				_ies.put(hsts, ids);
			}
			ids.add(id);
			_ids.add(id);
		}
		_todo = true;
		_incon++;
	}	
	
	public void flush(){
		if(!_todo)
			return;
		_todo = false;
		
		IncompatibleIdentifierGraph iig = new IncompatibleIdentifierGraph();
		
		for(String s:_ids){
			_rules.add(s);
			_prefixes.add(TemplateRule.parseId(s)[0]);
		}
		
		for(Map.Entry<Set<Statement>, Set<String>> e:_ies.entrySet()){
			_sincon++;
			
			Boolean u = iig.addInconsistency(e.getKey(), _mii);
			if(u==null){
				_sconic++;
			} else if(u){
				_suconic++;
				_sconic++;
			} else{
				_snconic++;
				continue;
			}
			
			_pw.print(InconsistencyException.RULE_ID_PREFIX);
			for(String s:e.getValue()){
				_arules.add(s);
				_aprefixes.add(TemplateRule.parseId(s)[0]);
				_pw.print(" "+s);
			}
			_pw.println();
			
			for(Statement n:e.getKey()){
				String s = n.toString();
				
//				_mii.processStatement(n);
				HashSet<Node> moreInfo = _mii.getInfo(n.cloneTriple());
				if(moreInfo!=null) {
					for(Node ns:moreInfo){
						_pw.println(s+" -- "+ns.toN3());
					}
				} else{
					_pw.println(s);
					_log.severe("Could not find additional information for statement "+s);
				}
				
				HashSet<Nodes> refInfo = _mii.getReflexiveInfo(n.cloneTriple());
				if(refInfo!=null){
					for(Nodes ns:refInfo){
						_pw.print(s+" --");
						for(Node no:ns.getNodes()) 
							_pw.print(" "+no.toN3());
						_pw.println();
					}
				}
			}
			_pw.println();
		}
		
		if(iig.idCount()!=0){
			_log.info("Inconsistent ID graph for canonical ID "+_mii.getLastId());
			_log.info("Inconsistent ID graph for all IDs "+_mii.getLastIds());
			_log.info("Inconsistent ID graph as follows "+iig);
			
			_mii.processInconsistency(iig);
		}
		
		//assuming low volume calls for logging...
		_pw.flush();
		_ies.clear();
		_ids.clear();
	}

	public void logStats(){
		_log.info("Inconsistency calls "+_incon);
		_log.info("Atomic inconsistencies "+_sincon);
		_log.info("Consolidation atomic inconsistencies "+_sconic);
		_log.info("Consolidation unique id atomic inconsistencies "+_suconic);
		_log.info("Non-consolidation atomic inconsistencies (skipped) "+_snconic);
		
		_log.info("Per-Entity Rules ...");
		_rules.printOrderedStats(_log, Level.INFO);
		_log.info("Per-Entity Prefixes ...");
		_prefixes.printOrderedStats(_log, Level.INFO);
		
		_log.info("Atomic Rules ...");
		_arules.printOrderedStats(_log, Level.INFO);
		_log.info("Atomic Prefixes ...");
		_aprefixes.printOrderedStats(_log, Level.INFO);
	}
	
	public static class HSTreeSet<E> extends TreeSet<E>{
		/**
		 * 
		 */
		private static final long serialVersionUID = -4553501040014628870L;
		private int _hc = 0;
		
		public HSTreeSet(){
			super();
		}
		
		public HSTreeSet(Collection<? extends E> c){
			super(c);
		}
		
		public HSTreeSet(Comparator<? super E> comp){
			super(comp);
		}
		
		public HSTreeSet(SortedSet<E> s){
			super(s);
		}
		
		public int hashCode(){
			if(_hc!=0) return _hc;
			_hc = super.hashCode();
			return _hc;
		}
	}
}
