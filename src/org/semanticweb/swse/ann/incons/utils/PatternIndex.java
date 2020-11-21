package org.semanticweb.swse.ann.incons.utils;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Logger;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.rules.SortedRuleSet.SortedLinkedRuleSet;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Variable;
import org.semanticweb.yars.util.LRUMapCache;

/**
 * An index for rules -- given a particular statement, the rule
 * index will return the rules that might be interested in the given
 * statement.
 *
 * The rule index has no real notion of terminological or assertional 
 * patterns, other than which (or both) to index. It's probably wise to 
 * keep two separate indexes where such a distinction is necessary.
 * 
 * @author Aidan Hogan
 *
 */
public class PatternIndex implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5922641019308677111L;

	private static Logger logger = Logger.getLogger(PatternIndex.class.getName());
	
	protected static int HASHSET_INIT_CAPACITY = 2;

	private HashSet<Statement> _empty;

	//indexes for full statements
	private HashMap<Nodes, HashSet<Statement>> _s;
	private HashMap<Nodes, HashSet<Statement>> _p;
	private HashMap<Nodes, HashSet<Statement>> _o;

	private HashMap<Nodes, HashSet<Statement>> _sp;
	private HashMap<Nodes, HashSet<Statement>> _po;
	private HashMap<Nodes, HashSet<Statement>> _so;

	private HashMap<Nodes, HashSet<Statement>> _spo;

	/**
	 * Build an index for rule patterns
	 * @param aboxRules rules to index
	 * @param tbox if t-box patterns should be indexed
	 * @param abox if a-box patterns should be indexed
	 */
	public PatternIndex(Collection<Statement> patterns){
		_empty = new HashSet<Statement>();

		//indexes for full statements
		_s = new HashMap<Nodes, HashSet<Statement>>();
		_p = new HashMap<Nodes, HashSet<Statement>>();
		_o = new HashMap<Nodes, HashSet<Statement>>();

		_sp = new HashMap<Nodes, HashSet<Statement>>();
		_po = new HashMap<Nodes, HashSet<Statement>>();
		_so = new HashMap<Nodes, HashSet<Statement>>();

		_spo = new HashMap<Nodes, HashSet<Statement>>();

		indexPatterns(patterns);
	}
	
	public void indexPatterns(Collection<Statement> patterns){
		for(Statement r:patterns){
			indexPattern(r);
		}
	}

	public void indexPattern(Statement pattern){
		boolean s = !(pattern.subject instanceof Variable);
		boolean p = !(pattern.predicate instanceof Variable);
		boolean o = !(pattern.object instanceof Variable);

		if(!s && !p && !o){
			_empty.add(pattern);
		} else if(!s && !o){
			addPattern(_p, pattern.predicate, pattern);
		} else if(!s && !p){
			addPattern(_o, pattern.object, pattern);
		} else if(!p && !o){
			addPattern(_s, pattern.subject, pattern);
		} else if(!s){
			addPattern(_po, new Nodes(pattern.predicate, pattern.object), pattern);
		} else if(!p){
			addPattern(_so, new Nodes(pattern.subject, pattern.object), pattern);
		} else if(!o){
			addPattern(_sp, new Nodes(pattern.subject, pattern.predicate), pattern);
		} else{
			addPattern(_sp, new Nodes(pattern.subject, pattern.predicate, pattern.object), pattern);
		}
	}
	
	private void addPattern(HashMap<Nodes, HashSet<Statement>> patternindex, Node key, Statement pattern){
		addPattern(patternindex, new Nodes(key), pattern);
	}

	private void addPattern(HashMap<Nodes, HashSet<Statement>> patternindex, Nodes key, Statement pattern){
		HashSet<Statement> patterns = patternindex.get(key);
		if(patterns==null){
			patterns = new HashSet<Statement>(HASHSET_INIT_CAPACITY);
			patternindex.put(key, patterns);
		}
		patterns.add(pattern);
	}

	/**
	 * Get the rules that could possibly be interested in a given statement.
	 * Statement can contain variables.
	 * @param stmt
	 * @return
	 */
	public HashSet<Statement> getRelevantPatterns(Statement stmt){
		HashSet<Statement> ans = new HashSet<Statement>();
		HashSet<Statement> hs = null;
		
		hs = _spo.get(new Nodes(stmt.subject, stmt.predicate, stmt.object));
		if(hs!=null)
			ans.addAll(hs);
		hs = null;


		hs = _po.get(new Nodes(stmt.predicate, stmt.object));
		if(hs!=null)
			ans.addAll(hs);
		hs = null;
		
		hs = _so.get(new Nodes(stmt.subject, stmt.object));
		if(hs!=null)
			ans.addAll(hs);
		hs = null;

		hs = _sp.get(new Nodes(stmt.subject, stmt.predicate));
		if(hs!=null)
			ans.addAll(hs);
		hs = null;

		hs = _s.get(new Nodes(stmt.subject));
		if(hs!=null)
			ans.addAll(hs);
		hs = null;

		hs = _p.get(new Nodes(stmt.predicate));
		if(hs!=null)
			ans.addAll(hs);
		hs = null;

		hs = _o.get(new Nodes(stmt.object));
		if(hs!=null)
			ans.addAll(hs);
		hs = null;

		if(_empty!=null)
			ans.addAll(_empty);

		return ans;
	}
	
	/**
	 * Get the rules that could possibly be interested in a given statement.
	 * Statement can contain variables.
	 * @param stmt
	 * @return
	 */
	public boolean isRelevant(Statement stmt){
		HashSet<Statement> hs = null;
		
		if(_empty!=null && !_empty.isEmpty())
			return true;
		
		hs = _spo.get(new Nodes(stmt.subject, stmt.predicate, stmt.object));
		if(hs!=null && !hs.isEmpty())
			return true;

		hs = _po.get(new Nodes(stmt.predicate, stmt.object));
		if(hs!=null && !hs.isEmpty())
			return true;

		hs = _so.get(new Nodes(stmt.subject, stmt.object));
		if(hs!=null && !hs.isEmpty())
			return true;

		hs = _sp.get(new Nodes(stmt.subject, stmt.predicate));
		if(hs!=null && !hs.isEmpty())
			return true;

		hs = _s.get(new Nodes(stmt.subject));
		if(hs!=null && !hs.isEmpty())
			return true;

		hs = _p.get(new Nodes(stmt.predicate));
		if(hs!=null && !hs.isEmpty())
			return true;

		hs = _o.get(new Nodes(stmt.object));
		if(hs!=null && !hs.isEmpty())
			return true;

		return false;
	}
	
	public HashSet<Statement> getAllPatterns(){
		HashSet<Statement> all = new HashSet<Statement>();
		for(Map.Entry<Nodes,HashSet<Statement>> slr:_s.entrySet()){
			all.addAll(slr.getValue());
		}
		for(Map.Entry<Nodes,HashSet<Statement>> slr:_p.entrySet()){
			all.addAll(slr.getValue());
		}
		for(Map.Entry<Nodes,HashSet<Statement>> slr:_o.entrySet()){
			all.addAll(slr.getValue());
		}
		for(Map.Entry<Nodes,HashSet<Statement>> slr:_sp.entrySet()){
			all.addAll(slr.getValue());
		}
		for(Map.Entry<Nodes,HashSet<Statement>> slr:_po.entrySet()){
			all.addAll(slr.getValue());
		}
		for(Map.Entry<Nodes,HashSet<Statement>> slr:_so.entrySet()){
			all.addAll(slr.getValue());
		}
		for(Map.Entry<Nodes,HashSet<Statement>> slr:_spo.entrySet()){
			all.addAll(slr.getValue());
		}
		all.addAll(_empty);
		
		return all;
	}
}