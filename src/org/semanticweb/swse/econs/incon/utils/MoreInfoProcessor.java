package org.semanticweb.swse.econs.incon.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.engine.unique.UniqueStatementFilter;
import org.semanticweb.saorr.index.StatementStore;
import org.semanticweb.swse.cons.utils.SameAsIndex.SameAsList;
import org.semanticweb.swse.econs.ercons.utils.ConsolidationIterator;
import org.semanticweb.swse.econs.incon.RMIEconsInconServer;
import org.semanticweb.swse.econs.incon.utils.SameAsEvidenceGraph.Partition;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.FOAF;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.reorder.ReorderIterator;

public class MoreInfoProcessor extends UniqueStatementFilter implements Iterator<Node[]>, Callback {
	static Logger _log = Logger.getLogger(MoreInfoProcessor.class.getName());
	
	static Map<Node, SameAsList> SPLIT_PARTITION = new HashMap<Node, SameAsList>();
	
	ConsolidatedDataIterator _csi = null;
	PeekAheadIterator _in = null;
	
	//first triple of new entity
	boolean _newTerm = true;
	
	//store of current entities data
	StatementStore _abox;

	//complete tuple for the last triple returned by next()
	Node[] _prevFull;
	Node[] _prev;
	Statement _prevStmt;
	Node _prevId;

	//ids for triple last returned by next()
	HashSet<Node> _lastIds;
	//ids for "reflexive" triple last returned by next()
	//(need to store two ids here)
	HashSet<Nodes> _lastRefIds;
	//all ids (union of both above)
	HashSet<Node> _lastAllIds;
	
	//ids for entity last returned by next()
	HashSet<Node> _ids;
	
	//explicit sameas ids for entity last returned
	HashSet<Nodes> _sameasIds = new HashSet<Nodes>();
	
	//trivial split
	HashSet<Node> _split = new HashSet<Node>();
	//repair
	HashMap<Node,Map<Node,SameAsList>> _repair = new HashMap<Node,Map<Node,SameAsList>>();
	
	int _aboxLast = 0;
	
	int _skipDupeT = 0;
//	int _skipDupeE = 0;
//	int _skipDupeR = 0;
	int _skipNR = 0;

	boolean _cons = false;
	
	InconLogger _il = null;

	/**
	 * Mapping from triples to IDs
	 */
	Map<Statement,HashSet<Node>> _info = new HashMap<Statement,HashSet<Node>>();
	
	/**
	 * Special mapping for reflexive triples
	 */
	Map<Statement,HashSet<Nodes>> _refinfo = new HashMap<Statement,HashSet<Nodes>>();

	Map<Statement,HashSet<Statement>> _inferences = new HashMap<Statement,HashSet<Statement>>();

	//mapping from IFP and FP triples (using orig IDs) to the input triples which infer them
	Map<Statement,HashSet<Statement>> _ifpExpls = new HashMap<Statement,HashSet<Statement>>();
	Map<Statement,HashSet<Statement>> _fpExpls = new HashMap<Statement,HashSet<Statement>>();
	
	HashSet<Statement> _lastInferences = new HashSet<Statement>();
	
	HashSet<Statement> _lastInput = new HashSet<Statement>();

	HashMap<Node, HashSet<Nodes>> _inlinks = new HashMap<Node, HashSet<Nodes>>();
	HashMap<Node, HashSet<Nodes>> _outlinks = new HashMap<Node, HashSet<Nodes>>();
	
	//ifp and fp properties
	HashSet<Node> _ifps;
	HashSet<Node> _fps;

	public Map<Node, Double> _outpstats;
	public Map<Node, Double> _inpstats;

	public void setInconsistencyLogger(InconLogger il){
		_il = il;
	}
	
	public MoreInfoProcessor(ConsolidatedDataIterator csi, StatementStore abox, HashSet<Node> ifps, HashSet<Node> fps, Map<Node, Double> inpstats, Map<Node, Double> outpstats){
		_csi = csi;
		_in = new PeekAheadIterator(_csi);
		_ifps = ifps;
		_fps = fps;
		_inpstats = inpstats;
		_outpstats = outpstats;
		
		_abox = abox;
		_ids = new HashSet<Node>();
		_lastIds = new HashSet<Node>();
		_lastRefIds = new HashSet<Nodes>();
		_lastAllIds = new HashSet<Node>();
	}

	public boolean hasNext() {
		return _in.hasNext();
	}

	public boolean newTerm(){
		return _newTerm;
	}

	/**
	 * Returns triples in original order with
	 * consolidated identifiers.
	 * 
	 * Serves as ABOX input to reasoner.
	 */
	public Node[] next(){
		Node[] ans = null;
		boolean first = true;
		
		boolean o = false, s = false;
		while(_in.hasNext()){
			Node[] next = _in.next();
			
			if(next[0].equals(new Resource("http://aidanhogan.com/foaf/FredSnr")) && next[1].equals(FOAF.BIRTHDAY)){
				System.err.println(Nodes.toN3(next));
			} else if(next[0].equals(new Resource("http://incon1.com"))){
				System.err.println(Nodes.toN3(next));
			}
			
			if(_prev!=null && !next[0].equals(_prevId)){
//				System.err.println("======"+_prevId+"========");
//				System.err.println(_info);
//				System.err.println(_ids);
//				System.err.println(_inferences);
//				System.err.println(_inlinks);
//				System.err.println(_outlinks);
//				System.err.println(_sameasIds);
				
				_newTerm = true;
				//must be first
				_il.flush();
				
				_abox.clear();
				_info.clear();
				_refinfo.clear();
				_ids.clear();
				_inferences.clear();
				_inlinks.clear();
				_outlinks.clear();
				_sameasIds.clear();
				
				_ifpExpls.clear();
				_fpExpls.clear();
				
			} else{
				_newTerm = false;
			}
			
			if(first){
				_lastIds = new HashSet<Node>();
				_lastRefIds = new HashSet<Nodes>();
				_lastAllIds = new HashSet<Node>();
				_lastInput = new HashSet<Statement>();
				first = false;
			}
			
			boolean isOpsc = next[next.length-1].equals(RMIEconsInconServer.OPSC_SUFFIX);
			
			Node consk = next[0];
			if(!next[next.length-3].equals(ConsolidationIterator.NO_REWRITE)){
				consk = next[next.length-3];
			}
			_lastAllIds.add(consk);
			
			boolean ref = false;
			//if reflexive, find other original identifier
			Node consl = next[2];
			if(next[0].equals(next[2])){
				if(!next[next.length-2].equals(ConsolidationIterator.NO_REWRITE)){
					consl = next[next.length-2];
				}
				if(!consk.equals(consl)){
					ref = true;
					_lastAllIds.add(consl);
				}
			}
			
			
			if(!ref){
				_lastIds.add(consk);
			} else if(!isOpsc){
				Nodes so = new Nodes(consk, consl);
				_lastRefIds.add(so);
				if(next[1].equals(OWL.SAMEAS))
					_sameasIds.add(so);
			}
			
			if(isOpsc){
				o = true;
				if(ans==null){
					ans = ReorderIterator.reorder(next, RMIEconsInconServer.OPS_ORDER);

					markObject(ans);
					_prevStmt = new Statement(ans);
					_prev = ans;
					_prevFull = next;
					_prevId = next[0];
				}
				
				//add inlinks for the original ID
				HashSet<Nodes> links = _inlinks.get(consk);
				if(links==null){
					links = new HashSet<Nodes>();
					_inlinks.put(consk, links);
				}
				links.add(new Nodes(next[1],consl));
				
				_lastInput.add(new Statement(consl, next[1], consk));
			} else{
				s = true;
				if(ans==null){
					ans = new Node[3];
					System.arraycopy(next, 0, ans, 0, ans.length);

					markObject(ans);
					_prevStmt = new Statement(ans);
					_prev = ans;
					_prevFull = next;
					_prevId = next[0];
				}

				//add outlinks for the original ID
				HashSet<Nodes> links = _outlinks.get(consk);
				if(links==null){
					links = new HashSet<Nodes>();
					_outlinks.put(consk, links);
				}
				links.add(new Nodes(next[1],consl));
				
				_lastInput.add(new Statement(consk, next[1], consl));
			}
			
			Node[] future = _in.peek();
			if(future==null ||
					(!next[0].equals(future[0]) || !next[1].equals(future[1])
							|| !next[2].equals(future[2]))){
				break;
			} else{
				_skipDupeT ++;
			}
		}
		
		_ids.addAll(_lastAllIds);
		
		//find other IDs for this triple and
		//load them
		HashSet<Node> nodes = _info.get(_prevStmt);
		if(nodes==null){
			nodes = new HashSet<Node>();
			_info.put(_prevStmt, nodes);
		}
		nodes.addAll(_lastIds);
		
		//handle reflexive IDs
		if(!_lastRefIds.isEmpty()){
			HashSet<Nodes> si = _refinfo.get(_prevStmt);
			if(si==null){
				si = new HashSet<Nodes>();
				_refinfo.put(_prevStmt, si);
			}
			si.addAll(_lastRefIds);
		}
		
		//find previous inferences found for this triple
		//and load them
		_lastInferences = _inferences.get(_prevStmt);
		
		if(_lastInferences==null){
			_lastInferences = new HashSet<Statement>();
			_inferences.put(_prevStmt, _lastInferences);
		} else{
			_log.severe("Inferences for "+_prevStmt+" been seen!");
		}
		
		//if ifp or fp
		if(s && _ifps.contains(_prevStmt.predicate)){
			for(Node n:_lastIds){
				Statement ifp_s = new Statement(n, _prevStmt.predicate, _prevStmt.object);
				add(ifp_s, ifp_s, _ifpExpls);
			}
			if(_lastRefIds!=null) for (Nodes ns:_lastRefIds){
				Statement ifp_s = new Statement(ns.getNodes()[0], _prevStmt.predicate, _prevStmt.object);
				add(ifp_s, ifp_s, _ifpExpls);
			}
		}
		if(o && _fps.contains(_prevStmt.predicate)){
			for(Node n:_lastIds){
				Statement fp_s = new Statement(_prevStmt.subject, _prevStmt.predicate, n);
				add(fp_s, fp_s, _fpExpls);
			}
			if(_lastRefIds!=null) for (Nodes ns:_lastRefIds){
				Statement fp_s = new Statement(_prevStmt.subject, _prevStmt.predicate, ns.getNodes()[1]);
				add(fp_s, fp_s, _fpExpls);
			}
		}
		
		_aboxLast = _abox.size();
		
//		System.err.println("Returning "+Nodes.toN3(ans)+" ids "+_lastIds);
		
		return ans;
	}
	
	private boolean add(Statement s, Statement e, Map<Statement,HashSet<Statement>> map){
		HashSet<Statement> set = map.get(s);
		if(set==null){
			set = new HashSet<Statement>();
			map.put(s,set);
		}
		return set.add(e);
	}
	
	public void processInconsistency(IncompatibleIdentifierGraph iig) {
		int idc = iig.idCount();
		if(idc == 0){
			return;
		} else if(idc == _ids.size() && iig.isStronglyConnected()){
			_repair.put(_prevId, SPLIT_PARTITION);
		} else{
			Partition p = SameAsEvidenceGraph.partition(this, iig);
			_repair.put(_prevId, p._partition);
		}
	}
	
	public Map<Node, Map<Node, SameAsList>> getRepair(){
		return _repair;
	}
	
	/**
	 * Marks objects of reflexive triples for tracking after reasoning
	 * @param na
	 * @return
	 */
	private void markObject(Node[] na){
		if(na[0].equals(na[2])){
			if(na[2] instanceof BNode){
				ObjectBNode ol = new ObjectBNode((BNode)na[2]);
				na[2] = ol;
			} else if(na[2] instanceof Resource){
				ObjectResource or = new ObjectResource((Resource)na[2]);
				na[2] = or;
			}
		}
	}

	public Nodes pair(Node a, Node b){
		if(a.compareTo(b)<=0){
			return new Nodes(a,b);
		}
		return new Nodes(b,a);
	}
	
	public HashSet<Node> getInfo(Statement triple){
		HashSet<Node> ns = _info.get(triple);
		return ns;
	}
	
	public HashSet<Nodes> getReflexiveInfo(Statement triple){
		HashSet<Nodes> ns = _refinfo.get(triple);
		return ns;
	}
	
	public HashSet<Nodes> getSameAsInfo(){
		return _sameasIds;
	}

	public void remove() {
		_in.remove();
	}
	
	public void flush(){
		_il.flush();
	}
	
	public Node getLastId(){
		return _prevId;
	}
	
	public HashSet<Node> getLastIds(){
		return _lastAllIds;
	}

	public void endDocument() {
		;
	}

	public void logStats() {
		_log.info("Literal statements skipped "+_csi.getSkippedLiterals());
		_log.info("Blacklisted statements skipped "+_csi.getSkippedBlacklisted());
		_log.info("Entities skipped "+_csi.getSkippedEntities());
		_log.info("Entities checked "+_csi.getKeptEntities());
		_log.info("Duplicate triples skipped "+_skipDupeT);
//		_log.info("Duplicate reasoning triples skipped "+_skipDupeR);
//		_log.info("Duplicate exact later triples skipped "+_skipDupeE);
		_log.info("Skipped irrelvant "+_skipNR);
	}

	//called for inferred statements (as well as input)
	public void processStatement(Node[] nx) {
		processStatement(new Statement(nx[0], nx[1], nx[2]));
	}
		
	public void processStatement(Statement stmt) {
		if(stmt.subject.equals(new Resource("http://incon1.com")) && stmt.predicate.equals(RDF.TYPE) && stmt.object.equals(FOAF.DOCUMENT)){
			System.err.println(stmt+" -- "+_prevStmt);
		}
		
		_lastInferences.add(stmt);

		HashSet<Node> nodes = _info.get(stmt);
		if(nodes==null){
			nodes = new HashSet<Node>();
			_info.put(stmt, nodes);
		}
		
		boolean se = stmt.subject.equals(_prevId);
		boolean oe = stmt.object.equals(_prevId);
		
		boolean so = stmt.subject instanceof ObjectNode;
		boolean oo = stmt.object instanceof ObjectNode;
		
		//handle symmetric IDs
		if(!_lastRefIds.isEmpty()){
			if(se && oe){
				if((so&&!oo) || (oo&&!so)){
					//if one nodes came from the subject and one from the object position
					HashSet<Nodes> si = _refinfo.get(stmt);
					if(si==null){
						si = new HashSet<Nodes>();
						_refinfo.put(_prevStmt, si);
					}
					
					if(oo&&!so){
						si.addAll(_lastRefIds);
					} else{
						for(Nodes ns:_lastRefIds){
							Node[] na = ns.getNodes();
							si.add(new Nodes(na[1], na[0]));
						}
					}
				} else if(so && oo){
					//if both nodes came from the object position
					for(Nodes ns:_lastRefIds){
						Node[] na = ns.getNodes();
						nodes.add(na[1]);
					}
				} else{
					//if both nodes came from the subject position
					for(Nodes ns:_lastRefIds){
						Node[] na = ns.getNodes();
						nodes.add(na[0]);
					}
				}
			} else if(so || oo){
				//if one came from the object
				for(Nodes ns:_lastRefIds){
					Node[] na = ns.getNodes();
					nodes.add(na[1]);
				}
			} else{
				//if one came from the subject
				for(Nodes ns:_lastRefIds){
					Node[] na = ns.getNodes();
					nodes.add(na[0]);
				}
			}
		} else{
			nodes.addAll(_lastIds);
		}
		
		//if ifp or fp
		if(se && _ifps.contains(stmt.predicate)){
			for(Node n:_lastIds){
				Statement ifp_s = new Statement(n, stmt.predicate, stmt.object);
				for(Statement i:_lastInput) add(ifp_s, i, _ifpExpls);
			}
			//if it comes from a reflexive statement
			//make sure to track the correct id
			if(_lastRefIds!=null) for (Nodes ns:_lastRefIds){
				Statement ifp_s = null;
				if(!so) ifp_s = new Statement(ns.getNodes()[0], stmt.predicate, stmt.object);
				else ifp_s = new Statement(ns.getNodes()[1], stmt.predicate, stmt.object);
				for(Statement i:_lastInput) add(ifp_s, i, _ifpExpls);
			}
		}
		if(oe && _fps.contains(stmt.predicate)){
			for(Node n:_lastIds){
				Statement fp_s = new Statement(stmt.subject, stmt.predicate, n);
				for(Statement i:_lastInput) add(fp_s, i, _fpExpls);
			}
			if(_lastRefIds!=null) for (Nodes ns:_lastRefIds){
				Statement fp_s = null;
				if(!so) fp_s = new Statement(ns.getNodes()[1], stmt.predicate, stmt.object);
				else fp_s = new Statement(ns.getNodes()[0], stmt.predicate, stmt.object);
				for(Statement i:_lastInput) add(fp_s, i, _fpExpls);
			}
		}
	}

	public void startDocument() {
		;
	}

	public boolean addSeen(Statement s) {
		return checkSeen(s);
	}

	public boolean checkSeen(Statement s) {
		if(s.subject.equals(new Resource("http://incon1.com")) && s.predicate.equals(RDF.TYPE) && s.object.equals(FOAF.DOCUMENT)){
			System.err.println(s+" -- "+_prevStmt);
		}
		
		//skip inferences not involving current entity
		if(_prev!=null && !s.subject.equals(_prev[0]) && !s.object.equals(_prev[0])){
			_skipNR++;
			return true;
		} else if(!_lastInferences.add(s)){
			return true;
		} 
		//good idea but buggy
//		else if(_lastRefIds.isEmpty()){ //otherwise lose marking of subject/object
//			HashSet<Statement> inferences = _inferences.get(s);
//			if(inferences!=null && inferences.size()>1){
//				for(Statement stmt:inferences){
//					processStatement(stmt);
//				}
//				
//				//skip reasoning for that statement
//				return true;
//			}
//		}
		return false;
	}

	public void clear() {
		;
	}

	public boolean remove(Statement s) {
		return false;
	}

	public int size() {
		return 0;
	}
	
	public static interface ObjectNode{
		
	}
	
	public static class ObjectResource extends Resource implements ObjectNode{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		public ObjectResource(Resource r){
			super(r.toN3().substring(1, r.toN3().length()-1));
		}
	}
	
	public static class ObjectBNode extends BNode implements ObjectNode{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		public ObjectBNode(BNode b){
			super(b.toN3().substring(BNode.PREFIX.length()));
		}
	}
}
