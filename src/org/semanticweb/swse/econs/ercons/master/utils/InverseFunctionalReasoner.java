package org.semanticweb.swse.econs.ercons.master.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.nx.reorder.ReorderIterator;

/**
 * Class for performing Reasoning over InverseFunctionalProperty 
 * instances using an on-disk index
 * @author aidhog
 */
public class InverseFunctionalReasoner extends OnDiskReasoner {
	/**
	 * Order of quad index for performing reasoning. Must be OPSC
	 */
	private static final int[] ORDER = {2, 1, 0, 3};
	private static final int P = 1;
	private static final int O = 0;
	private static final int C = 3;
	private static final int S = 2;
	
	public static final String PREFIX = "ifp";
	/**
	 * 
	 * @param dir - Directory for join indices for each iteration
	 * @param sai - SameAsIndex to be populated by reasoner and to be used for 
	 * rewriting input
	 * @throws IOException
	 */
	public InverseFunctionalReasoner(String dir) throws IOException{
		super(dir, PREFIX);
	}
	
	/**
	 * Add inverse functional property statement to reasoner
	 * @param q
	 * @throws IOException
	 */
	public void addStatement(Node[] q) throws IOException{
		if(!unique(q))
			return;
		_hasChanged = true;
		_addedRound++;
		_added++;
		JoinIndexContext rc = new JoinIndexContext(_contextStr+"/", _round, ORDER);
		Node[] quad = {q[0], q[1], q[2], rc};
		_current_input.processStatement( reorder(quad) );
	}
	
	/**
	 * Perform iteration on statements added through use of addStatement method
	 * @return null
	 * @throws IOException
	 * @throws ParseException
	 */
	public Iterator<Node[]> performReasoning(Iterator<Node[]> sai) throws IOException, ParseException{
		_round++;
		_addedRound = 0;
		Iterator<Node[]>in = prepareIndex(sai);
		return new IFPIterator(in);
	}
	
	private Node[] reorder(Node[] in){
		Node[] out = ReorderIterator.reorder(in, ORDER);
		return out;
	}
	
	public class IFPIterator implements Iterator<Node[]> {
		private Iterator<Node[]> _current;
		private Iterator<Node[]> _in;
		private Node[] _lastRead = null;
		
		public IFPIterator(Iterator<Node[]> in){
			_in = in;
			getNextBatch();
		}
		
		public boolean hasNext() {
			return _current!=null && _current.hasNext();
		}

		public Node[] next() {
			Node[] current = _current.next();
			if(!_current.hasNext()){
				getNextBatch();
			}
			return current;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		public void getNextBatch(){
			if(!_in.hasNext())
				return;
			
			TreeSet<Node[]> stmts = new TreeSet<Node[]>(NodeComparator.NC);
			
			Node oldpred = null, oldobj = null;
			TreeSet<Node> sameas = new TreeSet<Node>();
			TreeSet<Node> newsameas = new TreeSet<Node>();
			Node[] line = null;
				
			boolean done = false;
			while(!done){
				if(_lastRead!=null){
					line = _lastRead;
					_lastRead=null;
				}
				else if(_in.hasNext()){
					line = _in.next();
				} else done = true;
				
				if(oldpred == null)
					oldpred = line[P];
				if(oldobj == null)
					oldobj = line[O];
				
				if(!oldpred.equals(line[P]) || !oldobj.equals(line[O]) || done){
					newsameas = removeOld(newsameas, sameas);
					if((sameas.size()+newsameas.size())>1 && newsameas.size()>0){
						Iterator<Node> iter = newsameas.iterator();
						Node first = iter.next();
						
						while(iter.hasNext()){
							Node[] sa = {first, OWL.SAMEAS, iter.next(), _context};
							stmts.add(sa);
						}
						
						iter = sameas.iterator();
						while(iter.hasNext()){
							Node[] sa = {first, OWL.SAMEAS, iter.next(), _context};
							stmts.add(sa);
						}
						
						_current = stmts.iterator();
						_lastRead = line;
						return;
					}
					sameas = new TreeSet<Node>();
					newsameas = new TreeSet<Node>();
				}
				
				JoinIndexContext jic = JoinIndexContext.parse((Resource)line[C]);
				
				if(jic.getRound()==_round-1){
					newsameas.add(line[S]);
				}else{
					sameas.add(line[S]);
				}
				oldpred = line[P];
				oldobj = line[O];
			}
		}
	}
}
