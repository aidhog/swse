package org.semanticweb.swse.econs.ercons.master.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.ParseException;

/**
 * Class for performing Reasoning over FunctionalProperty 
 * instances using an on-disk index
 * @author aidhog
 */
public class FunctionalReasoner extends OnDiskReasoner {
	public static final String PREFIX = "fp";
	private boolean _subjLits = false;
	
	/**
	 * 
	 * @param dir - Directory for join indices for each iteration
	 * @param sai - SameAsIndex to be populated by reasoner and to be used for 
	 * rewriting input
	 * @throws IOException
	 */
	public FunctionalReasoner(String dir, boolean considerSubjLiterals) throws IOException{
		super(dir, PREFIX);
		_subjLits = considerSubjLiterals;
	}
	
	/**
	 * Perform iteration on statements added through use of addStatement method
	 * @return null
	 * @throws IOException
	 * @throws ParseException
	 */
	public Iterator<Node[]> performReasoning(Iterator<Node[]> sai) throws IOException, ParseException{
		_round++;
		_addedRound=0;
		
		Iterator<Node[]>in = prepareIndex(sai);
		return new FPIterator(in);
	}
	
	public class FPIterator implements Iterator<Node[]> {
		private Iterator<Node[]> _current;
		private Iterator<Node[]> _in;
		private Node[] _lastRead = null;
		
		public FPIterator(Iterator<Node[]> in){
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
			
			Node oldsubj = null, oldpred = null;
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
				
				if(oldsubj == null)
					oldsubj = line[0];
				if(oldpred == null)
					oldpred = line[1];
				
				if(!oldsubj.equals(line[0]) || !oldpred.equals(line[1]) || done){
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
				
				JoinIndexContext jic = JoinIndexContext.parse((Resource)line[3]);
				
				if(_subjLits || !(line[2] instanceof Literal)){
					if(jic.getRound()==_round-1){
						newsameas.add(line[2]);
					}else{
						sameas.add(line[2]);
					}
				}
				oldsubj = line[0];
				oldpred = line[1];
			}
		}
		
	}
}
