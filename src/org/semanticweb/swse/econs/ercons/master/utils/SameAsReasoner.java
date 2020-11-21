package org.semanticweb.swse.econs.ercons.master.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.econs.ercons.master.utils.SameAsIndex2.IndexFullException;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.mem.MemoryManager;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.nx.reorder.ReorderIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.PleaseCloseTheDoorWhenYouLeaveIterator;

/**
 * A not so simple class for performing SymmetricTransitive reasoning
 * using on-disk and accelerated using best fit in-memory
 * reasoning
 * @author aidhog
 */
public class SameAsReasoner extends OnDiskReasoner {
	
	public static final String PREFIX = "sameas";
	
	private String _last = null;
	
	public static final String ROUND_SUFFIX = "round#";
	public static final String ITER_SUFFIX = "iter#";
	
	/**
	 * Order of inverse statements for performing reasoning. Must be OP*. Do not change.
	 */
	static final int[] INV_ORDER = { 2, 1, 0, 3 };
	
	static final int S = 2;
	static final int P = 1;
	static final int O = 0;
	static final int C = 3;
	
	private static final String SEGMENT_SUFFIX = "_seg";
	private static final String UNIQUED_SUFFIX = "_u";

	private JoinIndexContext _con = new JoinIndexContext(_contextStr, _round, 0);
	
	private int _iter = 0;
	
	private boolean _reasoned = false;
	
	public SameAsReasoner(String dir) throws IOException{
		super(dir, PREFIX);
	}
	
	/**
	 * Add transitive property statement to reasoner
	 * @param q
	 * @throws IOException
	 */
	public void addStatement(Node[] q) throws IOException{
		if(!unique(q))
			return;
		_addedRound++;
		_added++;
		int comp = q[0].compareTo(q[2]);
		q[3] = _con;
		if(comp == 0)
			return;
		else{
			_hasChanged = true;
			_current_input.processStatement(q);
			
			Node[] inv = {q[2], q[1], q[0], q[3]};
			_current_input.processStatement(inv);
		}
	}
	
	public String getSameAsIndex() throws IOException, ParseException{
		if(!_reasoned || _hasChanged){
			performReasoning();
		} 
		return _last;
	}
	
	public void performReasoning() throws IOException, ParseException{
		_reasoned = true;
		System.err.println("==Starting sameas transitive reasoning==");
		performReasoning(null, true);
		System.err.println("==Finished sameas transitive reasoning==");
		_last = _last_index;
		_hasChanged = false;
	}
	
	public String performReasoning(Iterator<Node[]> sai, boolean nothing) throws IOException, ParseException{
		System.err.println("Starting transitive reasoning...");
		_addedRound=0;
		_round++;
		
		Iterator<Node[]> in = prepareIndex(null, _con);
		InputStream is = _current_is;
		
		boolean done = false;

		for(_iter=1; done==false; _iter++){
			String segname = _dir+PREFIX+_round+"."+_iter+SEGMENT_SUFFIX+UNSORTED_SUFFIX;
			System.err.println("Performing iteration "+_iter+"...");
			
			OutputStream os = new GZIPOutputStream(new FileOutputStream(segname));
			Callback out = new CallbackNxOutputStream(os);
			int stmts = performIteration(in, out, _iter);
			if(stmts<=0){
				stmts *= -1;
				if(_iter==1){
					System.err.println("Completed entire closure in-memory.");
					done = true;
				}
			}
			is.close();
			os.close();
			System.err.println("... iteration done. Wrote "+stmts+" stmts total and their inverses.");

			System.err.println("Sorting output segment from iteration "+_iter+"...");
			String sortedsegname = _dir+PREFIX+_round+"."+_iter+SEGMENT_SUFFIX+SORTED_SUFFIX;
			
			is = new GZIPInputStream(new FileInputStream(segname));
			in = new NxParser(is);
			
			SortIterator si = new SortIterator(in);
			
			os = new GZIPOutputStream(new FileOutputStream(sortedsegname));
			out = new CallbackNxOutputStream(os);
			
			while(si.hasNext()){
				out.processStatement(si.next());
			}
			os.close();
			is.close();
			
			System.err.println("... sorting done.");

			System.err.println("Merging segment into main file...");
			String sortedname = _dir+PREFIX+_round+"."+_iter+SORTED_SUFFIX;
			
			is = new GZIPInputStream(new FileInputStream(sortedsegname));
			in = new NxParser(is);
			
			InputStream is2 = new GZIPInputStream(new FileInputStream(_last_index));
			NxParser in2 = new NxParser(is2);
			
			os = new GZIPOutputStream(new FileOutputStream(sortedname));
			Callback merged = new CallbackNxOutputStream(os);
			
			Iterator<Node[]>[] iters = new Iterator[2];
			
			iters[0] = in;
			iters[1] = in2;
			
			MergeSortIterator msi = new MergeSortIterator(iters);
			
			while(msi.hasNext()){
				merged.processStatement(msi.next());
			}
			
			is.close();
			is2.close();
			os.close();
			System.err.println("... merging done");

			
			String uniqued = _dir+PREFIX+_round+"."+_iter+UNIQUED_SUFFIX+SORTED_SUFFIX;
			System.err.println("Removing old and non-pivotal statements, writing to "+uniqued+" ...");
			
			is = new GZIPInputStream(new FileInputStream(sortedname));
			in = new NxParser(is);
			
			os = new GZIPOutputStream(new FileOutputStream(uniqued));
			out = new CallbackNxOutputStream(os);
			
			int[] c = removeNonPivotal(in, out, new JoinIndexContext(_contextStr, _round-1, _iter));
			
			os.close();
			is.close();
			
			System.err.println("... removal done. Removed "+c[1]+" old and non-pivotal stmts and "+c[2]+" duplicate statements.");
		
			System.err.println("Iteration "+_iter+" complete!");
			System.err.println("Added "+c[0]+" new unique statements.");
			if(c[0]==0 || done){
				done = true;
				System.err.println("Closure computed!");
			}
			else{
				is = new GZIPInputStream(new FileInputStream(uniqued));
				in = new NxParser(is);
			}
			_last_index = uniqued;
		}
		_con = new JoinIndexContext(_contextStr, _round, 0);
		return _last_index;
	}
	
	public RewriteIterator performReasoning(Iterator<Node[]> sai) throws IOException, ParseException{
		String out = performReasoning(sai, true);
		InputStream is = new GZIPInputStream(new FileInputStream(out));
		NxParser nxp = new NxParser(is);
		
		return new RewriteIterator(new PleaseCloseTheDoorWhenYouLeaveIterator(nxp, is));
	}

	private int performIteration(Iterator<Node[]> in, Callback cb, int iter) throws IOException{
		int maxnodes = MemoryManager.estimateMaxNodes();
		System.err.println("Initialising symtransitive in-memory index with limit of "+maxnodes+" nodes.");
		SameAsIndex2 sai = new SameAsIndex2(maxnodes);
		boolean full = false;

		int stmts = 0;

		Node oldSub = null;
		TreeSet<Node> old = new TreeSet<Node>();

		TreeSet<Node> neu = new TreeSet<Node>();
		Node[] line = null;
		
		JoinIndexContext con = new JoinIndexContext(_contextStr, _round-1, iter);
		
		boolean done = !in.hasNext();
		while(!done){
			done = !in.hasNext();
			if(!done){
				line = in.next();
			}

//			if(line[0].equals(new BNode("C"))){
//				System.currentTimeMillis();
//			}
//			System.err.println(Nodes.toN3(line));
			
			if(oldSub!=null && (!oldSub.equals(line[0]) || done)){
				//if cannot fit anymore elements in in-mem SAI
				if(full && neu.size()>0){
					TreeSet<Node> ini = new TreeSet<Node>();
					TreeSet<Node> neuold = new TreeSet<Node>();
					TreeSet<Node> neuneu = new TreeSet<Node>();
					
					Node pivot = null;
					
					//find elements already in SAI, not in SAI
					for(Node n:neu){
						TreeSet<Node> chain = sai.getEquivalents(n);
						if(chain!=null)
							ini.add(chain.first());
						else neuneu.add(n);
					}
					
					for(Node n:old){
						TreeSet<Node> chain = sai.getEquivalents(n);
						if(chain!=null)
							ini.add(chain.first());
						else neuold.add(n);
					}
					
					TreeSet<Node> chain = sai.getEquivalents(oldSub);
					if(chain!=null)
						ini.add(chain.first());
					
					Node ip = null;
					
					//merge elements already in SAI together (not adding any new nodes)
					if(ini.size()>1){
						try{
							ip = sai.handleSameAsList(ini).getPivot();
						} catch(IndexFullException ife){
							;
						}
					} else if(ini.size()==1){
						ip = ini.first();
					}
					
					//only consider those not in SAI
					neu = neuneu;
					old = neuold;
					
					if(neu.size()>0 || old.size()>0){
						pivot = getPivot(neu, old);
						if(oldSub.compareTo(pivot)<0)
							pivot = oldSub;
						
						//get pivot = highest element of oldSub, inlinks and outlinks
						
						//if found new pivot for elements in SAI
						if(ip!=null && pivot.compareTo(ip)<0){
							//remove second node
							//(not a pivot, but should be past) 
							Node remove = null;
							TreeSet<Node> oldchain = sai.getEquivalents(ip);
							Iterator<Node> iteroc = oldchain.iterator();
							remove = iteroc.next();
							remove = iteroc.next();
							
							sai.forceNewPivot(remove, pivot);
							
							Node[] infer = new Node[4];
							infer[0] = remove;
							infer[1] = OWL.SAMEAS;
							infer[2] = pivot;
							infer[3] = con;
							cb.processStatement(infer);

							Node[] inv = getSymmetricQuad(infer);
							cb.processStatement(inv);
	
							stmts++;
						}
						//if found new pivot in SAI for current position
						else if(ip!=null && !pivot.equals(ip)){ 
							pivot = ip;
							
							if(!pivot.equals(oldSub)){
								Node[] infer = new Node[4];
								infer[0] = oldSub;
								infer[1] = OWL.SAMEAS;
								infer[2] = pivot;
								infer[3] = con;
								cb.processStatement(infer);
								Node[] inv = getSymmetricQuad(infer);
								cb.processStatement(inv);
							}
						}
					}
					
					Node[] infer = new Node[4];

					if(!oldSub.equals(pivot)){
						for(Node o:old){
							if(o.equals(pivot))
								continue;
	
							infer[0] = o;
							infer[1] = OWL.SAMEAS;
							infer[2] = pivot;
							infer[3] = con;
							cb.processStatement(infer);
							Node[] inv = getSymmetricQuad(infer);
							cb.processStatement(inv);
		
							stmts++;
						}
						
						for(Node i:neu){
							if(i.equals(pivot))
								continue;
							
							infer[0] = i;
							infer[1] = OWL.SAMEAS;
							infer[2] = pivot;
							infer[3] = con;
							cb.processStatement(infer);
							Node[] inv = getSymmetricQuad(infer);
							cb.processStatement(inv);
	
							stmts++;
						}
					}
				}
				else if(!full){
					Node pivot = getPivot(old, neu);
					if(oldSub.compareTo(pivot)<=0){
						pivot = oldSub;
						
						//even if no new stuff is added, if we are in a set of pivot
						//statements and something is found in the SameAsIndex, then a
						//new pivot has appeared earlier... must add all old equivalences
						//to attach to new pivot
						if(neu.size()==0){
							boolean add = false;
							for(Node n:old){
								TreeSet<Node> sal = sai.getEquivalents(n);
								if(sal!=null){
									add = true; 
									break;
								}
							}
							if(add){
								for(Node n:old){
									try {
										//if fills here, will push through next three anyways
										sai.handleSameAsPair(pivot, n);
									} catch (IndexFullException e) {
										if(!full)
											System.err.println("In memory index full... Finishing iteration via disk scan.");
										full = true;
									}
								}
							}
						}
					} 
					if((neu.size()>0)){
						if(!pivot.equals(oldSub)){
							try {
								//if fills here, will push through next three anyways
								sai.handleSameAsPair(pivot, oldSub);
							} catch (IndexFullException e) {
								if(!full)
									System.err.println("In memory index full... Finishing iteration via disk scan.");
								full = true;
							}
						}
						
						for(Node o:old){
							if(o.equals(pivot))
								continue;
	
							try {
								sai.handleSameAsPair(pivot, o);
							} catch (IndexFullException e) {
								if(!full)
									System.err.println("In memory index full... Finishing iteration via disk scan.");
								full = true;
							}
							
						}
						
						for(Node i:neu){
							if(i.equals(pivot))
								continue;
							try {
								sai.handleSameAsPair(pivot, i);
							} catch (IndexFullException e) {
								if(!full)
									System.err.println("In memory index full... Finishing iteration via disk scan.");
								full = true;
							}
						}
					}
				}
				
				old = new TreeSet<Node>();
				neu = new TreeSet<Node>();
			}

			oldSub = line[0];

			if(!done){
				JoinIndexContext context = JoinIndexContext.parse((Resource)line[C]);
				if(context==null){
					System.err.println(Nodes.toN3(line));
				}
				//System.out.println("====="+Nodes.toN3(line)+" "+_round+" "+iter);
				if((iter==1 || context.getIteration() == (iter-1)) 
						&& (_round == 1 || context.getRound()==_round-1)){
					neu.add(line[2]);
				}
				else 
					old.add(line[2]);
			}
		}

		stmts+= sai.writeToFile(cb, con);
		
		if(full)
			return stmts;
		else
			return stmts*-1;
	}
	
	private static Node[] getSymmetricQuad(Node[] q){
		if(q.length<3){
			return q;
		}
		
		Node[] inv = new Node[q.length];
		System.arraycopy(q, 0, inv, 0, q.length);
		
		inv[0] = q[2];
		inv[2] = q[0];
		
		return inv;
	}
	
	private Node getPivot(TreeSet<Node> one, TreeSet<Node> two){
		if(one==null || one.size()==0){
			if(two==null || two.size()==0)
				return null;
			else
				return two.first();
		} else if(two==null || two.size()==0){
			return one.first(); 
		} else{
			Node of = one.first();
			Node tf = two.first();
			int comp = of.compareTo(tf);
			if(comp==0){
				return of;
			} else if(comp>0){
				return tf;
			} else {
				return of;
			}
		}
	}

	private int[] removeNonPivotal(Iterator<Node[]> in, Callback cb, Resource current) throws IOException{
		int[] c = new int[3];

		Node[] old = null;
		int writeAll = 0;
		while(in.hasNext()){
			Node[] line = in.next();
//			if(line[0].equals(new Resource("http://sw.deri.org/~aidanh/foaf/foaf.rdf#Aidan_Hogan"))){
//				System.err.println(Nodes.toN3(line));
//			}
			if(old==null || !NodeComparator.NC_VAR.equals(old, line)){
				
				if(old==null || !old[0].equals(line[0])){
					if(line[0].compareTo(line[2])<=0)
						writeAll = 1;
					else{
						writeAll = 2;
					}
				}
				
				if(line[3].equals(current)){
					c[0]++;
					cb.processStatement(line);
					if(writeAll==2)
						writeAll=0;
				} else if(writeAll==1){
					cb.processStatement(line);
				} else if(writeAll==2){
					cb.processStatement(line);
					writeAll = 0;
				} else c[1]++;
				
				
				old = new Node[3];
				for(int i=0; i<old.length; i++)
					old[i] = line[i];
			} else c[2]++;
		}

		return c;
	}
	
	public int writeFinalOutput(Callback cb) throws IOException, ParseException{
		return writeFinalOutput(cb, null);
	}
	
	public int writeFinalOutput(Callback cb, int[] reorder) throws IOException, ParseException{
		InputStream is = new GZIPInputStream(new FileInputStream(_last_index));
		NxParser in = new NxParser(is);
		
		Node old = null;
		boolean writeall = true;
		int write = 0;
		while(in.hasNext()){
			Node[] line = in.next();
			if(old==null || !line[0].equals(old)){
				int comp = line[0].compareTo(line[2]);
				if(comp==0)
					continue;
				else if(comp < 0 )
					writeall = true;
				else if(comp > 0){
					writeall = false;
					Node[] n = { line[0], line[1], line[2], _context };
					if(reorder!=null)
						n = ReorderIterator.reorder(n, reorder);
					cb.processStatement(n);
					write++;
				}
				old = line[0];
			}
			
			if(writeall){
				Node[] n = { line[0], line[1], line[2], _context };
				if(reorder!=null)
					n = ReorderIterator.reorder(n, reorder);
				cb.processStatement(n);
			}
			
		}
		
		return write;
	}

	public class SymTransitiveResultsIterator implements Iterator<Node[]> {
		private Node[] _current;
		private Iterator<Node[]> _in;
		
		public SymTransitiveResultsIterator(Iterator<Node[]> in){
			_in = in;
			getNext();
		}
		
		public boolean hasNext() {
			return _current!=null;
		}

		public Node[] next() {
			Node[] current = new Node[_current.length];
			System.arraycopy(_current, 0, current, 0, _current.length);
			getNext();
			return current;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		public void getNext(){
			_current = null;
			Node[] line;
			while(_in.hasNext()){
				line = _in.next();
				JoinIndexContext jic = JoinIndexContext.parse((Resource)line[C]);
				if(jic.getRound()==_round-1
						&& jic.getIteration()>0){
					Node[] n = { line[0], line[1], line[2], _context };
					_current = n;
					return;
				}
			}
		}
	}
	
	public class RewriteIterator implements Iterator<Node[]> {
		private Node[] _current;
		private Iterator<Node[]> _in;
		
		public RewriteIterator(Iterator<Node[]> in){
			_in = in;
			getNext();
		}
		
		public boolean hasNext() {
			return _current!=null;
		}

		public Node[] next() {
			Node[] current = new Node[_current.length];
			System.arraycopy(_current, 0, current, 0, _current.length);
			getNext();
			return current;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
		
		public void getNext(){
			Node[] line = null;
			Node old = null;
			
			if(_current!=null){
				old = _current[0];
			}
			
			_current = null;
			
			while(_in.hasNext()){
				line = _in.next();
				if(line[0].compareTo(line[2])<=0)
					continue;
				if(old == null || !line[0].equals(old)){
					_current = line;
					return;
				}
			}
		}
	}
}
