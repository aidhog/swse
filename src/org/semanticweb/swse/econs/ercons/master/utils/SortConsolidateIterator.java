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

import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.cli.Main;
import org.semanticweb.yars.nx.mem.MemoryManager;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.nx.reorder.ReorderIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.PleaseCloseTheDoorWhenYouLeaveIterator;

public class SortConsolidateIterator implements Iterator<Node[]>{
	/**
	 * An iterator which takes a sorted stream as input, consolidating
	 * the stream according to the SameAsIndex. Unchanged statements are
	 * written as is to a sorted file. Changed statements are written to
	 * sorted batches. Once the stream has been read, a MergeSortIterator
	 * is opened over the unchanged file and sorted changed batches.
	 */
	//default to rewriting subject and object of a quad
	private Iterator<Node[]> _sai;
	private Iterator<Node[]> _iter;
	
	private String _tempF = null;
	
	static final int[] INV_ORDER = new int[]{2, 1, 0, 3};
	
	static final int S = 2;
	static final int P = 1;
	static final int O = 0;
	static final int C = 3;
	
	static final String SPOC = "0123";
	static final String OPSC = "2103";
	
	static final String BATCH_FILENAME_PREFIX = "batch";
	
	private boolean _checkSameAs = false;
	
	private static final int P_ = 1;

	public SortConsolidateIterator(Iterator<Node[]> iter, Iterator<Node[]> sai) throws ParseException{
		this(iter, sai,  -1, null, false);
	}
	
	public SortConsolidateIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, boolean checkSameAs) throws ParseException{
		this(iter, sai,  -1, null, checkSameAs);
	}
	
	public SortConsolidateIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, Resource invContext) throws ParseException{
		this(iter, sai,  -1, invContext, false);
	}
	
	public SortConsolidateIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, int round) throws ParseException{
		this(iter, sai,  round, null, false);
	}
	
	public SortConsolidateIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, int round, Resource invContext) throws ParseException{
		this(iter, sai,  round, invContext, false);
	}
	
	public SortConsolidateIterator(Iterator<Node[]> iter, Iterator<Node[]> sai, int round, Resource invContext, boolean checkSameAs) throws ParseException{
		_sai = sai;
		_checkSameAs = checkSameAs;
		try {
			init(iter, sai, round, invContext);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public boolean hasNext() {
//		if(!_iter.hasNext() && _tempF!=null){
//			Yars2.deleteTempFolder(new File(_tempF));
//			_tempF = null;
//		}
		return _iter.hasNext();
	}
	
	public Node[] next() {
		return _iter.next();
	}
	
	private Node[] getNextPivot(Node old){
		Node[] line = null;
		
		while(_sai.hasNext()){
			line = _sai.next();
			if(line[0].compareTo(line[2])<=0)
				continue;
			if(old == null || !line[0].equals(old)){
				Node[] nextP = new Node[]{line[0], line[2]};
				return nextP;
			}
		}
		return null;
	}

	public void init(Iterator<Node[]> iter, Iterator<Node[]> sai, int round, Resource invContext) throws IOException, ParseException{
		_tempF = Main.getTempSubDir();
		
		String unc_f = _tempF+"unc"+OnDiskReasoner.SORTED_SUFFIX;
		
		OutputStream os = new GZIPOutputStream(new FileOutputStream(unc_f));
		Callback cb = new CallbackNxOutputStream(os);
		
		int batch = 0;
		TreeSet<Node[]> sorted = new TreeSet<Node[]>(NodeComparator.NC);
		int maxbatch = MemoryManager.estimateMaxStatements(4);
		
		Node[] nextP = getNextPivot(null);
		Node oldin = null;
		int comp = Integer.MAX_VALUE, r=0;
		
		while(iter.hasNext()){
			Node[] next = iter.next();
			Node[] rewritten = new Node[next.length];
			
			System.arraycopy(next, 0, rewritten, 0, next.length);
			
			if(nextP==null)
				comp = Integer.MAX_VALUE;
			else if(oldin == null || !oldin.equals(next[0])){
				oldin = next[0];
				comp = nextP[0].compareTo(next[0]);
				while(comp<0){
					nextP = getNextPivot(nextP[0]);
					if(nextP==null)
						comp = Integer.MAX_VALUE;
					else
						comp = nextP[0].compareTo(next[0]);
				}
			}
			
			if(comp==0 && (!_checkSameAs || !next[P_].equals(OWL.SAMEAS))){
				rewritten[0] = nextP[1];
				r++;
				
				if(round!=-1){
					try{
						JoinIndexContext rc = JoinIndexContext.parse((Resource)rewritten[3]);
						JoinIndexContext rcnew;
						if(JoinIndexContext.toString(rc.getOrder()).equals(SPOC))
							rcnew = new JoinIndexContext(rc.getRoot(), round, 0);
						else
							rcnew = new JoinIndexContext(rc.getRoot(), round, 0, rc.getOrder());
						rewritten[3] = rcnew;
					} catch(Exception e){}
				}

				sorted.add(rewritten);
				if(sorted.size()>=maxbatch){
					OutputStream os2 = new GZIPOutputStream(new FileOutputStream(_tempF+BATCH_FILENAME_PREFIX+batch+OnDiskReasoner.SORTED_SUFFIX));
					Callback bat = new CallbackNxOutputStream(os2);
					for(Node[] na:sorted){
						bat.processStatement(na);
					}
					batch++;
					os2.close();
					sorted = new TreeSet<Node[]>(NodeComparator.NC);
				}
				
				if(invContext!=null){
					Node[] inv = ReorderIterator.reorder(rewritten, INV_ORDER);
					if(rewritten[3] instanceof JoinIndexContext){
						JoinIndexContext jic = (JoinIndexContext)rewritten[3];
						if(JoinIndexContext.toString(jic.getOrder()).equals(OPSC)){
							inv[3] = new JoinIndexContext(jic.getRoot(), jic.getRound());
						} else
							inv[3] = invContext;
					} else{
						try{
							JoinIndexContext jic = JoinIndexContext.parse((Resource)rewritten[3]);
							if(JoinIndexContext.toString(jic.getOrder()).equals(OPSC)){
								inv[3] = new JoinIndexContext(jic.getRoot(), jic.getRound());
							} else
								inv[3] = invContext;
						} catch(Exception e){ inv[3] = invContext; }
					}
					
					sorted.add(inv);
					if(sorted.size()>=maxbatch){
						OutputStream os2 = new GZIPOutputStream(new FileOutputStream(_tempF+BATCH_FILENAME_PREFIX+batch+OnDiskReasoner.SORTED_SUFFIX));
						Callback bat = new CallbackNxOutputStream(os2);
						for(Node[] na:sorted){
							bat.processStatement(na);
						}
						batch++;
						os2.close();
						sorted = new TreeSet<Node[]>(NodeComparator.NC);
					}
				}
			} else{
				cb.processStatement(rewritten);
			}
		}
		
		os.close();
		
		System.out.println("SortConsolidateIterator rewrote "+r+" statements!");
		
		InputStream isunc = new GZIPInputStream(new FileInputStream(unc_f));
		Iterator<Node[]> inunc = new NxParser(isunc);
		inunc = new PleaseCloseTheDoorWhenYouLeaveIterator(inunc, isunc);
		if(batch==0 && sorted.size()==0){
			_iter = inunc;
		} else if(sorted.size()==0){
			Iterator<Node[]>[] iters = new Iterator[batch+1];
			iters[0] = inunc;
			for(int i=0; i<batch; i++){
				InputStream isbat = new GZIPInputStream(new FileInputStream(_tempF+BATCH_FILENAME_PREFIX+i+OnDiskReasoner.SORTED_SUFFIX));
				Iterator<Node[]> inbat = new NxParser(isbat);
				iters[i+1] = new PleaseCloseTheDoorWhenYouLeaveIterator(inbat, isbat);
			}
			_iter = new MergeSortIterator(iters);
		} else{
			Iterator<Node[]>[] iters = new Iterator[batch+2];
			iters[0] = inunc;
			iters[1] = sorted.iterator();
			for(int i=0; i<batch; i++){
				InputStream isbat = new GZIPInputStream(new FileInputStream(_tempF+BATCH_FILENAME_PREFIX+i+OnDiskReasoner.SORTED_SUFFIX));
				Iterator<Node[]> inbat = new NxParser(isbat);
				iters[i+2] = new PleaseCloseTheDoorWhenYouLeaveIterator(inbat, isbat);
			}
			_iter = new MergeSortIterator(iters);
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
}
