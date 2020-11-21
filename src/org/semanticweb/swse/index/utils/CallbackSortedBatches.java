package org.semanticweb.swse.index.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIUtils;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.util.CallbackNxOutputStream;


public class CallbackSortedBatches {
	private ArrayList<String> _batches;
	private String _outdir;
	
	private TreeSet<Node[]> _batch = null;
	
	private static final String BATCH_NAME = "batch.s.nq.gz";
	
	private NodeComparator _nc = NodeComparator.NC;
	
	private int _limit = Integer.MAX_VALUE;
	
	public CallbackSortedBatches(String outdir, int maxbatch){
		_batches = new ArrayList<String>();
		_outdir = outdir;
		_limit = maxbatch;
		RMIUtils.mkdirs(outdir);
	}
	
	public synchronized void processStatements(Set<Node[]> batch) throws FileNotFoundException, IOException{
		if(_batch==null){
			_batch = new TreeSet<Node[]>(_nc);
		}
		_batch.addAll(batch);
		if(_batch.size()>_limit){
			writeBatch();
		}
		
	}
	
	public int getLimit(){
		return _limit;
	}
	
	private void writeBatch() throws FileNotFoundException, IOException{
		if(_batch==null || _batch.isEmpty())
			return;
		String outfile = _outdir+"/"+_batches.size()+BATCH_NAME;
		OutputStream os = new GZIPOutputStream(new FileOutputStream(outfile));
		CallbackNxOutputStream c = new CallbackNxOutputStream(os);
		
		for(Node[] na:_batch){
			c.processStatement(na);
		}
		
		os.close();
		_batch = null;
		_batches.add(outfile);
	}
	
	public ArrayList<String> getBatches() throws FileNotFoundException, IOException{
		writeBatch();
		return _batches;
	}
}
