package org.semanticweb.swse.cons2.master.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.engine.unique.DoubleHashSet;
import org.semanticweb.saorr.engine.unique.UniqueStatementFilter;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.SideCallbackIterator;

/**
 * Abstract class giving default functionality to reasoning primitives
 * which require a specific on-disk join index for A-Box reasoning.
 * @author aidhog
 *
 */
public abstract class OnDiskReasoner implements Callback {
	public static final String CONTEXT_PREFIX = "http://swse.deri.org/cons#";
	
	public static final String INPUT_SUFFIX = "_in";
	public static final String REWRITTEN_SUFFIX = "_rw";
	
	public static final String UNSORTED_SUFFIX = ".nq.gz";
	public static final String SORTED_SUFFIX = ".s.nq.gz";
	
	protected String _dir;
	protected String _current_input_fn;
	protected Callback _current_input;
	protected OutputStream _current_os;
	
	protected InputStream _current_is;
	
	protected String _prefix;

	protected String _last_index = null;
	protected String _delete_index = null;
	protected String _delete_input = null;
	
	protected Resource _context;
	protected String _contextStr;
	
	protected int _added;
	protected int _addedRound;
	protected int _lastcall;

	protected int _round = 0;
	private boolean _closed = false;
	
	protected int _rule;
	
	protected boolean _hasChanged = false;
	
	protected UniqueStatementFilter _usf;
	
	public int getAdded(){
		return _added;
	}
	
	public int getAddedSinceLastRound(){
		return _addedRound;
	}
	
	public int getAddedSinceLastCall(){
		int a = _added - _lastcall;
		_lastcall = _added;
		return a;
	}
	
	public OnDiskReasoner(String dir, String prefix) throws IOException{
		_dir = dir;
		_prefix = prefix;
		
		_usf = new DoubleHashSet();
		
		File f = new File(dir);
		if(!f.exists()){
			f.mkdirs();
		} else if(f.isFile()){
			throw new IOException("Cannot create dir at "+dir+" Already a file!");
		}
		
		_current_input_fn = _dir + _prefix + INPUT_SUFFIX+ _round+ UNSORTED_SUFFIX;
		_current_os = new GZIPOutputStream(new FileOutputStream(_current_input_fn));
		_current_input = new CallbackNxOutputStream(_current_os);
		_context = new Resource(CONTEXT_PREFIX+prefix);
		_contextStr = _context.toString()+"/";
	}
	
	/**
	 * Reset the flag which indicates whether new statements have been added.
	 * @return
	 */
	public boolean resetHasChangedFlag(){
		boolean old = _hasChanged;
		_hasChanged = false;
		return old;
	}
	
	public String getLastIndex(){
		return _last_index;
	}
	
	/**
	 * Check the flag which indicates whether new statements have been added.
	 * @return boolean indicating whether a statement has been added since the
	 * object instantiation or last call to resetHasChangedFlag() method.
	 */
	public boolean hasChanged(){
		return _hasChanged;
	}

	public abstract Iterator<Node[]> performReasoning(Iterator<Node[]> sai) throws IOException, ParseException;

	public void startDocument(){
		;
	}
	
	public void endDocument(){
		;
	}
	
	public void processStatement(Node[] q){
		try{
			addStatement(q);
		} catch(IOException e){
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Add !relevant! statement to reasoner.
	 * No test is performed to check statement is relevant. 
	 * @param q
	 * @throws IOException
	 * @throws ClosedException 
	 */
	public void addStatement(Node[] q) throws IOException{
		if(!unique(q))
			return;
		_added++;
		_addedRound++;
		_hasChanged = true;
		JoinIndexContext rc = new JoinIndexContext(_contextStr, _round);
		Node[] quad = {q[0], q[1], q[2], rc};
		_current_input.processStatement( quad );
	}
	
	public boolean unique(Node[] q){
		Statement s = new Statement(new Node[]{q[0], q[1], q[2]});
		if(_usf.checkSeen(new Statement(q)))
			return false;
		_usf.addSeen(s);
		return true;
	}
	
	/**
	 * Set flag such that no new statements SHOULD be added to reasoner
	 * @return boolean value indicated if previously closed
	 */
	public boolean close(){
		boolean temp = _closed;
		_closed = true;
		return temp;
	}
	
	/**
	 * @return boolean value indicating if statements SHOULD NOT be added
	 */
	public boolean isClosed(){
		return _closed;
	}

	protected Iterator<Node[]> prepareIndex(Iterator<Node[]> sai) throws IOException, ParseException{
		return prepareIndex(sai, null);
	}
	/**
	 * Closes input write stream, and returns a merge-sort iterator over new and old data for input 
	 * @return Iterator<Node[]> used for next iteration
	 * @throws IOException
	 * @throws ParseException
	 */
	protected Iterator<Node[]> prepareIndex(Iterator<Node[]> sai, Resource invContext) throws IOException, ParseException{
		if(_delete_index!=null){
			File f = new File(_delete_index);
			if(f.exists() && f.isFile())
				f.delete();
		}
		if(_delete_index!=null){
			File f = new File(_delete_input);
			if(f.exists() && f.isFile())
				f.delete();
		}
		
		_current_os.close();
		
		_current_is = new GZIPInputStream(new FileInputStream(_current_input_fn));
		//if first round of reasoning
		
		Iterator<Node[]> in = new NxParser(_current_is);
		in = new SortIterator(in);
		
		OutputStream os1 = null;
		
		if(_last_index==null){
			if(sai!=null){
				in = new SortConsolidateIterator(in, sai, invContext);
			}
			
			_last_index = _dir + _prefix + _round+ SORTED_SUFFIX;
			os1 = new GZIPOutputStream(new FileOutputStream(_last_index));
			Callback cb = new CallbackNxOutputStream(os1, true);
			
			in = new SideCallbackIterator(in, cb, true);
			
			_delete_input = _current_input_fn; 
			_current_input_fn = _dir + _prefix + INPUT_SUFFIX+ _round+ UNSORTED_SUFFIX;
			_current_os = new GZIPOutputStream(new FileOutputStream(_current_input_fn));
			_current_input = new CallbackNxOutputStream(_current_os);
			return in;
		} else{
			Iterator<Node[]>[] segs = new Iterator[2];
			segs[0] = in;
			
			InputStream is2 = new GZIPInputStream(new FileInputStream(_last_index));
			Iterator<Node[]> in2 = new NxParser(is2);

			segs[1] = in2;
			in = new MergeSortIterator(segs);
			
			if(sai!=null){
				in = new SortConsolidateIterator(in, sai, _round-1, invContext);
			}
			
			String newindex = _dir + _prefix + _round+ SORTED_SUFFIX;
			os1 = new GZIPOutputStream(new FileOutputStream(newindex));
			Callback cb = new CallbackNxOutputStream(os1, true);
			
			in = new SideCallbackIterator(in, cb, true);
			
			_delete_input = _current_input_fn; 
			_current_input_fn = _dir + _prefix + INPUT_SUFFIX+ _round+ UNSORTED_SUFFIX;
			_current_os = new GZIPOutputStream(new FileOutputStream(_current_input_fn));
			_current_input = new CallbackNxOutputStream(_current_os);
			
			_delete_index = _last_index;
			_last_index = newindex;
			
			return in;
		}
	}
	
	
	public int getRound(){
		return _round;
	}
	
	public static TreeSet<Node> removeOld(TreeSet<Node> neu, TreeSet<Node> old){
		TreeSet<Node> unique = new TreeSet<Node>();
		for(Node n:neu){
			if(!old.contains(n))
				unique.add(n);
		}
		return unique;
	}
	
	public static Hashtable<Node, TreeSet<Node>> removeOld(Hashtable<Node, TreeSet<Node>> neu, Hashtable<Node, TreeSet<Node>> old){
		Hashtable<Node, TreeSet<Node>> unique = new Hashtable<Node, TreeSet<Node>>();
		for(Entry<Node, TreeSet<Node>> e:neu.entrySet()){
			TreeSet<Node> oldns = old.get(e.getKey());
			if(oldns==null){
				unique.put(e.getKey(), e.getValue());
			} else{
				unique.put(e.getKey(), removeOld(e.getValue(), oldns));
			}
		}
		return unique;
	}
}
