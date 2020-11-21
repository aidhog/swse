package org.semanticweb.swse.ldspider.remote.queue;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.NodeComparator.NodeComparatorArgs;
import org.semanticweb.yars.nx.cli.Main;
import org.semanticweb.yars.nx.mem.MemoryManager;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.ontologycentral.ldspider.tld.TldManager;

public class SimpleLinkStore {
	private static final Logger _log = Logger.getLogger(DiskQueue.class.getSimpleName());
	
	private static final String U_ORDERED_FN = "links.u";
	private static final String C_ORDERED_FN = "links.c";
	
	private static final String SORTED_SUFFIX = ".s.nq.gz";
	private static final String UNSORTED_SUFFIX = ".nq.gz";
	
	//higher values equals less memory for sorting
	private static final int MEMORY_FACTOR = 4;
	
	private static final NodeComparatorArgs NODE_SORTING_NCA = new NodeComparatorArgs();
	static{
		NODE_SORTING_NCA.setNumeric(new boolean[]{false, true});
		NODE_SORTING_NCA.setNoZero(true);
		NODE_SORTING_NCA.setNoEquals(true);
		NODE_SORTING_NCA.setOrder(new int[]{0,1});
	}
	
	private static final NodeComparator NODE_SORTING_NC = new NodeComparator(NODE_SORTING_NCA);
	
	private static final NodeComparatorArgs COUNT_SORTING_NCA = new NodeComparatorArgs();
	static{
		COUNT_SORTING_NCA.setNumeric(new boolean[]{false, true});
		COUNT_SORTING_NCA.setNoZero(true);
		COUNT_SORTING_NCA.setNoEquals(true);
		COUNT_SORTING_NCA.setOrder(new int[]{1,0});
		COUNT_SORTING_NCA.setReverse(new boolean[]{false, true});
	}
	
	private static final NodeComparator COUNT_SORTING_NC = new NodeComparator(COUNT_SORTING_NCA);
	
	String _qdir;
	String _tmpDir;
	
	int _round = 0;
	
	Callback _current = null;
	OutputStream _currentOS = null;
	String _lastSortedFN = null;
	
	String _currentFN = null;
	
	public SimpleLinkStore(String qdir){
		_log.info("Creating link store at "+qdir);
		_qdir = qdir;
		_tmpDir = _qdir+"/tmp/";
	}
	
	public synchronized void addLink(URI link) throws FileNotFoundException, IOException{
		addLink(link, 1);
	}
	
	public synchronized void addLink(URI link, int count) throws FileNotFoundException, IOException{
		if(_current==null){
			initialiseCurrentCallback();
		}
		Node[] ci = new Node[]{new Resource(link.toASCIIString()), new Literal(Integer.toString(count)) };
		_current.processStatement(ci);
	}
	
	public synchronized void initNextRound() throws IOException, ParseException{
		initNextRound(null);
	}
	
	public synchronized void initNextRound(Set<String> crawledprev) throws IOException, ParseException{
		initNextRound(crawledprev, false);
	}
	
	public synchronized void initNextRound(Set<String> crawledprev, boolean done) throws IOException, ParseException{
		_log.info("Pre-initialising link store for round "+_round);
		organise(crawledprev);
		_round++;
		
		if(!done)
			initialiseCurrentCallback();
	}
	
	/**@TODO Fix incase URIs not added for one round
	 * 
	 * @param q
	 * @param tldm
	 * @throws IOException
	 * @throws ParseException
	 */ 
	public synchronized void schedule(TempQueue q, TldManager tldm) throws IOException, ParseException{
		_log.info("Scheduling on-disk link store for round "+_round);
		
		if(_lastSortedFN==null){
			return;
		}
		
		InputStream is = new GZIPInputStream(new FileInputStream(_lastSortedFN));
		NxParser nxp = new NxParser(is);
		
		SortArgs sa = new SortArgs(nxp);
		sa.setComparator(COUNT_SORTING_NC);
		sa.setLinesPerBatch(MemoryManager.estimateMaxStatements(MEMORY_FACTOR));
		sa.setTmpDir(Main.getTempSubDir(_tmpDir));
		
		_log.info("Sorting links by indegree...");
		SortIterator si = new SortIterator(sa);
		_log.info("...links sorted by indegree.");
		
		String sorted_c =  createSortedCountFile(_round, _qdir);
		
		//make sorted by count persistent for the craic
		OutputStream os = new GZIPOutputStream(new FileOutputStream(sorted_c));
		Callback cb = new CallbackNxOutputStream(os);
		
		int count = 0;
		
		_log.info("Creating in-memory queue...");
		while(si.hasNext()){
			
			Node[] lc = si.next();
			
			int c = Integer.parseInt(lc[1].toString());
			
			if(c<0)
				break;
			
			count++;
			
			cb.processStatement(lc);
			try{
				URI u = new URI(lc[0].toString());
				String pld = tldm.getPLD(u);
				q.tryAdd(u, pld, c);
			} catch(URISyntaxException e){
				_log.fine("Error parsing URI "+lc[0].toString()+":\n"+e.getMessage());
			}
		}
		_log.info("...created in-memory queue.");
		
		is.close();
		os.close();
		
		_log.info("...scheduling link store from disk finished... still "+count+" active URIs to crawl.");
	}
	
	private void initialiseCurrentCallback() throws FileNotFoundException, IOException {
		_currentFN = createLinkFile(_round, _qdir);
		_currentOS = new GZIPOutputStream(new FileOutputStream(_currentFN));
		_current = new CallbackNxOutputStream(_currentOS);
	}
	
	private void organise(Set<String> crawled) throws IOException, ParseException{
		_log.info("Aggregating links in on-disk store.");
		
		if(_currentOS==null)
			return;
		
		_currentOS.close();
		
		_log.info("Sorting new links...");
		InputStream is = new GZIPInputStream(new FileInputStream(_currentFN));
		NxParser nxp = new NxParser(is);
		SortArgs sa = new SortArgs(nxp);
		sa.setComparator(NODE_SORTING_NC);
		sa.setLinesPerBatch(MemoryManager.estimateMaxStatements(MEMORY_FACTOR));
		sa.setTmpDir(Main.getTempSubDir(_tmpDir));
		
		String oldsorted = _lastSortedFN;
		_lastSortedFN = createSortedLinkFile(_round, _qdir);
		OutputStream os = new GZIPOutputStream(new FileOutputStream(_lastSortedFN));
		Callback cb = new CallbackNxOutputStream(os);
		
		Iterator<Node[]> s = new SortIterator(sa);
		_log.info("...sorted new links.");
		
		InputStream oldis = null;
		if(oldsorted!=null){
			_log.info("...merging with legacy links from "+oldsorted);
			oldis = new GZIPInputStream(new FileInputStream(oldsorted));
			NxParser oldit = new NxParser(oldis);
			MergeSortArgs msa = new MergeSortArgs(oldit, s);
			msa.setComparator(NODE_SORTING_NC);
			s = new MergeSortIterator(msa);
		}
		
		Node[] old = null;
		int linkc = 0;
		boolean done = false;
		
		_log.info("...aggregating indegree counts and seen flags for links.");
		if(crawled!=null){
			_log.info("...found "+crawled.size()+" URIs crawled in last round to flag done.");
		}
		
		int all = 0, u = 0, useen = 0, uactive = 0;
		while(s.hasNext()){
			Node[] lc = s.next();
			all++;
			if(old!=null && !lc[0].equals(old[0])){
				u++;
				if(done || (crawled!=null && crawled.contains(old[0].toString()))){
					linkc*=-1;
					useen++;
				} else{
					uactive++;
				}
				cb.processStatement(new Node[]{old[0], new Literal(Integer.toString(linkc))});
				
				done = false;
				linkc = 0;
			}
			old = lc;
			
			int c = Integer.parseInt(lc[1].toString());
			int absc = c;
			
			if(c<0){
				absc*=-1;
				done = true;
			}
			linkc += absc;
		}
		
		//do last one
		if(old!=null){
			if(done || (crawled!=null && crawled.contains(old[0].toString()))){
				linkc*=-1;
				useen++;
			} else{
				uactive++;
			}
			cb.processStatement(new Node[]{old[0], new Literal(Integer.toString(linkc))});
		}
		os.close();
		
		_log.info("..."+u+" links aggregated from "+all+" total, with "+useen+" seen and "+uactive+" unseen.");
	}
	
	public void close(Set<String> _donePrev) throws IOException, ParseException{
		_log.info("Closing on-disk link store... (preparing final frontier for possible restart)");		
		if(_currentOS!=null){
			initNextRound(_donePrev, true);
		}
		_log.info("...done.");
	}
	
	private static String createLinkFile(int round, String qdir){
		return qdir+"/"+round+"."+U_ORDERED_FN+UNSORTED_SUFFIX;
	}
	
	private static String createSortedLinkFile(int round, String qdir){
		return qdir+"/"+round+"."+U_ORDERED_FN+SORTED_SUFFIX;
	}
	
	private static String createSortedCountFile(int round, String qdir){
		return qdir+"/"+round+"."+C_ORDERED_FN+SORTED_SUFFIX;
	}
}
