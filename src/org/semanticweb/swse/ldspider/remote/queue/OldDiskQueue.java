package org.semanticweb.swse.ldspider.remote.queue;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.logging.Logger;

import org.semanticweb.swse.ldspider.remote.utils.ErrorHandlerCounter;
import org.semanticweb.swse.ldspider.remote.utils.LinkFilter;
import org.semanticweb.swse.ldspider.remote.utils.PersistentRedirects;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

import com.ontologycentral.ldspider.queue.Pollable;
import com.ontologycentral.ldspider.queue.memory.Redirects;
import com.ontologycentral.ldspider.tld.TldManager;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.SecondaryIndex;
import com.sleepycat.persist.StoreConfig;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

/**
 * A berkeley DB based queue, specifically designed for remote crawler.
 * 
 * @author aidhog, juum
 */
public class OldDiskQueue implements Pollable<URI>{

	private static final Logger _log = Logger.getLogger(OldDiskQueue.class.getSimpleName());

//	private static final Integer PER_PLD_THRES = 100;

	private File envDir;
	private Environment _env;
	private EntityStore _store;
	private PrimaryIndex<String, URLObject> _urlIndex;
	private SecondaryIndex<Integer, String, URLObject> _countIndex;
	private PldManager _pldm = null;
	private PldManager _pldmRound = null;
	
	PersistentRedirects _redirs;
	
	private int _b4round = 0;

//	private long _time;
	
	//used for scheduling
	private long _mindelay;
	
	//score PLDs by structured data returned
	private boolean _score;
	
	private TldManager _tldm;

	private TempQueue _queue;
	
	private LinkFilter _lf = null;
	
	private boolean _checkedDone = false;
	
	private ErrorHandlerCounter _ehc;
	
	String[] _blacklist = { ".txt", ".html", ".jpg", ".pdf", ".htm", ".png", ".jpeg", ".gif" };

	public OldDiskQueue(TldManager tldm, PersistentRedirects pr, long mindelay, boolean score, String queueLocation, ErrorHandlerCounter ehc) throws URISyntaxException {
		this.envDir = new File(queueLocation);
		_tldm = tldm;
		_score = score;
		_redirs = pr;
		_mindelay = mindelay;
		_ehc = ehc;
		
		setup();
	}
	
	public void setPldManager(PldManager pldm){
		_pldm = pldm;
	}
	
	public void setPldManagerForRound(PldManager pldm){
		_pldmRound = pldm;
	}
	
	public Redirects getKnownRedirects(){
		return _redirs;
	}

	public void setup() throws DatabaseException {
		envDir.mkdirs();

		/* Open a transactional Berkeley DB engine environment. */
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		_env = new Environment(envDir, envConfig);

		/* Open a transactional entity store. */
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setTransactional(true);
		_store = new EntityStore(_env, this.getClass().getSimpleName(), storeConfig);

		/* Primary index of the queue */
		_urlIndex = _store.getPrimaryIndex(String.class, URLObject.class);

		/* Secondary index of the queue */
		_countIndex = _store.getSecondaryIndex(_urlIndex, Integer.class, "count");
	}


	public boolean close() {
		boolean success = true;
		if (_store != null) {
			try {
				_store.close();
			} catch (DatabaseException dbe) {
				_log.warning("Error closing store: " +
						dbe.toString());
				success = false;
			}
		}
		if (_env != null) {
			try {
				// Finally, close environment.
				_env.close();
			} catch (DatabaseException dbe) {
				_log.warning("Error closing env: " +
						dbe.toString());
				success = false;
			}
		}
		_redirs.close();
		printQueue();
		return success;
	}

	public void addFrontier(URI url){
		addFrontier(url, 1);
	}
	
	public void setLinkFilter(LinkFilter lf){
		_lf = lf;
	}
	
	/**
	 * Add normalised verified URI and inlink
	 */
	public void addFrontier(URI u, int inlinks){
		String uri = u.toASCIIString();
		
		synchronized (_urlIndex) {
			URLObject o = _urlIndex.get(uri);
			if(o==null)
				o = new URLObject(uri);
			else
				o.incrementCount(inlinks);
			
			_urlIndex.put(o);
		}
	}

	public URI poll() {
		if(_queue==null)
			return null;
		URI u = _queue.poll();
		if(u==null && !_checkedDone){
			Set<String> done = _queue.getDone();
			if(done!=null){
				for(String s:done){
					seen(s);
				}
			}
			_checkedDone = true;
		}
		
		return u;
	}

	public void schedule(int maxuris, int minuris, int targeturis) {
		_checkedDone = false;
		
		_b4round = _ehc.getLookups();
		
		int target = targeturis + _ehc.getLookups();
		
		_queue = new TempQueue(maxuris, minuris, target, _mindelay, _pldm, _pldmRound, _score, _ehc);

		_log.info("Schedule new queue with max uris per pld:"+maxuris+" min uris per pld:"+minuris+" target uris:"+targeturis+" and mindelay:"+_mindelay);
//		_pldMap = new HashMap<String, Integer>();
		EntityCursor<URLObject> cursor  = _countIndex.entities();
		URLObject o = cursor.prev();
		while(o!=null){
			if(o.getCount()<0){
				break;
			} else{
				URI u = o.getURI();
				_queue.tryAdd(u, _tldm.getPLD(u));
			}
			o = cursor.prev();
		} 

		_log.info("...scheduled "+_queue.size()+" uris from "+_queue.domains()+" PLDs");
		cursor.close();
	}
	
	public void setFinished(){
		if(_queue!=null)
			_queue.setFinished();
	}

	public synchronized void seen(URI url) {
		seen(url.toASCIIString());
	}
	
	public synchronized void seen(String s) {
		URLObject o = _urlIndex.get(s);
		if(o==null)
			o = new URLObject(s);
		o.setSeen();
		_urlIndex.put(o);
	}
	
	public synchronized int count(URI url) {
		return count(url.toASCIIString());
	}
	
	public synchronized int count(String s) {
		URLObject o = _urlIndex.get(s);
		if(o==null)
			return 0;
		return o.getCount();
	}
	
	public synchronized void unseen(URI url) {
		unseen(url.toASCIIString());
	}

	public synchronized void unseen(String s) {
		URLObject o = _urlIndex.get(s);
		if(o==null)
			o = new URLObject(s);
		o.unsetSeen();
		_urlIndex.put(o);
	}
	
	/**
	 * Returns from directly... Redirects handled externally.
	 * @deprecated
	 */
	public URI obtainRedirect(URI from) {
//		URI to = _redirs.getRedirect(from);
//		if (from != to) {
//			_log.info("redir from " + from + " to " + to);
//			seen(to);
//			return to;
//		}
		return from;
	}

	public void setRedirect(URI from, URI to) {
		try {
			to = LinkFilter.normalise(to);
		} catch (URISyntaxException e) {
			_log.info(to +  " not parsable, skipping " + to);
			return;
		}
		
		_queue.addRedirectPLD(_tldm.getPLD(from));

		if(to!=null){
			if(!from.equals(to)){
				_redirs.put(from, to);
			}
			
			URLObject o = _urlIndex.get(from.toASCIIString());
			if(o==null)
				o = new URLObject(from.toASCIIString());
			
			int c = o.getAbsCount();
			
			o.setHasRedirect();
//			o.setSeen();
			_urlIndex.put(o);
			
			if (from.equals(to)) {
				_log.info("redirected to same uri " + from);
				return;
			}
			
			if(_lf!=null){
				_lf.addLink(to.toString(), c+1);
			}
		}
	}
	
	public int size() {
		return _queue.size();
	}
	
	public int crawled(){
		return _ehc.getLookups() - _b4round;
	}


	public void printQueue() {
		EntityCursor<URLObject> cursor  = _countIndex.entities();
		URLObject o = null; 
		_log.info("============================\nQueue\n============================");
		do{
			o = cursor.prev();
			if(o!=null)
				_log.info(o.toString());
		}while(o != null);
		cursor.close();
		_log.info("============================");
	}
	
	public static void printQueue(String qloc){
		/* Open a transactional Berkeley DB engine environment. */
		File envDir = new File(qloc);
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setReadOnly(true);
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		Environment env = new Environment(envDir, envConfig);

		/* Open a transactional entity store. */
		StoreConfig storeConfig = new StoreConfig();
		storeConfig.setAllowCreate(true);
		storeConfig.setReadOnly(true);
		storeConfig.setTransactional(true);
		EntityStore store = new EntityStore(env, OldDiskQueue.class.getSimpleName(), storeConfig);

		/* Primary index of the queue */
		PrimaryIndex<String, URLObject> urlIndex = store.getPrimaryIndex(String.class, URLObject.class);

		/* Secondary index of the queue */
		SecondaryIndex<Integer, String, URLObject> countIndex = store.getSecondaryIndex(urlIndex, Integer.class, "count");
		
		EntityCursor<URLObject> cursor  = countIndex.entities();
		URLObject o = null; 
		_log.info("============================\nQueue\n============================");
		do{
			o = cursor.prev();
			if(o!=null)
				_log.info(o.toString());
		}while(o != null);
		cursor.close();
		_log.info("============================");
	}

//	private void addToQueue(URLObject o) {
//	//check the pld
//	String pld;
//	pld = _tld.getPLD(o.getURI());
//	Integer count = _pldMap.get(pld);
//	if(count == null) count = 0;
//	else if(count > PER_PLD_THRES) return;

//	//change value
//	//	    o.setTBCrawled();
//	//	    _urlIndex.put(o);
//	_tmpQueue.add(o.getURI());
//	_pldMap.put(pld, ++count);

//	}

//	//    @Override
//	//    public Stack<String> poll(int n) throws IOException {
//	//	Stack<String> s = new Stack<String>();
//	//	for (int i=0; i < n; i++) {
//	//	    String uri = poll();
//	//	    if (uri != null)
//	//		s.add(uri);
//	//	}
//	//	if(s.isEmpty()) 
//	//	    return null;
//	//	return s;
//	//    }

//	void removeDbFiles() {

//	for (File f : envDir.listFiles()) {
//	f.delete();
//	}
//	}
//
}

@Entity
class URLObject {

	@PrimaryKey
	String url;

	/* Many queues may have the same count. */
	@SecondaryKey(relate = com.sleepycat.persist.model.Relationship.MANY_TO_ONE)
	int count;

	long timestamp;
	
	//does it redirect somewhere?
	boolean redirects;

	public URLObject(String url) {
		this.url = url;
		count=1;
	}

	public void setSeen() {
		if(count>0)
			count *= -1;	
	}

	private URLObject() {} // Needed for deserialization.

	public URI getURI(){
		try {
			return new URI(url);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return null;
	}

	public int getCount(){
		return count;
	}
	
	public int getAbsCount(){
		if(count<0){
			return count*-1;
		}
		return count;
	}

	public void incrementCount(){
		incrementCount(1);
	}
	
	public void incrementCount(int by){
		if(count<0)
			count-=by;
		else count+=by;
	}

	public void unsetSeen() {
		if(count<0)
			count *= -1;	
	}
	
	public void setHasRedirect() {
		redirects = true;	
	}

	public long getTimestamp(){
		return timestamp;
	}

	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(this.url+" count:");
		if(count<0){
			buf.append((this.count*-1)+" done");
		} else{
			buf.append(this.count);
		}
		
		if(redirects){
			buf.append(" hasRedirect");
		}
		
		return buf.toString();
	}
}
