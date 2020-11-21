package org.semanticweb.swse.ldspider.remote.queue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Set;
import java.util.logging.Logger;

import org.semanticweb.swse.ldspider.remote.utils.ErrorHandlerCounter;
import org.semanticweb.swse.ldspider.remote.utils.LinkFilter;
import org.semanticweb.swse.ldspider.remote.utils.PersistentRedirects;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.yars.nx.parser.ParseException;

import com.ontologycentral.ldspider.queue.Pollable;
import com.ontologycentral.ldspider.queue.memory.Redirects;
import com.ontologycentral.ldspider.tld.TldManager;

/**
 * A berkeley DB based queue, specifically designed for remote crawler.
 * 
 * @author aidhog, juum
 */
public class DiskQueue implements Pollable<URI>{

	private static final Logger _log = Logger.getLogger(DiskQueue.class.getSimpleName());

//	private static final Integer PER_PLD_THRES = 100;

	private SimpleLinkStore _sls = null;
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
	
	private Set<String> _donePrev;
	
	private LinkFilter _lf = null;
	
	private boolean _checkedDone = false;
	
	private ErrorHandlerCounter _ehc;
	
	String[] _blacklist = { ".txt", ".html", ".jpg", ".pdf", ".htm", ".png", ".jpeg", ".gif" };

	public DiskQueue(TldManager tldm, PersistentRedirects pr, long mindelay, boolean score, String queueLocation, ErrorHandlerCounter ehc) throws URISyntaxException {
		_sls = new SimpleLinkStore(queueLocation);
		_tldm = tldm;
		_score = score;
		_redirs = pr;
		_mindelay = mindelay;
		_ehc = ehc;
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

	public boolean close() throws IOException, ParseException {
		_redirs.close();
		_sls.close(_donePrev);
		_pldm.logStats();
		return true;
	}

	public void setLinkFilter(LinkFilter lf){
		_lf = lf;
	}
	
	public void addFrontier(URI url) throws FileNotFoundException, IOException{
		addFrontier(url, 1);
	}
	
	/**
	 * Add normalised verified URI and inlink
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public void addFrontier(URI u, int inlinks) throws FileNotFoundException, IOException{
		_sls.addLink(u, inlinks);
	}

	public URI poll() {
		if(_queue==null)
			return null;
		URI u = _queue.poll();
		if(u==null && !_checkedDone){
			_donePrev = _queue.getDone();
			_checkedDone = true;
		}
		
		return u;
	}

	public void schedule(int maxuris, int minuris, int targeturis) throws IOException, ParseException {
		_checkedDone = false;
		
		_sls.initNextRound(_donePrev);
		
		_b4round = _ehc.getLookups();
		
		int target = targeturis + _ehc.getLookups();
		
		_queue = new TempQueue(maxuris, minuris, target, _mindelay, _pldm, _pldmRound, _score, _ehc);

		_log.info("Schedule new queue with max uris per pld:"+maxuris+" min uris per pld:"+minuris+" target uris:"+targeturis+" and mindelay:"+_mindelay);
		
		_sls.schedule(_queue, _tldm);

		_log.info("...scheduled "+_queue.size()+" uris from "+_queue.domains()+" PLDs");
	}
	
	public void setFinished(){
		if(_queue!=null)
			_queue.setFinished();
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
			
			int linkcount = _queue.getLinkCount(from);
			
			if (from.equals(to)) {
				_log.info("redirected to same uri " + from);
				return;
			}
			
			if(_lf!=null){
				_lf.addLink(to.toString(), linkcount+1);
			}
		}
	}
	
	public int size() {
		return _queue.size();
	}
	
	public int crawled(){
		return _ehc.getLookups() - _b4round;
	}
}
