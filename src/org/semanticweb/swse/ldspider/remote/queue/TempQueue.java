package org.semanticweb.swse.ldspider.remote.queue;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

import org.semanticweb.swse.ldspider.remote.utils.ErrorHandlerCounter;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

import com.ontologycentral.ldspider.queue.memory.Redirects;

/**
 * A buffer which stores a certain schedule of URIs in memory
 * 
 * @author aidhog, juum
 */
public class TempQueue {
	Logger _log = Logger.getLogger(this.getClass().getName());

	//old stats for reading
	PldManager _pldm;
	
	//new stats for writing for new round
	PldManager _newPldm;

	Redirects _redirs;
	
	//load balance if there's not enough domains/URIs to fill minDelay
//	boolean _loadbalance = false;
//	boolean _checked = false;

	int _maxplduris;
	
	int _minplduris;
	
	int _targeturis;

	int _rounds = 0;
	
	int _polled = 0, _notpolled = 0;

	long _mindelay;
	
	int _totalsize = 0;
	
	int _maxqsize = 0;
	
	int _qsatmaxsize = 0;
	
	boolean _finished = false;
	
	ErrorHandlerCounter _ehc;

	Map<String, Queue<URI>> _queues;
	Queue<String> _current = null;
	
	Map<URI, Integer> _linkCounts = null;
	
	HashSet<String> _pldRedirects = null;
	
	Set<String> _done;

	long _time;
	
	boolean _score = false;
	
	public TempQueue(int maxplduris, int minplduris, int targeturis, long mindelay, PldManager pldm, PldManager newPldm, boolean score, ErrorHandlerCounter ehc) {
		_log.info("Setting up in memory queue with "+maxplduris+" maxplduris, "+minplduris+" minplduris, "+targeturis+" target uris, mindelay of "+mindelay+", and scoring "+score);
		
		_maxplduris = maxplduris;
		
		_minplduris = minplduris;
		
		_targeturis = targeturis;

		_mindelay = mindelay;
		
		_pldm = pldm;
		
		_newPldm = newPldm;

		_redirs = new Redirects();
		
		_done = Collections.synchronizedSet(new HashSet<String>());

		_queues = Collections.synchronizedMap(new HashMap<String, Queue<URI>>());
		
		_linkCounts = Collections.synchronizedMap(new HashMap<URI, Integer>());
		
		_ehc = ehc;
		
		_score = score;
	}
	
	
	public int getPLDQueueSize(String pld){
		Queue<URI> pldq =  _queues.get(pld);
		if(pldq==null)
			return 0;
		return pldq.size();
	}

	/**
	 * Poll a URI, one PLD after another.
	 * If queue turnaround is smaller than DELAY, wait for DELAY ms to
	 * avoid overloading servers.
	 * 
	 * @return URI
	 */
	public synchronized URI poll() {
		if(_finished){
			return null;
		}
		if (_current == null) {
			schedule();
			if(_current == null){
				return null;
			}
		}

		URI next = null;

		if (_current.isEmpty()) {
			_log.info("per-domain round queue emptied");
			if(endRound()) return null;
		}
		if(_targeturis<=_done.size()){
			_log.info("reached target uri threshold of "+_targeturis);
			if(endRound()) return null;
		}

//		PldScorePair pldScore = _current.poll();
		String pld = _current.poll();
//		String pld = pldScore.getPld();
//		_newPldm.incrementPolled(pld);
		Queue<URI> q = _queues.get(pld);

		//doubly safe (could remove first if condition)
		if (q != null && !q.isEmpty()) {
			if(!toPollOrNotToPoll(_pldm.getScore(pld))){
				_notpolled++;
				_newPldm.incrementSkipped(pld);
				return poll();
			}
			_newPldm.incrementPolled(pld);
			
			next = q.poll();
			
			_polled++;
			_totalsize--;
			checkDecrementMaxsize(q.size());
			if (q.isEmpty()) {
				_queues.remove(pld);
			}
			
		} else {
			_queues.remove(pld);
			poll();
		}

		if(next!=null)
			_done.add(next.toASCIIString());

		return next;
	}

	private boolean toPollOrNotToPoll(double score) {
//		if(!_loadbalance)
//			return true;
		
		if(!_score)
			return true;
		double random = Math.random();
		return random<score;
	}

	private boolean endRound() {
		_rounds++;
		
		long time1 = System.currentTimeMillis();
		
		_log.info("per-domain queue round"+_rounds+" turnaround in " + (time1-_time) + " ms -- round "+_rounds+" polled " + _polled + " skipped "+_notpolled+" remaining round plds "+_current.size()+" total active plds "+_queues.size()+" uris "+size());
		
		//end if max rounds reached
		if(_rounds==_maxplduris){
			_log.info("max uris per domain reached for in-memory queue, finishing at round "+_rounds+" with active URIs "+size());
			_finished = true;
			return true;
		} 
		//end if empty
		else if (_queues.size() == 0) {
			_log.info("entire in-memory queue emptied, finishing at round "+_rounds);
			_finished = true;
			return true;
		} 
		//end if target URIs reached
		else if(_targeturis<=_ehc.getLookups()){
			_log.info("reached target uri threshold, finishing at round "+_rounds+" with active plds "+_queues.size()+" and active URIs "+size());
			_finished = true;
			return true;
		}
		else if ((time1 - _time) < _mindelay) {
			//if not filling mindelay, turn off scoring
//			if(_score){
//				_score = false;
//			}
			
			//end if min delay not being filled, scoring off, and min rounds fulfilled
			if(_rounds>=_minplduris){
				_log.info("min rounds reached and not filling min delay... finishing at round "+_rounds+" with active plds "+_queues.size()+" and active URIs "+size());
				_finished = true;
				return true;
			}
			
			//delay to fill min delay
			long delay = _mindelay - (time1 - _time);
			try {
				_log.info("delaying per-domain queue " + delay + " ms ...");
				Thread.sleep(delay);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		

		_time = System.currentTimeMillis();
		
		_polled = 0;
		_notpolled = 0;
		
		schedule();
		return false;
	}
	
	public void setFinished(){
		_rounds++;
		_log.info("per-domain queue round "+_rounds+" turnaround in " + (System.currentTimeMillis()-_time) + " ms -- round "+_rounds+" polled " + _polled + " skipped "+_notpolled+" remaining round plds "+_current.size()+" total active plds "+_queues.size()+" uris "+size());
		_finished = true;
		_log.info("queue round killed remotely,  finishing at round "+_rounds+" with active plds "+_queues.size()+" and active URIs "+size());
	}

	public Set<String> getDone(){
		return _done;
	}
	
	/**
	 * Schedule
	 * 
	 * @param u
	 */
	public synchronized void schedule() {
		_time = System.currentTimeMillis();
		_current = new ConcurrentLinkedQueue<String>();
		for(Entry<String,Queue<URI>> pld:_queues.entrySet()){
			_current.add(pld.getKey());
			if(_pldRedirects!=null && _pldRedirects.contains(pld.getKey()))
				_current.add(pld.getKey());
		}
		
		_pldRedirects = null;
	}
	
	/**
	 * Add a redirect PLD to it can be polled again in the
	 * same round 
	 */
	public synchronized void addRedirectPLD(String pld){
		if(_pldRedirects == null){
			_pldRedirects = new HashSet<String>();
		}
		_pldRedirects.add(pld);
	}
	
	public synchronized boolean tryAdd(URI u, String pld) {
		return tryAdd(u, pld, 1);	
	}
	
	/**
	 * Add URI directly to queues.
	 * 
	 * @param u
	 */
	public synchronized boolean tryAdd(URI u, String pld, int count) {
		if (pld != null) {	
			Queue<URI> q = _queues.get(pld);
			if (q == null) {
				q = new ConcurrentLinkedQueue<URI>();
				_queues.put(pld, q);
			} else if(q.size()>=_maxplduris*2){ //*2 for redirects
				return false;
			}
			_totalsize++;
			q.add(u);
			checkIncrementMaxsize(q.size());
			_linkCounts.put(u, count);
			return true;
		}
		return false;
	}
	
	public int getLinkCount(URI u){
		if(_linkCounts==null){
			return 0;
		}
		return _linkCounts.get(u);
	}
	
	private void checkIncrementMaxsize(int qsize){
		if(qsize>_maxqsize){
			_maxqsize = qsize;
			_qsatmaxsize = 1;
		} else if(qsize==_maxqsize){
			_qsatmaxsize++; 
		}
	}
	
	private void checkDecrementMaxsize(int qsize){
		if(qsize==(_maxqsize-1)){
			_qsatmaxsize--;
			if(_qsatmaxsize==0)
				_maxqsize--;
		}
	}
	
	public int size() {
		return _totalsize;
	}
	
	public int domains() {
		return _queues.size();
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();

		for (String pld : _queues.keySet()) {
			Queue<URI> q = _queues.get(pld);
			sb.append(pld);
			sb.append(": ");
			sb.append(q.size());
			sb.append("\n");
		}

		return sb.toString();
	}
}