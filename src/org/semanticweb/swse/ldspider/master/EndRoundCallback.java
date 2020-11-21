package org.semanticweb.swse.ldspider.master;

import java.util.Collection;
import java.util.Iterator;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.cb.CallbackException;
import org.semanticweb.swse.cb.CallbackExceptionThrow;
import org.semanticweb.swse.cb.CallbackFinished;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;

public class EndRoundCallback implements CallbackFinished{
	public static final long DELAY_KILL = 10*1000; 
	
	VoidRMIThread[] _endrounds;
	CallbackException<Exception> _eh;
	
	private boolean _done = false;
	
	private static Logger _log = Logger.getLogger(EndRoundCallback.class.getName());
	
	public EndRoundCallback(Collection<RMICrawlerInterface> stubs){
		this(stubs, new CallbackExceptionThrow());
	}
	
	public EndRoundCallback(Collection<RMICrawlerInterface> stubs, CallbackException<Exception> eh){
		_eh = eh;
		_endrounds = new VoidRMIThread[stubs.size()];
		Iterator<RMICrawlerInterface> iter = stubs.iterator();
		for(int i=0; i<stubs.size(); i++){
			_endrounds[i] = new RemoteCrawlerEndRoundThread(iter.next(), i, false);
		}
	}
	
	public void handleFinished(int server) {
		if(!_done){
			_done = true;
			_log.info("Waiting "+DELAY_KILL+"ms to kill threads on other servers...");
			synchronized(this){
				try{
					this.wait(DELAY_KILL);
				} catch(InterruptedException ie){
					;
				}
			}
			
			_log.info("Starting end round threads...");
			for(int i=0; i<_endrounds.length; i++){
				if(i!=server)
					_endrounds[i].start();
			}
			
			_log.info("awaiting thread return...");
			for(int i=0; i<_endrounds.length; i++){
				if(i!=server){
					try {
						_endrounds[i].join();
						if(!_endrounds[i].successful()){
							_log.warning("Error ending round on server "+i+":\n"+_endrounds[i].getException());
						}
					} catch (InterruptedException e) {
						_log.warning("Error ending round on server "+i+":\n"+_endrounds[i].getException());
					}	
				}
			}
			_log.info("...ending rounds done.");
			
			long idletime = RMIThreads.idleTime(_endrounds);
			_log.info("Total idle time for co-ordination "+idletime+"...");
			_log.info("Average idle time for co-ordination "+(double)idletime/(double)(_endrounds.length)+"...");
		}
	}
	
}
