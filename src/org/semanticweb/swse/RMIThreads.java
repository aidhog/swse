package org.semanticweb.swse;

import org.semanticweb.swse.cb.CallbackException;
import org.semanticweb.swse.cb.CallbackExceptionThrow;


public class RMIThreads {
	private RMIThread<? extends Object>[] _rmithreads;
	private CallbackException<Exception> _eh = new CallbackExceptionThrow();
	
	RMIThreads(RMIThread<? extends Object>... rmithreads){
		_rmithreads = rmithreads;
	}
	
	void setExceptionHandler(CallbackException<Exception> eh){
		_eh = eh;
	}
	
	public void start(){
		for(int i=0; i<_rmithreads.length; i++){
			_rmithreads[i].start();
		}
	}
	
	public void join() throws Exception{
		for(int i=0; i<_rmithreads.length; i++){
			_rmithreads[i].join();
			if(!_rmithreads[i].successful()){
				_eh.handleException(i, _rmithreads[i].getException());
			}
		}
	}
	
	public static long idleTime(RMIThread<? extends Object>[] threads){
		long maxtime = 0;
		for(int i=0; i<threads.length; i++){
			long time = threads[i].getTotalTime();
			if(time>maxtime)
				maxtime = time;
		}
		
		long totalidletime = 0;
		for(int i=0; i<threads.length; i++){
			totalidletime += maxtime - threads[i].getTotalTime();
		}
		
		return totalidletime;
	}
}
