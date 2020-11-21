package org.semanticweb.swse;

import java.util.ArrayList;
import java.util.Collection;

import org.semanticweb.swse.cb.CallbackException;
import org.semanticweb.swse.cb.CallbackExceptionThrow;

public class RMIThreadsFactory {
	Collection<RMIThread<? extends Object>> _threads;
	CallbackException<Exception> _eh = new CallbackExceptionThrow();
	
	public RMIThreadsFactory(){
		_threads = new ArrayList<RMIThread<? extends Object>>();
	}
	
	public void addThread(RMIThread<? extends Object> thread){
		_threads.add(thread);
	}
	
	public void setExceptionHandler(CallbackException<Exception> eh){
		_eh = eh;
	}
	
	public RMIThreads createRMIThreads(){
		RMIThread<? extends Object>[] ta = new RMIThread[_threads.size()];
		_threads.toArray(ta);
		RMIThreads rmit = new RMIThreads(ta);
		rmit.setExceptionHandler(_eh);
		return rmit;
	}
}
