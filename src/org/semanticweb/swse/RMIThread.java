package org.semanticweb.swse;

import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.cb.CallbackFinished;
import org.semanticweb.swse.cb.CallbackResult;

/**
 * Abstract thread class for running a remote method on a remote crawler, 
 * where E is the return type for the remote method (see VoidRMIThread
 * for running void remote methods).
 * @author aidhog
 *
 */
public abstract class RMIThread<E> extends Thread {
	private final static Logger _log = Logger.getLogger(RMIThread.class.getSimpleName());
	protected Exception _error = null;
	protected E _result = null;
	protected long _startTime = System.currentTimeMillis();
	protected long _endTime = System.currentTimeMillis();
	protected CallbackResult<E> _cr = null;
	protected CallbackFinished _cf = null;
	protected int _server;
	
	public RMIThread(int server, String name){
		setName(name);
		_server = server;
	}
	
	public RMIThread(int server){
		_server = server;
		setName(getClass().getName()+server);
	}
	
	public RMIThread(int server, Class<? extends RMIThread<? extends Object>> c){
		setName(c.getName()+server);
		_server = server;
	}
	
	public void setResultCallback(CallbackResult<E> cr){
		_cr = cr;
	}
	
	public void setFinishedCallback(CallbackFinished cf){
		_cf = cf;
	}
	
	public String toString(){
		return getName();
	}
	
	public E getResult(){
		return _result;
	}
	
	public boolean successful(){
		return _error == null;
	}
	
	public Exception getException(){
		return _error;
	}
	
	protected abstract E runRemoteMethod() throws RemoteException;
	
	public void run() {
		_log.log(Level.INFO, "Started thread "+getName());
		_startTime = System.currentTimeMillis();
		
		try {
			_result = runRemoteMethod();
			if(_cr!=null)
				_cr.handleResult(_server, _result);
		} catch (RemoteException e) {
			_endTime = System.currentTimeMillis();
			_error = new Exception("Exception in thread "+getName()+":\n"+e.getMessage());
			return;
		}
		
		_endTime = System.currentTimeMillis();
		_log.log(Level.INFO, getName()+" finished in "+(_endTime-_startTime)+" ms");
		
		if(_cf!=null)
			_cf.handleFinished(_server);
	}
	
	public long getStartTime(){
		return _startTime;
	}
	
	public long getEndTime(){
		return _endTime;
	}
	
	public long getTotalTime(){
		return _endTime-_startTime;
	}
	
	public static abstract class VoidRMIThread extends RMIThread<Object>{
		/**
		 * Utility class for calling RMI threads with a void return type
		 * @param name
		 */
		public VoidRMIThread(int server, String name){
			super(server, name);
		}
		
		public VoidRMIThread(int server){
			super(server);
		}
		
		public VoidRMIThread(Class<? extends RMIThread<? extends Object>> c, int server){
			super(server, c);
		}

		protected Object runRemoteMethod() throws RemoteException {
			runRemoteVoidMethod();
			return null;
		}
		
		protected abstract void runRemoteVoidMethod() throws RemoteException;
	}
}
