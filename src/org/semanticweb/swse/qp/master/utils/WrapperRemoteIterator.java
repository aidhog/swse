package org.semanticweb.swse.qp.master.utils;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Logger;

import com.healthmarketscience.rmiio.RemoteIterator;

public class WrapperRemoteIterator<E> implements Iterator<E>{
	static Logger _log = Logger.getLogger(WrapperRemoteIterator.class.getName());
	
	RemoteIterator<E> _remoteIter; 
	Exception _e = null;
	
	public WrapperRemoteIterator(RemoteIterator<E> ri){
		_remoteIter = ri;
	}

	public boolean hasNext() {
		try{
			if(_remoteIter.hasNext())
				return true;
		} catch(IOException e){
			setException(e);
		}
		try{
			finalize();
		} catch(Exception e){
			setException(e);
		}
		return false;
	}

	public E next() {
		try{
			return _remoteIter.next();
		} catch(Exception e){
			setException(e);
		}
		
		try{
			finalize();
		} catch(Exception e){
			setException(e);
		}
		return null;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	protected void finalize() throws Exception {
		_remoteIter.close();
	}
	
	protected void setException(Exception e){
		_e = e;
		_log.severe("Exception for RemoteIterator:\n"+e);
	}
	
	public boolean successful(){
		return _e == null;
	}
	
	public Exception getException(){
		return _e;
	}
}
