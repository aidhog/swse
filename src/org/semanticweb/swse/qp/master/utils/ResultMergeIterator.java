package org.semanticweb.swse.qp.master.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Logger;

import org.semanticweb.yars.nx.Node;

import com.healthmarketscience.rmiio.RemoteIterator;

public class ResultMergeIterator implements Iterator<Node[]>{
	static Logger _log = Logger.getLogger(ResultMergeIterator.class.getName());
	
	Iterator<RemoteIterator<Node[]>> _inIter;
	RemoteIterator<Node[]> _current = null;

	Exception _e = null;

	public ResultMergeIterator(ArrayList<RemoteIterator<Node[]>> in) throws IOException{
		_inIter = in.iterator();
		
		while(_inIter.hasNext() && (_current==null || !_current.hasNext())){
			_current = _inIter.next();
			if(!_current.hasNext()){
				_current.close();
			}
		}
	}
	
	public boolean successful(){
		return _e == null;
	}
	
	public Exception getException(){
		return _e;
	}

	public boolean hasNext() {
		try{
			if(_current.hasNext()){
				return true;
			} 
		} catch(Exception e){
			setException(e);
		}
		try{
			finalize();
		} catch(Exception e){
			setException(e);
		}
		return false;
	}

	public Node[] next() {
		Node[] next = null;
		try{
			next = _current.next();
			if(!_current.hasNext()){
				_current.close();
			}

			while(_inIter.hasNext() && !_current.hasNext()){
				_current = _inIter.next();
				if(!_current.hasNext()){
					_current.close();
				}
			}
		} catch(Exception e){
			setException(e);
		}
		return next;
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	protected void finalize() throws Exception {
		_current.close();
		while(_inIter.hasNext()){
			_inIter.next().close();
		}
	}

	protected void setException(Exception e){
		_e = e;
		_log.severe("Exception for RemoteIterator:\n"+e);
	}
}
