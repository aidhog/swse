package org.semanticweb.swse.cb;

/**
 * Callback for handling the result of a remote method call.
 * @author aidhog
 *
 * @param <E> The type of the result
 */
public class CallbacksResult<E> implements CallbackResult<E>{
	CallbackResult<E>[] _crs;
	
	public CallbacksResult(CallbackResult<E>... crs){
		_crs = crs;
	}
	
	public void handleResult(int server, E result){
		for(CallbackResult<E> cr:_crs){
			cr.handleResult(server, result);
		}
	}
}
