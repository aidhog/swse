package org.semanticweb.swse.cb;

/**
 * Callback for handling the return of a remote method call.
 * @author aidhog
 *
 * @param <E> The type of the result
 */
public class CallbacksFinished implements CallbackFinished{
	CallbackFinished[] _cfs;
	
	public CallbacksFinished(CallbackFinished... cfs){
		_cfs = cfs;
	}
	
	public void handleFinished(int server){
		for(CallbackFinished cf:_cfs){
			cf.handleFinished(server);
		}
	}
}
