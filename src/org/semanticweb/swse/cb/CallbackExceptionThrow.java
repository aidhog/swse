package org.semanticweb.swse.cb;

public class CallbackExceptionThrow implements CallbackException<Exception>{
	public void handleException(int server, Exception e) throws Exception{
		throw e;
	}
}
