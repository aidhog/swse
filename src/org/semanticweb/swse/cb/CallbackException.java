package org.semanticweb.swse.cb;

public interface CallbackException<E extends Exception> {
	public void handleException(int server, E e) throws E;
}
