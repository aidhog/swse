package org.semanticweb.swse.cb;

/**
 * Callback for handling the result of a remote method call.
 * @author aidhog
 *
 * @param <E> The type of the result
 */
public interface CallbackResult<E> {
	public void handleResult(int server, E result);
}
