package org.semanticweb.swse;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RMIInterface extends Remote, Serializable{
	/**
	 * Clear allocated resources and close streams as required
	 * @throws RemoteException
	 */
	public void clear() throws RemoteException;
}
