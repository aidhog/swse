package org.semanticweb.swse.cons.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.cons.RMIConsolidationInterface;
import org.semanticweb.swse.cons.utils.SameAsIndex;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteConsolidateThread extends RMIThread<Object> {
	private RMIConsolidationInterface _stub;
	private SameAsIndex _sai;
	private int[] _els;

	public RemoteConsolidateThread(RMIConsolidationInterface stub, int server, SameAsIndex sai, int[] els){
		super(server);
		_stub = stub;
		_sai = sai;
		_els = els;
	}
	
	protected Object runRemoteMethod() throws RemoteException{
		return _stub.consolidate(_sai, _els);
	}
}
