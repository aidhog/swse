package org.semanticweb.swse.lucene.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.lucene.RMILuceneInterface;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteLuceneBuildThread extends VoidRMIThread {
	private RMILuceneInterface _stub;

	public RemoteLuceneBuildThread(RMILuceneInterface stub, int server){
		super(server);
		_stub = stub;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.buildLucene();
	}
}
