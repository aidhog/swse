package org.semanticweb.swse.ann.repair.master;

import java.rmi.RemoteException;

import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.index.StatementAnnotationMap;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ann.repair.RMIAnnRepairInterface;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteAnnRepairThread extends RMIThread<Object> {
	private RMIAnnRepairInterface _stub;
	private StatementAnnotationMap<RankAnnotation> _deltaAll;
	private StatementAnnotationMap<RankAnnotation> _deltaD;

	public RemoteAnnRepairThread(RMIAnnRepairInterface stub, int server, StatementAnnotationMap<RankAnnotation> deltaAll, StatementAnnotationMap<RankAnnotation> deltaD){
		super(server);
		_stub = stub;
		_deltaAll = deltaAll;
		_deltaD = deltaD;
	}
	
	protected Object runRemoteMethod() throws RemoteException{
		return _stub.repair(_deltaAll, _deltaD);
	}
}
