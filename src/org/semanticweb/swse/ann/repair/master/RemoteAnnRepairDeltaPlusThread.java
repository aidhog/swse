package org.semanticweb.swse.ann.repair.master;

import java.rmi.RemoteException;

import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.index.StatementAnnotationMap;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ann.repair.RMIAnnRepairInterface;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteAnnRepairDeltaPlusThread extends RMIThread<StatementAnnotationMap<RankAnnotation>> {
	private RMIAnnRepairInterface _stub;
	private StatementAnnotationMap<RankAnnotation> _delta;

	public RemoteAnnRepairDeltaPlusThread(RMIAnnRepairInterface stub, int server, StatementAnnotationMap<RankAnnotation> delta){
		super(server);
		_stub = stub;
		_delta = delta;
	}
	
	protected StatementAnnotationMap<RankAnnotation> runRemoteMethod() throws RemoteException{
		return _stub.getDeltaPlus(_delta);
	}
}
