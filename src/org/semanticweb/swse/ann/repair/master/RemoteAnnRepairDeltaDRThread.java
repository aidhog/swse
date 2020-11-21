package org.semanticweb.swse.ann.repair.master;

import java.rmi.RemoteException;
import java.util.ArrayList;

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
public class RemoteAnnRepairDeltaDRThread extends RMIThread<ArrayList<StatementAnnotationMap<RankAnnotation>>> {
	private RMIAnnRepairInterface _stub;
	private StatementAnnotationMap<RankAnnotation> _delta;
	private StatementAnnotationMap<RankAnnotation> _deltaPlus;

	public RemoteAnnRepairDeltaDRThread(RMIAnnRepairInterface stub, int server, StatementAnnotationMap<RankAnnotation> delta, StatementAnnotationMap<RankAnnotation> deltaPlus){
		super(server);
		_stub = stub;
		_delta = delta;
		_deltaPlus = deltaPlus;
	}
	
	protected ArrayList<StatementAnnotationMap<RankAnnotation>> runRemoteMethod() throws RemoteException{
		return _stub.getDeltaDMinus(_delta, _deltaPlus);
	}
}
