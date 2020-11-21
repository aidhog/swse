package org.semanticweb.swse.ann.reason.master;

import java.rmi.RemoteException;

import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerInterface;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteAnnReasonerThread extends VoidRMIThread {
	private RMIAnnReasonerInterface _stub;
	private LinkedRuleIndex<AnnotationRule<RankAnnotation>> _tmplRules;

	public RemoteAnnReasonerThread(RMIAnnReasonerInterface stub, int server, LinkedRuleIndex<AnnotationRule<RankAnnotation>> tmplRules){
		super(server);
		_stub = stub;
		_tmplRules = tmplRules;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.reason(_tmplRules);
	}
}
