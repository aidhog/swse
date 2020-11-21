package org.semanticweb.swse.ann.incons.master;

import java.rmi.RemoteException;

import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ann.incon.RMIAnnInconInterface;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteAnnInconThread extends VoidRMIThread {
	private RMIAnnInconInterface _stub;
	private LinkedRuleIndex<AnnotationRule<RankAnnotation>> _tmplRules;

	public RemoteAnnInconThread(RMIAnnInconInterface stub, int server, LinkedRuleIndex<AnnotationRule<RankAnnotation>> tmplRules){
		super(server);
		_stub = stub;
		_tmplRules = tmplRules;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.reasonInconsistencies(_tmplRules);
	}
}
