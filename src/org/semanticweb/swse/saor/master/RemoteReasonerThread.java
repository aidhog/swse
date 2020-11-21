package org.semanticweb.swse.saor.master;

import java.rmi.RemoteException;

import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.saor.RMIReasonerInterface;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteReasonerThread extends VoidRMIThread {
	private RMIReasonerInterface _stub;
	private LinkedRuleIndex<Rule> _tmplRules;

	public RemoteReasonerThread(RMIReasonerInterface stub, int server, LinkedRuleIndex<Rule> tmplRules){
		super(server);
		_stub = stub;
		_tmplRules = tmplRules;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.reason(_tmplRules);
	}
}
