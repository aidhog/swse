package org.semanticweb.swse.ann.incons.master;

import java.rmi.RemoteException;
import java.util.Collection;

import org.semanticweb.saorr.Statement;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ann.incon.RMIAnnInconInterface;
import org.semanticweb.yars.stats.Count;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteAnnInconCardinalitiesThread extends RMIThread<Count<Statement>> {
	private RMIAnnInconInterface _stub;
	private Collection<Statement> _patterns;

	public RemoteAnnInconCardinalitiesThread(RMIAnnInconInterface stub, int server, Collection<Statement> patterns){
		super(server);
		_stub = stub;
		_patterns = patterns;
	}
	
	protected Count<Statement> runRemoteMethod() throws RemoteException{
		return _stub.getCardinalities(_patterns);
	}
}
