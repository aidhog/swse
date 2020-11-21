package org.semanticweb.swse.ann.incons.master;

import java.rmi.RemoteException;
import java.util.Collection;

import org.semanticweb.saorr.Statement;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.ann.incon.RMIAnnInconInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run extract tbox method on a remote reasoner
 * @author aidhog
 *
 */
public class RemoteAnnInconNewTboxThread extends RMIThread<RemoteInputStream> {
	private RMIAnnInconInterface _stub;
	private Collection<Statement> _patterns;
	
	public RemoteAnnInconNewTboxThread(RMIAnnInconInterface stub, int server, Collection<Statement> patterns){
		super(server);
		_stub = stub;
		_patterns = patterns;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.extractStatements(_patterns);
	}
}
