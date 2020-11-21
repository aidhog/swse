package org.semanticweb.swse.econs.ercons.master;

import java.rmi.RemoteException;
import java.util.Set;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.RMIThread;
import org.semanticweb.swse.econs.ercons.RMIConsolidationInterface;

import com.healthmarketscience.rmiio.RemoteInputStream;

/**
 * Thread to run init method on a remote crawler
 * @author aidhog
 *
 */
public class RemoteConsolidationTriplesThread extends RMIThread<RemoteInputStream> {
	private RMIConsolidationInterface _stub;
	private Set<Statement> _ps;
	private Rule[] _rules;
	private String _tbox;

	public RemoteConsolidationTriplesThread(RMIConsolidationInterface stub, int server, Set<Statement> ps, String tbox, Rule[] ruls){
		super(server);
		_stub = stub;
		_rules = ruls;
		_tbox = tbox;
		_ps = ps;
	}
	
	protected RemoteInputStream runRemoteMethod() throws RemoteException{
		return _stub.extractStatements(_ps, _tbox, _rules);
	}
}
