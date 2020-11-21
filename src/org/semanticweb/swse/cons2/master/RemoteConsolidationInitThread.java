package org.semanticweb.swse.cons2.master;

import java.rmi.RemoteException;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.cons2.RMIConsolidationInterface;

/**
 * Thread to run init method on a remote consolidation server
 * @author aidhog
 *
 */
public class RemoteConsolidationInitThread extends VoidRMIThread {
	private int _server;
	private RMIConsolidationInterface _stub;
	private String _infile;
	private boolean _gz;
	private RMIRegistries _servers;
	private String _outdir;
	private String _tmpdir;

	public RemoteConsolidationInitThread(RMIConsolidationInterface stub, int server, RMIRegistries servers, String infile, boolean gz, String outdir, String tmpdir){
		super(server);
		_stub = stub;
		_servers = servers;
		_server = server;
		_gz = gz;
		_infile = infile;
		_outdir = outdir;
		_tmpdir = tmpdir;
	}
	
	protected void runRemoteVoidMethod() throws RemoteException{
		_stub.init(_server, _servers, _infile, _gz, _outdir, _tmpdir);
	}
}
