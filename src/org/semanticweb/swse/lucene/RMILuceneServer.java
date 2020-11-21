package org.semanticweb.swse.lucene;
//import java.rmi.RemoteException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.semanticweb.nxindex.ScanIterator;
import org.semanticweb.nxindex.block.NodesBlockReaderNIO;
import org.semanticweb.saorr.auth.RedirectsAuthorityInspector;
import org.semanticweb.saorr.auth.redirs.FileRedirects;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.lucene.utils.LuceneIndexBuilder;
import org.semanticweb.yars.nx.parser.NxParser;

/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMILuceneServer implements RMILuceneInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8378684295307346191L;

	private final static Logger _log = Logger.getLogger(RMILuceneServer.class.getSimpleName());

	public final static int TICKS = 1000000;

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveLuceneArgs _sla;
	

	public RMILuceneServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveLuceneArgs sla) throws RemoteException {
		try {
			RMIUtils.setLogFile(sla.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+sla.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+sla.getSlaveLog()+" on server "+serverId+"\n"+e);
		}
		
		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+sla);
		
		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		
		_sla = sla;
		
		RMIUtils.mkdirs(_sla.getOutDir());

		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}

	public void buildLucene() throws RemoteException {
		_log.info("Making lucene index...");
		long b4 = System.currentTimeMillis();
		
		try{
			NodesBlockReaderNIO nbr = new NodesBlockReaderNIO(_sla.getIndex());
			ScanIterator si = new ScanIterator(nbr);
			
			InputStream is = new GZIPInputStream(new FileInputStream(_sla.getRanks()));
			
			NxParser nxp = new NxParser(is);
			
			FileRedirects r = null;
			
			try{
				_log.log(Level.INFO, "Reading redirects from "+_sla.getRedirects()+" gz:"+_sla.getGzRedirects()+" on server "+_serverID);
				r = FileRedirects.createCompressedFileRedirects(_sla.getRedirects(), _sla.getGzRedirects());
				_log.log(Level.INFO, "Loaded "+r.size()+" redirects on server "+_serverID);
			} catch(Exception e){
				_log.log(Level.SEVERE, "Error reading redirects file "+_sla.getRedirects()+" gz:"+_sla.getGzRedirects()+" on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error reading redirects file "+_sla.getRedirects()+" gz:"+_sla.getGzRedirects()+" on server "+_serverID+"\n"+e);
			}
			
			RedirectsAuthorityInspector rai = new RedirectsAuthorityInspector(r);
			
			LuceneIndexBuilder.buildLucene(si, nxp, rai, _sla.getOutDir());
			
			is.close();
			
			nbr.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error building lucene index on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error building lucene index on server "+_serverID+"\n"+e);
		}
		
		_log.info("...lucene index file built in "+(System.currentTimeMillis()-b4)+" ms.");
	}

	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
		RMILuceneServer rmi = new RMILuceneServer();

		RMILuceneInterface stub = (RMILuceneInterface) UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public void clear() throws RemoteException {
		_sla = null;
	}
}
