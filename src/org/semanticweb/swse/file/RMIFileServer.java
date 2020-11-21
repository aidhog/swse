package org.semanticweb.swse.file;
//import java.rmi.RemoteException;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;

/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIFileServer implements RMIFileInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8378684295307346191L;

	private final static Logger _log = Logger.getLogger(RMIFileServer.class.getSimpleName());

	public final static int TICKS = 1000000;

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;
	private transient ArrayList<RemoteOutputStreamServer> _toClose = new ArrayList<RemoteOutputStreamServer>();

//	private transient SlaveFileArgs _sla;
	

	public RMIFileServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveFileArgs sla) throws RemoteException {
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
		
//		_sla = sla;
		
		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}

	public void receiveFile(RemoteInputStream ris, String s) throws RemoteException {
		_log.info("Writing file "+s+" from remote stream...");
		long b4 = System.currentTimeMillis();
		
		try{
			InputStream fileData = RemoteInputStreamClient.wrap(ris);
			FileOutputStream fos = new FileOutputStream(s);
			
			// Transfer bytes from in to out
		    byte[] buf = new byte[1024];
		    int len;
		    while ((len = fileData.read(buf)) > 0) {
		    	fos.write(buf, 0, len);
		    }
		    fileData.close();
		    fos.close();
		    
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error writing file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error writing file on server "+_serverID+"\n"+e);
		}
		
		_log.info("...file written in "+(System.currentTimeMillis()-b4)+" ms.");
	}
	
	public RemoteOutputStream getRemoteOutputStream(String s) throws RemoteException {
		_log.info("Opening remote outputstream to file "+s+"...");
		long b4 = System.currentTimeMillis();
		
		RemoteOutputStreamServer ostream = null;
		try{
			
			ostream = new SimpleRemoteOutputStream(new BufferedOutputStream(
				      new FileOutputStream(s)));
			
			_log.info("...outputstream opened in "+(System.currentTimeMillis()-b4)+" ms.");
			_toClose.add(ostream);
			return ostream;		    
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output stream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output stream on server "+_serverID+"\n"+e);
		}
	}

	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
		RMIFileServer rmi = new RMIFileServer();

		RMIFileInterface stub = (RMIFileInterface) UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public void clear() throws RemoteException {
		for(RemoteOutputStreamServer ros:_toClose)
			ros.close();
	}
}
