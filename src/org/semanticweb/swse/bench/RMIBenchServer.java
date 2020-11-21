package org.semanticweb.swse.bench;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.bench.master.RemoteBenchGatherThread;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;


/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIBenchServer implements RMIBenchInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7517618659456394121L;
	private final static Logger _log = Logger.getLogger(RMIBenchServer.class.getSimpleName());
	public final static String GATHER_FILE = "gather.nq.gz";
	public final static String SCATTER_FILE = "scatter.nq.gz";
	
	public final static String TEMP_DIR = "tmp";

	public final static int BUFFER_SIZE = 1024;

	private transient RMIClient<RMIBenchInterface> _rmic;

	public final static int TICKS = 1000000;

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;
	
	private transient ArrayList<RemoteInputStream> _ris = null;

	private transient String _outdir;

	private transient CallbackNxOutputStream _c;
	
	private transient OutputStream _os = null;

	public RMIBenchServer() throws IOException, ClassNotFoundException{
		;
	}

	public void init(int serverId, RMIRegistries servers, String outdir) throws RemoteException {
		_log.log(Level.INFO, "Initialising server "+serverId+".");

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);

		_outdir = outdir;
		RMIUtils.mkdirs(outdir);

		_log.log(Level.INFO, "...connecting to peers...");
		try {
			_rmic = new RMIClient<RMIBenchInterface>(_servers, this, RMIBenchConstants.DEFAULT_STUB_NAME);
			
			String out = outdir+"/"+GATHER_FILE;
			_os = new GZIPOutputStream(new FileOutputStream(out));
			_c = new CallbackNxOutputStream(_os);
			
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up connections from server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up connections from server "+serverId+"\n"+e);
		}
		_log.log(Level.INFO, "...connected to peers...");

		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}

	public void close() throws RemoteException {
		try{
			if(_os!=null)
				_os.close();
			if(_ris!=null) for(RemoteInputStream ris:_ris){
				ris.close(true);
			}
		} catch(Exception e){
			_log.log(Level.INFO, "Error closing batch file on server "+_serverID+"\n"+e);
			e.printStackTrace();
		}
	}

	public void gather(RemoteInputStream inFile) throws RemoteException {
		_log.info("Gathering file...");
		long b4 = System.currentTimeMillis();
		InputStream is = null;
		ArrayList<Node[]> buffer = new ArrayList<Node[]>();
		try{
			is = RemoteInputStreamClient.wrap(inFile);
			is = new GZIPInputStream(is);

			NxParser nxp = new NxParser(is);

			while(nxp.hasNext()){
				buffer.add(nxp.next());
				if(buffer.size()==BUFFER_SIZE){
					for(Node[] na:buffer){
						_c.processStatement(na);
					}
					buffer = new ArrayList<Node[]>();
				}
			}

			for(Node[] na:buffer){
				_c.processStatement(na);
			}
			
			buffer = null;
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error gathering file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error gathering file on server "+_serverID+"\n"+e);
		} 
		_log.info("...file gathered in "+(System.currentTimeMillis()-b4)+" ms.");
	}

	public void scatter(String infile, boolean gzip) throws RemoteException {
		ArrayList<VoidRMIThread> gatherThreads = new ArrayList<VoidRMIThread>();

		ArrayList<RemoteInputStreamServer> riss = new ArrayList<RemoteInputStreamServer>();

		_log.info("Scattering "+infile+" to remote servers...");
		String[] files = null;
		try{
			InputStream is = new FileInputStream(infile);
			if(gzip){
				is = new GZIPInputStream(is);
			}
			NxParser nxp = new NxParser(is);

			files = split(nxp, 0);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error splitting local file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error splitting local file on server "+_serverID+"\n"+e);
		}
		for(int j=0; j<files.length; j++){
			RemoteInputStreamServer istream = null;

			riss.add(istream);

			try {
				istream = new SimpleRemoteInputStream(new BufferedInputStream(
						new FileInputStream(files[j])));
				RemoteInputStream result = istream.export();
				riss.add(istream);

				_log.info("Scattering "+files[j]+" to remote server");
				RMIBenchInterface rmii = _rmic.getStub(j);

				RemoteBenchGatherThread rigt = new RemoteBenchGatherThread(rmii, j, result);

				rigt.start();

				gatherThreads.add(rigt);
			} catch(IOException e){ 
				_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			}
		}

		_log.log(Level.INFO, "...awaiting threads return...");
		long b4 = System.currentTimeMillis();
		try{
			for(VoidRMIThread t:gatherThreads){
				_log.log(Level.INFO, "...awaiting return of thread "+t+"...");
				t.join();
			}
		} catch (Exception e){
			_log.log(Level.SEVERE, "Error waiting for gather thread "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error waiting for gather thread "+_serverID+"\n"+e);
		}

		for(RemoteInputStreamServer ris:riss){
			if(ris!=null) ris.close();
		}
		_log.info("...joining threads took "+(System.currentTimeMillis()-b4)+" ms.");
	}

	private String[] split(Iterator<Node[]> in, int el) throws FileNotFoundException, IOException{
		long b4 = System.currentTimeMillis();
		_log.info("Splitting file...");
		int files = _servers.getServerCount();
		String[] fns = new String[files];
		OutputStream[] os = new OutputStream[files];
		Callback[] cb = new Callback[files];

		RMIUtils.mkdirs(_outdir+"/"+TEMP_DIR);

		for(int i=0; i<files; i++){
			String outfile = _outdir+"/"+TEMP_DIR+"/"+i+"."+SCATTER_FILE;
			fns[i] = outfile;
			os[i] = new GZIPOutputStream(new FileOutputStream(outfile));
			cb[i] = new CallbackNxOutputStream(os[i]);
		}

		int c = 0;
		while(in.hasNext()){
			c++;
			if(c%TICKS==0){
				_log.info("...split "+c+" statements...");
			}

			Node[] next = in.next();
			int server = _servers.getServerNo(next[el]);
			cb[server].processStatement(next);
		}

		for(int i=0; i<files; i++){
			os[i].close();
		}
		_log.info("...splitting file done in "+(System.currentTimeMillis()-b4)+" ms... split "+c+" statements.");
		return fns;
	}

	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
		RMIBenchServer rmi = new RMIBenchServer();

		RMIBenchInterface stub = (RMIBenchInterface) UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public RemoteInputStream gatherLocal(String inFile, boolean gzip) throws RemoteException {
		RemoteInputStreamServer istream = null;

		_ris = new ArrayList<RemoteInputStream>();
		
		try {
			InputStream is = new FileInputStream(inFile);
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					is));
			RemoteInputStream result = istream.export();
			_ris.add(result);
			return result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		}
		
	}

	public void clear() throws RemoteException {
		_ris = null;
		_c = null;
		_os = null;
		_rmic = null;
	}


}
