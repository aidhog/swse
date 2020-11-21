package org.semanticweb.swse.rank;
//import java.rmi.RemoteException;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.deri.idrank.identifiers.IdentifierRank;
import org.deri.idrank.namingauthority.NamingAuthority;
import org.deri.idrank.namingauthority.Redirects;
import org.deri.idrank.pagerank.PageRankInfo;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.mem.MemoryManager;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.CallbackSet;
import org.semanticweb.yars.util.LRUMapCache;

import com.healthmarketscience.rmiio.RemoteInputStream;
import com.healthmarketscience.rmiio.RemoteInputStreamClient;
import com.healthmarketscience.rmiio.RemoteInputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteInputStream;
import com.ontologycentral.ldspider.tld.TldManager;


/**
 * Takes calls from the stub and translates into crawler actions.
 * Also co-ordinates some inter-communication between servers for
 * scattering URIs.
 * 
 * @author aidhog
 */
public class RMIRankingServer implements RMIRankingInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3745161794195554948L;
	private final static Logger _log = Logger.getLogger(RMIRankingServer.class.getSimpleName());
	public final static String ID_RANK_FILENAME_UNSORTED = "idrank.nx.gz";
	public final static String ID_RANK_FILENAME_SORTED = "idrank.s.nx.gz";
	public final static String TEMP_DIR = "tmp";
	
	private final static int BYTE_BUFFER_LEN = 1024;


	private transient int _serverID = -1;
	private transient RMIRegistries _servers;
	
	private transient SlaveRankingArgs _sra;
	
	private transient Redirects _r;
	

	public RMIRankingServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveRankingArgs sra) throws RemoteException {
		try {
			RMIUtils.setLogFile(sra.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+sra.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+sra.getSlaveLog()+" on server "+serverId+"\n"+e);
		}
		
		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+sra);

		_sra = sra;
		
		RMIUtils.mkdirs(_sra.getOut());
		if(_sra.getOutFinal()!=null)
			RMIUtils.mkdirsForFile(_sra.getOutFinal());

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);

		_log.log(Level.INFO, "Connected.");
		
		try{
			if(_sra.getPLD()){
				TldManager tldm = new TldManager();
				_log.info("Reading redirect from file "+_sra.getRedirects()+" - gzipped "+_sra.getGzRedirects());
				
				InputStream is = new FileInputStream(_sra.getRedirects());
				if(_sra.getGzRedirects()){
					is = new GZIPInputStream(is);
				}
				
				NxParser nxp = new NxParser(is);
				
				LRUMapCache<Node,String> pldDictionary = new LRUMapCache<Node,String>(10000); 
				
				_r = new Redirects();
				while(nxp.hasNext()){
					Node[] next = nxp.next();
					if(next.length<2)
						continue;
					try{
						String pldS = pldDictionary.get(next[1]);
						
						if(pldS==null){
							URI to = new URI(next[1].toString());
							
							pldS = tldm.getPLD(to);
							if(pldS!=null){
								String toPLD = "http://"+tldm.getPLD(to);
								if(toPLD!=null){
									pldDictionary.put(next[1], toPLD);
									_r.put(next[0].toString(), toPLD);
								}
							}
						}
					} catch(URISyntaxException e){
						_log.info("Error reading redirect "+Nodes.toN3(next));
					}
				}
				
				pldDictionary = null;
				
				is.close();
				_log.info("...read "+_r.size()+" unique redirect pairs.");
			}
			else _r = Redirects.readNxFile(sra.getRedirects(), sra.getGzRedirects());
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error loading redirects "+sra.getRedirects()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error loading redirects "+sra.getRedirects()+" on server "+serverId+"\n"+e);
		}
	}

	public int getServerID(){
		return _serverID;
	}

	public Set<Node[]> extractNamingAuthority() throws RemoteException {
		Logger.getLogger(NamingAuthority.class.getName()).setLevel(Level.INFO);
		_log.info("Extracting naming authority...");
		
		NxParser nxp = null;
		InputStream is = null;
		
		try{
			is = new FileInputStream(_sra.getInNa());
			if(_sra.getGzInNa()){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getInNa()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getInNa()+" on server "+_serverID+"\n"+e);
		}

		CallbackSet cs = null;
		
		try{
			cs = new CallbackSet();
			NamingAuthority na = new NamingAuthority(_r);
			na.process(nxp, cs, _sra.getPLD(), !_sra.getTboxNa());
			is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating naming-authority object on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating naming-authority object on server "+_serverID+"\n"+e);
		}

		_log.info("..."+cs.getSet().size()+" naming authority extracted.");
		return cs.getSet();
	}

	public RemoteInputStream getIdRank(PageRankInfo pri) throws RemoteException {
		NxParser nxp = null;
		InputStream is = null;
		_log.info("Calculating ID ranks.");
		_log.info("NaRanks size: "+pri.size());
		
		try{
			is = new FileInputStream(_sra.getInId());
			if(_sra.getGzInId()){
				is = new GZIPInputStream(is);
			}
			nxp = new NxParser(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra.getInId()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra.getInId()+" on server "+_serverID+"\n"+e);
		}

		Logger.getLogger(IdentifierRank.class.getName()).setLevel(Level.INFO);
		

		//@todo
		Callback cb = null;
		OutputStream os = null;
		String unsorted = _sra.getOut()+"/"+ID_RANK_FILENAME_UNSORTED;
		try{
			os = new FileOutputStream(unsorted);
			os = new GZIPOutputStream(os);
			cb = new CallbackNxOutputStream(os);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+unsorted+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+unsorted+" on server "+_serverID+"\n"+e);
		}

		
		
		try{
			NamingAuthority na = new NamingAuthority(_r);
			//TODO verify args
			IdentifierRank.extractIDRanks(na, nxp, _sra.getPLD(), true, !_sra.getPLD(), _sra.getTboxId(), pri, cb);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error extracting ranks on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error extracting ranks on server "+_serverID+"\n"+e);
		}
		
		_log.info("...extracted ID ranks.");
		
		_log.info("...dellocating redirects (free-mem-b4):"+MemoryManager.estimateFreeSpace());
		_r = null;
		_log.info("...dellocated redirects (free-mem-after):"+MemoryManager.estimateFreeSpace());
		
		_log.info("...extracted ID ranks.");
		Runtime.getRuntime().gc();
		
		String sorted = _sra.getOut()+"/"+ID_RANK_FILENAME_SORTED;
		try{
			os.close();
			
			os = new FileOutputStream(sorted);
			os = new GZIPOutputStream(os);
			cb = new CallbackNxOutputStream(os);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating sorted file "+sorted+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating unsorted file "+unsorted+" on server "+_serverID+"\n"+e);
		}
		
		try{
			is = new FileInputStream(unsorted);
			is = new GZIPInputStream(is);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error opening unsorted file "+unsorted+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening unsorted file "+unsorted+" on server "+_serverID+"\n"+e);
		}
		
		String tmpDir = _sra.getOut()+"/"+TEMP_DIR+"/";
		RMIUtils.mkdirs(tmpDir);
		
		try{
			SortIterator.SortArgs sa = new SortIterator.SortArgs(new NxParser(is), (short)3);
			sa.setTmpDir(tmpDir);
			sa.setComparator(NodeComparator.NC);
			
			SortIterator si = new SortIterator(sa);
			while(si.hasNext()){
				Node[] next = si.next();
				if(next[1].equals(new Resource("http://sw.deri.org/~aidanh/foaf/foaf.rdf")))
					System.err.println(Nodes.toN3(next)+" "+_serverID);
				cb.processStatement(next);
			}
			
			os.close();
			is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error sorting unsorted file "+unsorted+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error sorting unsorted file "+unsorted+" on server "+_serverID+"\n"+e);
		}
		
		try{
			os.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing sorted file "+sorted+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing unsorted file "+unsorted+" on server "+_serverID+"\n"+e);
		}
		
		RemoteInputStreamServer istream = null;

		try {
			istream = new SimpleRemoteInputStream(new BufferedInputStream(
					new FileInputStream(sorted)));
			// export the final stream for returning to the client
			RemoteInputStream result = istream.export();
			// after all the hard work, discard the local reference (we are passing
			// responsibility to the client)
			istream = null;
			return result;
		} catch(IOException e){ 
			_log.log(Level.SEVERE, "Error creating RemoteInputStream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating RemoteInputStream on server "+_serverID+"\n"+e);
		} finally {
			// we will only close the stream here if the server fails before
			// returning an exported stream
			if(istream != null) istream.close();
		}

	}
	
	public void gatherRanks(RemoteInputStream ranksFile) throws RemoteException {
		_log.info("Gathering ranks to "+_sra.getOutFinal()+"...");
		long b4 = System.currentTimeMillis();
		InputStream in = null;
		OutputStream out = null;
		long total = 0;
		 
		try {
			out = new FileOutputStream(_sra.getOutFinal());
			in = RemoteInputStreamClient.wrap(ranksFile);
			
			byte[] buf = new byte[BYTE_BUFFER_LEN];
			
            for (;;) {
                int res = in.read(buf);
                if (res == -1) {
                    break;
                }
                if (res > 0) {
                    total += res;
                    if (out != null) {
                        out.write(buf, 0, res);
                    }
                }
            }

            in.close();
            in = null;
            out.close();
            out = null;
        } catch(Exception e){
			_log.log(Level.SEVERE, "Error gathering ranks on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error gathering ranks on server "+_serverID+"\n"+e);
		}
		_log.info("...file gathered in "+(System.currentTimeMillis()-b4)+" ms. Gathered "+total+" bytes.");
	}
	
	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
    	RMIRankingServer rmi = new RMIRankingServer();
    	
    	Remote stub = UnicastRemoteObject.exportObject(rmi, 0);

	    // Bind the remote object's stub in the registry
    	Registry registry;
    	if(hostname==null)
    		registry = LocateRegistry.getRegistry(port);
    	else
    		registry = LocateRegistry.getRegistry(hostname, port);
    	
	    registry.bind(stubname, stub);
	}
	
	public void clear() throws RemoteException {
		_sra = null;
		_r = null;
	}
}
