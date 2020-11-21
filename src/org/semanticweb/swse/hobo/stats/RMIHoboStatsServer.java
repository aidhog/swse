package org.semanticweb.swse.hobo.stats;
//import java.rmi.RemoteException;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.hobo.cli.GetPldStats;
import org.semanticweb.hobo.stats.PerPLDNamingStats;
import org.semanticweb.hobo.stats.PerPLDNamingStats.PerPLDStatsResults;
import org.semanticweb.hobo.stats.utils.Redirects;
import org.semanticweb.hobo.stats.utils.VerifyLengthIterator;
import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.hobo.stats.utils.RemoteScatter;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
import org.semanticweb.yars.stats.Count;
import org.semanticweb.yars.tld.TldManager;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.healthmarketscience.rmiio.RemoteOutputStream;
import com.healthmarketscience.rmiio.RemoteOutputStreamServer;
import com.healthmarketscience.rmiio.SimpleRemoteOutputStream;


/**
 * Takes calls from the stub and translates into ranking actions.
 * Also co-ordinates some inter-communication between servers for
 * scattering data.
 * 
 * @author aidhog
 */
public class RMIHoboStatsServer implements RMIHoboStatsInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3753950315603407560L;

	private final static Logger _log = Logger.getLogger(RMIHoboStatsServer.class.getSimpleName());
	public final static String TEMP_DIR = "tmp";

	public final static String GATHER_PREFIX = "gall";

	public final static String GATHER_SUFFIX = ".nq.gz";

	//	private transient int _gathered = 0;

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveHoboStatsArgs _saa;

	public final static int TICKS = 10000000;

	private transient Vector<String> _toGatherFn;

	private transient RMIClient<RMIHoboStatsInterface> _rmic;

	public RMIHoboStatsServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveHoboStatsArgs saa, String stubName) throws RemoteException {
		long b4 = System.currentTimeMillis();
		
		NxParser.DEFAULT_PARSE_DTS = false;
		Logger log = Logger.getLogger(TldManager.class.getName());
		log.setLevel(Level.WARNING);
		log = Logger.getLogger(Redirects.class.getName());
		log.setLevel(Level.WARNING);
		
		try {
			RMIUtils.setLogFile(saa.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+saa.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+saa.getSlaveLog()+" on server "+serverId+"\n"+e);
		}

		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+saa);

		_saa = saa;

		_toGatherFn = new Vector<String>();

		RMIUtils.mkdirs(_saa.getOutDir());
		RMIUtils.mkdirs(_saa.getOutScatterDir());
		RMIUtils.mkdirs(_saa.getTmpDir());
		RMIUtils.mkdirs(_saa.getLocalGatherDir());

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		_log.log(Level.INFO, "...connecting to peers...");

		try {
			_rmic = new RMIClient<RMIHoboStatsInterface>(_servers, this, stubName);
		} catch (NotBoundException e) {
			_log.log(Level.SEVERE, "Error setting up connections from server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up connections from server "+serverId+"\n"+e);
		}
		_log.log(Level.INFO, "...connected to peers...");

		_log.log(Level.INFO, "Connected.");

		_log.log(Level.INFO, "Connected in "+(System.currentTimeMillis() - b4)+" ms.");
		
		
	}

	public void clear() throws RemoteException {
		if(_toGatherFn!=null) _toGatherFn.clear();
	}

	public RemoteOutputStream getRemoteOutputStream(String filename) throws RemoteException {
		_log.info("Opening remote outputstream to file "+filename+"...");
		filename = RMIUtils.getLocalName(filename);
		RMIUtils.mkdirsForFile(filename);

		long b4 = System.currentTimeMillis();

		RemoteOutputStreamServer ostream = null;
		try{

			ostream = new SimpleRemoteOutputStream(new BufferedOutputStream(
					new FileOutputStream(filename)));

			synchronized(this){
				_toGatherFn.add(filename);
			}

			_log.info("...outputstream to file "+filename+" opened in "+(System.currentTimeMillis()-b4)+" ms.");
			return ostream;		    
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output stream on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output stream on server "+_serverID+"\n"+e);
		}
	}

	public void scatterTriples(int[] order, Set<Node> ignorePreds) throws RemoteException {
		_log.info("Sorting and scattering triples...");
		String data = _saa.getIn();

		long b4 = System.currentTimeMillis();

		InputStream is = null;
		Iterator<Node[]> input = null;
		try{
			is = new FileInputStream(data);
			if(_saa.getGzIn())
				is = new GZIPInputStream(is); 

			input = new NxParser(is);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+data+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+data+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+data);

		SortArgs sa = new SortArgs(input);
		sa.setComparator(new NodeComparator(order));
		sa.setTicks(TICKS);
		sa.setTmpDir(_saa.getTmpDir());

		try{
			SortIterator si = new SortIterator(sa);

			RMIUtils.mkdirs(_saa.getOutScatterDir());
			RemoteScatter.scatter(si, _rmic, _saa.getOutScatterDir(), _saa.getRemoteGatherDir(), order, ignorePreds);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error scanning/sorting/scattering triple ranks on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error scanning/sorting/scattering triple ranks on server "+_serverID+"\n"+e);
		}

		_log.info("...data sorted and scattered in "+(System.currentTimeMillis()-b4)+" ms.");
	}

	public void aggregateTriples(int[] order, String out) throws RemoteException {
		_log.info("Aggregating triples...");
		long b4 = System.currentTimeMillis();


		InputStream[] iss = new InputStream[_toGatherFn.size()];
		NxParser[] nxps = new NxParser[_toGatherFn.size()];


		for(int i=0; i<_toGatherFn.size(); i++){
			try{
				_log.info("...opening "+_toGatherFn.get(i)+"...");
				iss[i] = new FileInputStream(_toGatherFn.get(i));
				iss[i] = new GZIPInputStream(iss[i]);

				nxps[i] = new NxParser(iss[i]);
			} catch(Exception e){
				_log.log(Level.SEVERE, "Error opening input file "+_toGatherFn.get(i)+" on server "+_serverID+"\n"+e);
				e.printStackTrace();
				throw new RemoteException("Error opening input file "+_toGatherFn.get(i)+" on server "+_serverID+"\n"+e);
			}
		}


		String outFN = RMIUtils.getLocalName(out, _serverID);;//_saa.getLocalGatherDir()+GATHER_PREFIX+gather+GATHER_SUFFIX;
		RMIUtils.mkdirsForFile(outFN);

		_log.info("...output to "+outFN+"...");
		OutputStream os = null;
		try{
			os = new FileOutputStream(outFN);
			os = new GZIPOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+outFN+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+outFN+" on server "+_serverID+"\n"+e);
		}

		_log.info("...starting merge sort...");
		MergeSortArgs msa = new MergeSortArgs(nxps);
		msa.setTicks(TICKS);
		msa.setComparator(new NodeComparator(order));

		MergeSortIterator msi = new MergeSortIterator(msa);

		Callback cb = new CallbackNxOutputStream(os);

		int c = 0;
		while(msi.hasNext()){
			c++;
			if(TICKS>0 && c%TICKS==0){
				System.err.println("Processed "+c);
			}
			cb.processStatement(msi.next());
		}

		_toGatherFn.clear();

		try{
			os.close();
			for(InputStream is:iss)
				is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing streams on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing streams on server "+_serverID+"\n"+e);
		}

		_log.info("...output "+c+" aggregated triples.");
		_log.info("...triples aggregated in "+(System.currentTimeMillis()-b4)+" ms.");
	}

	public int getServerID(){
		return _serverID;
	}

	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
		RMIHoboStatsServer rmi = new RMIHoboStatsServer();

		Remote stub = UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public static final Node SPOC_SUFFIX = new Literal("0");
	public static final Node OPSC_SUFFIX = new Literal("1");
	public static final int[] OPSC_REORDER = new int[]{2,1,0,3};

	public static final String NO_PLD = "no_pld";

	/**
	 * @param args
	 * @throws IOException 
	 * @throws org.semanticweb.yars.nx.parser.ParseException 
	 */
	public Map<String,PerPLDStatsResults> stats(String spoc, String opsc) throws RemoteException {
		Logger log = Logger.getLogger(TldManager.class.getName());
		log.setLevel(Level.WARNING);

		spoc = RMIUtils.getLocalName(spoc, _serverID);
		opsc = RMIUtils.getLocalName(opsc, _serverID);
		
		HashMap<String,PerPLDStatsResults> map = null;
		try{
			map = GetPldStats.getStats(spoc, true, 
				opsc, true, 
				_saa.getR(), _saa.getGzR(), 
				_saa.getA(), _saa.getGzA(), 
				_saa.getC(), _saa.getGzC());
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error generating stats on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error generating stats on server "+_serverID+"\n"+e);
		}
		
//		Iterator<Node[]> its = null;
//		Iterator<Node[]> ito = null;
//		Redirects redirs = null;
//		HashSet<String> errors = new HashSet<String>();
//		InputStream as = null;
//		InputStream rs = null;
//		InputStream iss = null;
//		InputStream ios = null;
//		try{
//			iss = new FileInputStream(spoc); 
//			iss = new GZIPInputStream(iss);
//
//			ios = new FileInputStream(opsc); 
//			ios = new GZIPInputStream(ios);
//
//			//		OutputStream os = new FileOutputStream(cmd.getOptionValue("o"));
//			//		os = new GZIPOutputStream(os);
//
//			//		OutputStream ss = new FileOutputStream(cmd.getOptionValue("s"));
//			//		ss = new GZIPOutputStream(ss);
//
//			//		Callback cb = new CallbackNxOutputStream(os);
//
//			
//			_log.info("Reading access log...");
//
//
//			as = new FileInputStream(_saa.getA()); 
//			if(_saa.getGzA()){
//				as = new GZIPInputStream(as);
//			}
//
//
//			BufferedReader br = new BufferedReader(new InputStreamReader(as));
//			String line;
//			Count<String> cts = new Count<String>();
//			Count<Integer> rcs = new Count<Integer>();
//			while((line = br.readLine())!=null){
//				try{
//					String[] tokenise = line.split(" ");
//					int rc = Integer.parseInt(tokenise[3].substring("TCP_HIT/".length()));
//					rcs.add(rc);
//					if(rc==200){
//						String ct = "-";
//						if(tokenise.length==10)
//							ct = tokenise[9];
//						cts.add(ct);
//						if(!ct.equals("application/rdf+xml")){
//							errors.add(tokenise[6]);
//						}
//					} else if (rc>399){
//						errors.add(tokenise[6]);
//					}
//				} catch(Exception e){
//					_log.warning("Could not read access log line "+line);
//				}
//			}
//
//			_log.info("...found "+errors.size()+" non-dereferenceable URIs");
//			_log.info("===Response codes===");
//			rcs.printOrderedStats(_log, Level.INFO);
//			_log.info("===Content types===");
//			cts.printOrderedStats(_log, Level.INFO);
//
//			
//
//
//			br.close();
//
//			_log.info("Repening access log to find 303s...");
//			as = new FileInputStream(_saa.getA()); 
//			if(_saa.getGzA()){
//				as = new GZIPInputStream(as);
//			}
//			br = new BufferedReader(new InputStreamReader(as));
//
//			_log.info("Reading redirects...");
//			rs = new FileInputStream(_saa.getR()); 
//			if(_saa.getGzR()){
//				rs = new GZIPInputStream(rs);
//			}
//
//			redirs = Redirects.readIterator(new NxParser(rs), br);
//
//			_log.info("...read " + redirs.size() + " redirects.");
//
//			its = new NxParser(iss);
//			ito = new NxParser(ios);
//		}catch(Exception e){
//			_log.log(Level.SEVERE, "Error opening input on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error opening input on server "+_serverID+"\n"+e);
//		}
//
//		its = new VerifyLengthIterator(its, 4);
//		ito = new VerifyLengthIterator(ito, 4);
//
//		_log.info("Running stats...");
//
//		int i=0;
//		HashMap<String,PerPLDStatsResults> map = PerPLDNamingStats.stats(its, ito, redirs, errors);

		_log.info("...finished stats");

		int i = 0;
		
		PerPLDStatsResults all = map.remove(PerPLDNamingStats.ALL_PLDS);
		for(Entry<String,PerPLDStatsResults> pldc:map.entrySet()){
			Node[] ans = prepend(new Resource("http://"+pldc.getKey()+"/"), pldc.getValue().toNodeArray());
			if(ans.length-1!=PerPLDStatsResults.HEADER.length){
				_log.warning("Header length "+PerPLDStatsResults.HEADER.length+" Tuple length"+(ans.length-1));
			}
			_log.info(Nodes.toN3(ans));
			i++;
		}
		
		Node[] ans = prepend(new Literal(PerPLDNamingStats.ALL_PLDS), all.toNodeArray());
		_log.info(Nodes.toN3(ans));
		
		map.put(PerPLDNamingStats.ALL_PLDS, all);

		_log.info("...serialising stats");
		
		try{
			OutputStream ss = new GZIPOutputStream(new FileOutputStream(_saa.getStatsOut()));
			PerPLDNamingStats.serialise(map, ss);
			ss.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error serialising results on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error serialising results on server "+_serverID+"\n"+e);
		}
		
		_log.info("...done... cleaning up...");
		_log.info("...done. Finished! Returning "+map.size()+" results.");
		
		return map;
	}

	static Node[] prepend(Node prefix, Node[] suffix){
		Node[] ans =  new Node[suffix.length+1];
		ans[0] = prefix;
		System.arraycopy(suffix, 0, ans, 1, suffix.length);
		return ans;
	}


//	public PerPLDStatsResults stats(String spoc, String opsc, Set<Node> ignorePreds) throws RemoteException {
//		spoc = RMIUtils.getLocalName(spoc, _serverID);
//		opsc = RMIUtils.getLocalName(opsc, _serverID);
//
//		TldManager tldm = null;
//		try{
//			tldm = new TldManager();
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error starting TldManager on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error starting TldManager on server "+_serverID+"\n"+e);
//		}
//
//
//		InputStream spocIs = null;
//		Iterator<Node[]> spocInput = null;
//		try{
//			spocIs = new FileInputStream(spoc);
//			spocIs = new GZIPInputStream(spocIs); 
//
//			spocInput = new NxParser(spocIs);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error opening input file "+spoc+" on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error opening input file "+spoc+" on server "+_serverID+"\n"+e);
//		}
//		_log.info("...input from "+spoc);
//
//		spocInput = new AppendIterator(spocInput, new Node[]{SPOC_SUFFIX});
//
//		InputStream opscIs = null;
//		Iterator<Node[]> opscInput = null;
//		try{
//			opscIs = new FileInputStream(opsc);
//			opscIs = new GZIPInputStream(opscIs);
//
//			opscInput = new NxParser(opscIs);
//		} catch(Exception e){
//			_log.log(Level.SEVERE, "Error opening input file "+opsc+" on server "+_serverID+"\n"+e);
//			e.printStackTrace();
//			throw new RemoteException("Error opening input file "+opsc+" on server "+_serverID+"\n"+e);
//		}
//		_log.info("...input from "+opsc);
//
//		opscInput = new ReorderIterator(opscInput, OPSC_REORDER);
//		opscInput = new AppendIterator(opscInput, new Node[]{OPSC_SUFFIX});
//
//		MergeSortArgs msa = new MergeSortArgs(spocInput, opscInput);
//		msa.setComparator(new NodeComparator(true, true));
//
//		MergeSortIterator msi = new MergeSortIterator(msa);
//		Node old[] = null;
//		int s = 0, so = 0, ss = 0;
//
//		HashSet<String> plds = new HashSet<String>();
//		HashSet<Node> docs = new HashSet<Node>();
//		HashSet<String> pldsS = new HashSet<String>();
//		HashSet<Node> docsS = new HashSet<Node>();
//		HashSet<String> pldsO = new HashSet<String>();
//		HashSet<Node> docsO = new HashSet<Node>();
//		HashSet<String> pldsE = new HashSet<String>();
//		HashSet<String> pldsES = new HashSet<String>();
//		HashSet<String> pldsEO = new HashSet<String>();
//
//		StatsResults sr = new StatsResults();
//
//		String pldS = null;
//
//		boolean r = false, b = false;
//
//		int po = 0, ps = 0;
//
//		boolean bpo = false, bps = false, bpol = false, bpsl = false;
//
//		while(msi.hasNext()){
//			Node[] next = msi.next();
//			//			System.err.println(_serverID+ " "+Nodes.toN3(next));
//			boolean isOpsc = next[4].equals(OPSC_SUFFIX);
//
//			if(ignorePreds!=null && ignorePreds.contains(next[1])){
//				continue;
//			} else if(isOpsc && next[1].equals(RDF.TYPE)){
//				continue;
//			}
//
//			boolean es = false, ep = false, eo = false;
//
//			if(old==null){
//				if(next[0] instanceof Resource){
//					r = true;
//					pldS = SameAsIndex.getPLD(next[0]);
//					if(pldS==null){
//						pldS = NO_PLD;
//					}
//				}
//				else if(next[0] instanceof BNode){
//					b = true;
//					pldS = SameAsIndex.getPLD(next[0]);
//					if(pldS==null){
//						pldS = NO_PLD;
//					}
//				}
//			} else{
//				es = next[0].equals(old[0]);
//				ep = next[1].equals(old[1]);
//				eo = next[2].equals(old[2]);
//
//				if(!es){
//					if(b || r){
//						if(b){
//							sr.distribB.add(s);
//							sr.distribBO.add(so);
//							sr.distribBS.add(ss);
//							sr.bnodes++;
//						} else if(r){
//							sr.distribU.add(s);
//							sr.distribUO.add(so);
//							sr.distribUS.add(ss);
//							sr.uris++;
//						}
//
//						sr.distribC.add(docs.size());
//						sr.distribP.add(plds.size());
//
//						sr.distribCO.add(docsO.size());
//						sr.distribPO.add(pldsO.size());
//
//						sr.distribCS.add(docsS.size());
//						sr.distribPS.add(pldsS.size());
//
//						sr.distribPE.add(pldsE.size());
//						sr.distribPES.add(pldsES.size());
//						sr.distribPEO.add(pldsEO.size());
//
//						if(s>sr.maxsr){
//							sr.maxsr = s;
//							sr.maxsrn = old[0];
//						} 
//						if(docs.size()>sr.maxdr){
//							sr.maxdr = docs.size();
//							sr.maxdrn = old[0];
//						}
//						if(plds.size()>sr.maxpr){
//							sr.maxpr = plds.size();
//							sr.maxprn = old[0];
//						}
//					} else{
//						sr.distribL.add(s);
//						sr.distribLO.add(so);
//						sr.distribLS.add(ss);
//						sr.lits++;
//					}
//
//					plds = new HashSet<String>();
//					docs = new HashSet<Node>();
//					pldsS = new HashSet<String>();
//					docsS = new HashSet<Node>();
//					pldsO = new HashSet<String>();
//					docsO = new HashSet<Node>();
//					pldsE = new HashSet<String>();
//					pldsES = new HashSet<String>();
//					pldsEO = new HashSet<String>();
//
//					s = 0;
//					ss = 0;
//					so = 0;
//
//					sr.terms++;
//
//					r = false; b = false;
//					if(next[0] instanceof Resource){
//						r = true;
//						pldS = SameAsIndex.getPLD(next[0]);
//						if(pldS==null){
//							pldS = NO_PLD;
//						}
//					}
//					else if(next[0] instanceof BNode){
//						b = true;
//						pldS = SameAsIndex.getPLD(next[0]);
//						if(pldS==null){
//							pldS = NO_PLD;
//						}
//					}
//				}
//
//				if(!es || !ep){
//					if(po!=0){
//						sr.distribPObj.add(po);
//						if(po>sr.maxpo){
//							sr.maxpo = po;
//							sr.maxpons = new Nodes(old[0],old[1]);
//						}
//
//						if(po%50==0){
//							_log.info("Milestone "+po+" "+new Nodes(old[0],old[1]).toN3());
//						}
//
//						if(!bpol){
//							sr.distribPNObj.add(po);
//							if(po>sr.maxnlpo){
//								sr.maxnlpo = po;
//								sr.maxnlpons = new Nodes(old[0],old[1]);
//							}
//						}
//
//						po = 0;
//					}
//					if(ps!=0){
//						sr.distribPSub.add(ps);
//						if(ps>sr.maxps){
//							sr.maxps = ps;
//							sr.maxpsns = new Nodes(old[0],old[1]);
//						}
//
//						if(ps%50==0){
//							_log.info("Milestone "+ps+" "+new Nodes(old[0],old[1]).toN3());
//						}
//
//						if(!bpsl){
//							sr.distribPNSub.add(ps);
//							if(ps>sr.maxnlps){
//								sr.maxnlps = ps;
//								sr.maxnlpsns = new Nodes(old[0],old[1]);
//							}
//						}
//						ps = 0;
//					}
//					bpo = false;
//					bps = false;
//					bpol = false;
//					bpsl = false;
//				} else if(!eo){
//					bpo = false;
//					bps = false;
//				}
//
//				if(isOpsc && !bpo){
//					po++;
//					bpo = true;
//					if(next[2] instanceof Literal){
//						bpol = true;
//					}
//				}else if(!isOpsc && !bps){
//					ps++;
//					bps = true;
//					if(next[2] instanceof Literal){
//						bpsl = true;
//					}
//				}
//			}
//
//			if(pldS==null)
//				pldS = NO_PLD;
//
//			s++;
//
//
//
//			if(isOpsc)
//				so++;
//			else
//				ss++;
//
//			if(r || b){
//				if(docs.isEmpty() || !next[3].equals(old[3])){
//					String pldC = null;
//					try{
//						URI u = new URI(next[3].toString());
//						pldC = tldm.getPLD(u);
//					} catch(URISyntaxException u){
//						pldC = NO_PLD;
//					}
//					if(pldC==null)
//						pldC = NO_PLD;
//
//					plds.add(pldC);
//					docs.add(next[3]);
//					boolean ext = !pldC.equals(pldS);
//					if(ext){
//						pldsE.add(pldC);
//					}
//					if(isOpsc){
//						docsO.add(next[3]);
//						pldsO.add(pldC);
//						if(ext)
//							pldsEO.add(pldC);
//					} else{
//						docsS.add(next[3]);
//						pldsS.add(pldC);
//						if(ext)
//							pldsES.add(pldC);
//					}
//				}
//			}
//			old = next;
//		}
//
//		//do last
//		if(b || r){
//			if(b){
//				sr.distribB.add(s);
//				sr.distribBO.add(so);
//				sr.distribBS.add(ss);
//				sr.bnodes++;
//			}else if(r){
//				sr.distribU.add(s);
//				sr.distribUO.add(s);
//				sr.distribUS.add(s);
//				sr.uris++;
//			}
//
//			sr.distribC.add(docs.size());
//			sr.distribP.add(plds.size());
//
//			sr.distribCO.add(docsO.size());
//			sr.distribPO.add(pldsO.size());
//
//			sr.distribCS.add(docsS.size());
//			sr.distribPS.add(pldsS.size());
//
//			sr.distribPE.add(pldsE.size());
//			sr.distribPES.add(pldsES.size());
//			sr.distribPEO.add(pldsEO.size());
//
//			if(s>sr.maxsr){
//				sr.maxsr = s;
//				sr.maxsrn = old[0];
//			} 
//			if(docs.size()>sr.maxdr){
//				sr.maxdr = docs.size();
//				sr.maxdrn = old[0];
//			}
//			if(plds.size()>sr.maxpr){
//				sr.maxpr = plds.size();
//				sr.maxprn = old[0];
//			}
//		} else{
//			sr.distribL.add(s);
//			sr.distribLO.add(so);
//			sr.distribLS.add(ss);
//			sr.lits++;
//		}
//
//		plds = new HashSet<String>();
//		docs = new HashSet<Node>();
//		pldsS = new HashSet<String>();
//		docsS = new HashSet<Node>();
//		pldsO = new HashSet<String>();
//		docsO = new HashSet<Node>();
//		pldsE = new HashSet<String>();
//		pldsES = new HashSet<String>();
//		pldsEO = new HashSet<String>();
//
//		s = 0;
//		ss = 0;
//		so = 0;
//
//		sr.terms++;
//
//
//		return sr;
//	}

	public static class StatsResults implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = -2767437237983955653L;

		public Count<Integer> distribB = new Count<Integer>();
		public Count<Integer> distribU = new Count<Integer>();
		public Count<Integer> distribL = new Count<Integer>();

		public Count<Integer> distribBO = new Count<Integer>();
		public Count<Integer> distribUO = new Count<Integer>();
		public Count<Integer> distribLO = new Count<Integer>();

		public Count<Integer> distribBS = new Count<Integer>();
		public Count<Integer> distribUS = new Count<Integer>();
		public Count<Integer> distribLS = new Count<Integer>();

		public Count<Integer> distribC = new Count<Integer>();
		public Count<Integer> distribP = new Count<Integer>();
		public Count<Integer> distribCS = new Count<Integer>();
		public Count<Integer> distribPS = new Count<Integer>();
		public Count<Integer> distribCO = new Count<Integer>();
		public Count<Integer> distribPO = new Count<Integer>();
		public Count<Integer> distribPE = new Count<Integer>();
		public Count<Integer> distribPES = new Count<Integer>();
		public Count<Integer> distribPEO = new Count<Integer>();

		public Count<Integer> distribPSub = new Count<Integer>();
		public Count<Integer> distribPObj = new Count<Integer>();

		public Count<Integer> distribPNSub = new Count<Integer>();
		public Count<Integer> distribPNObj = new Count<Integer>();

		public int maxdr = 0;
		public Node maxdrn = null;
		public int maxpr = 0;
		public Node maxprn = null;
		public int maxsr = 0;
		public Node maxsrn = null;

		public int maxpo = 0;
		public Nodes maxpons = null;
		public int maxps = 0;
		public Nodes maxpsns = null;

		public int maxnlpo = 0;
		public Nodes maxnlpons = null;
		public int maxnlps = 0;
		public Nodes maxnlpsns = null;

		public int bnodes = 0, uris = 0, terms = 0, lits = 0;

		public void addStatsResults(StatsResults sr){
			bnodes += sr.bnodes;
			uris += sr.uris;
			terms += sr.terms;
			lits += sr.lits;

			if(sr.maxdr>maxdr && sr.maxdrn!=null){
				maxdr = sr.maxdr;
				maxdrn = sr.maxdrn;
			}

			if(sr.maxpr>maxpr  && sr.maxprn!=null){
				maxpr = sr.maxpr;
				maxprn = sr.maxprn;
			}

			if(sr.maxsr>maxsr && sr.maxsrn!=null){
				maxsr = sr.maxsr;
				maxsrn = sr.maxsrn;
			}

			if(sr.maxpo>maxpo && sr.maxpons!=null){
				maxpo = sr.maxpo;
				maxpons = sr.maxpons;
			}

			if(sr.maxps>maxps && sr.maxpsns!=null){
				maxps = sr.maxps;
				maxpsns = sr.maxpsns;
			}

			if(sr.maxnlpo>maxnlpo && sr.maxnlpons!=null){
				maxnlpo = sr.maxnlpo;
				maxnlpons = sr.maxnlpons;
			}

			if(sr.maxnlps>maxnlps && sr.maxnlpsns!=null){
				maxnlps = sr.maxnlps;
				maxnlpsns = sr.maxnlpsns;
			}

			distribB.addAll(sr.distribB);
			distribU.addAll(sr.distribU);
			distribL.addAll(sr.distribL);

			distribBO.addAll(sr.distribBO);
			distribUO.addAll(sr.distribUO);
			distribLO.addAll(sr.distribLO);

			distribBS.addAll(sr.distribBS);
			distribUS.addAll(sr.distribUS);
			distribLS.addAll(sr.distribLS);

			distribC.addAll(sr.distribC);
			distribP.addAll(sr.distribP);

			distribCS.addAll(sr.distribCS);
			distribPS.addAll(sr.distribPS);

			distribCO.addAll(sr.distribCO);
			distribPO.addAll(sr.distribPO);

			distribPE.addAll(sr.distribPE);
			distribPES.addAll(sr.distribPES);
			distribPEO.addAll(sr.distribPEO);

			distribPSub.addAll(sr.distribPSub);
			distribPObj.addAll(sr.distribPObj);

			distribPNSub.addAll(sr.distribPNSub);
			distribPNObj.addAll(sr.distribPNObj);
		}

		public void logStats(Logger log, Level l){
			log.log(l, "STATS FOR DATA TERMS");
			log.log(l, "Terms "+terms);
			log.log(l, "BNodes "+bnodes);
			log.log(l, "URIs "+uris);
			log.log(l, "Literals "+lits);

			if(maxsrn!=null)
				log.log(l, "Most triples for URI/Bnode "+maxsr+" "+maxsrn);
			if(maxdrn!=null)
				log.log(l, "Most documents for URI/Bnode "+maxdr+" "+maxdrn);
			if(maxprn!=null)
				log.log(l, "Most PLDs for URI/Bnode "+maxpr+" "+maxprn);

			if(maxpons!=null)
				log.log(l, "Biggest predicate-object pair (unique subjs) "+maxpo+" "+maxpons.toN3());
			if(maxpsns!=null)
				log.log(l, "Biggest predicate-subject pair (unique objs) "+maxps+" "+maxpsns.toN3());

			if(maxnlpons!=null)
				log.log(l, "Biggest predicate-object pair (unique non-lit subjs) "+maxnlpo+" "+maxnlpons.toN3());
			if(maxnlpsns!=null)
				log.log(l, "Biggest predicate-subject pair (unique non-lit objs) "+maxnlps+" "+maxnlpsns.toN3());


			log.log(l, "====Distribution of blank-node refs (avg: "+getAverage(distribB)+")====");
			distribB.printOrderedStats(log, l);
			log.log(l, "====Distribution of URI refs (avg: "+getAverage(distribU)+")====");
			distribU.printOrderedStats(log, l);
			log.log(l, "====Distribution of literal refs (avg: "+getAverage(distribL)+")====");
			distribL.printOrderedStats(log, l);

			log.log(l, "====Distribution of pred-obj cardinalities (avg: "+getAverage(distribPObj)+")====");
			distribPObj.printOrderedStats(log, l);
			log.log(l, "====Distribution of pred-sub cardinalities (avg: "+getAverage(distribPSub)+")====");
			distribPSub.printOrderedStats(log, l);

			log.log(l, "====Distribution of non-lit pred-obj cardinalities (avg: "+getAverage(distribPNObj)+")====");
			distribPNObj.printOrderedStats(log, l);
			log.log(l, "====Distribution of non-lit pred-sub cardinalities (avg: "+getAverage(distribPNSub)+")====");
			distribPNSub.printOrderedStats(log, l);

			log.log(l, "====Distribution of blank-node object refs (avg: "+getAverage(distribBO)+")====");
			distribBO.printOrderedStats(log, l);
			log.log(l, "====Distribution of URI object refs (avg: "+getAverage(distribUO)+")====");
			distribUO.printOrderedStats(log, l);
			log.log(l, "====Distribution of literal object refs (avg: "+getAverage(distribLO)+")====");
			distribLO.printOrderedStats(log, l);

			log.log(l, "====Distribution of blank-node subject refs (avg: "+getAverage(distribBS)+")====");
			distribBS.printOrderedStats(log, l);
			log.log(l, "====Distribution of URI subject refs (avg: "+getAverage(distribUS)+")====");
			distribUS.printOrderedStats(log, l);
			log.log(l, "====Distribution of literal subject refs (avg: "+getAverage(distribLS)+")====");
			distribLS.printOrderedStats(log, l);

			log.log(l, "====Distribution of blank-node/URI document refs (avg: "+getAverage(distribC)+")====");
			distribC.printOrderedStats(log, l);
			log.log(l, "====Distribution of blank-node/URI pld refs (avg: "+getAverage(distribP)+")====");
			distribP.printOrderedStats(log, l);
			log.log(l, "====Distribution of blank-node/URI document subject refs (avg: "+getAverage(distribCS)+")====");
			distribCS.printOrderedStats(log, l);
			log.log(l, "====Distribution of blank-node/URI pld subject refs (avg: "+getAverage(distribPS)+")====");
			distribPS.printOrderedStats(log, l);
			log.log(l, "====Distribution of blank-node/URI document object refs (avg: "+getAverage(distribCO)+")====");
			distribCO.printOrderedStats(log, l);
			log.log(l, "====Distribution of blank-node/URI pld object refs (avg: "+getAverage(distribPO)+")====");
			distribPO.printOrderedStats(log, l);
			log.log(l, "====Distribution of blank-node/URI pld external refs (avg: "+getAverage(distribPE)+")====");
			distribPE.printOrderedStats(log, l);
			log.log(l, "====Distribution of blank-node/URI pld external subject refs (avg: "+getAverage(distribPES)+")====");
			distribPES.printOrderedStats(log, l);
			log.log(l, "====Distribution of blank-node/URI pld external object refs (avg: "+getAverage(distribPEO)+")====");
			distribPEO.printOrderedStats(log, l);
		}

		public double getAverage(Count<Integer> stats){
			long occurrences = 0;
			long sum = 0;
			for(Map.Entry<Integer, Integer> e:stats.entrySet()){
				sum += e.getKey() * e.getValue();
				occurrences += e.getValue();
			}

			return (double)sum / (double)occurrences;
		}
	}
}

