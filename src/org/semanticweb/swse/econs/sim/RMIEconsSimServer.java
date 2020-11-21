package org.semanticweb.swse.econs.sim;
//import java.rmi.RemoteException;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.rmi.AlreadyBoundException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.econs.sim.utils.RemoteScatter;
import org.semanticweb.swse.econs.sim.utils.Stats;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.mem.MemoryManager;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.reorder.ReorderIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.SortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.nx.sort.SortIterator.SortArgs;
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
public class RMIEconsSimServer implements RMIEconsSimInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3753950315603407560L;

	private final static Logger _log = Logger.getLogger(RMIEconsSimServer.class.getSimpleName());
	public final static String TEMP_DIR = "tmp";

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveEconsSimArgs _ssa;

	public final static int TICKS = 10000000;
	
	public static final Node SP_MARKER = new Literal("s");
	public static final Node OP_MARKER = new Literal("o");
	
	public static final Node ALL = new Literal("ALL");
	
	private transient Vector<String> _toGatherFn;

	private transient RMIClient<RMIEconsSimInterface> _rmic;
	
	public static final int[] OPSC_ORDER = new int[]{2,1,0,3};
	
	private transient ArrayList<HashMap<Node,PredStats>> _predStats = null;

	public RMIEconsSimServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveEconsSimArgs saa, String stubName) throws RemoteException {
		long b4 = System.currentTimeMillis();
		try {
			RMIUtils.setLogFile(saa.getSlaveLog());
		} catch (Exception e) {
			_log.log(Level.SEVERE, "Error setting up log file "+saa.getSlaveLog()+" on server "+serverId+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error setting up log file "+saa.getSlaveLog()+" on server "+serverId+"\n"+e);
		}

		_log.log(Level.INFO, "Initialising server "+serverId+".");
		_log.log(Level.INFO, "Setup "+saa);

		_ssa = saa;
		
		_toGatherFn = new Vector<String>();

		RMIUtils.mkdirs(_ssa.getOutDir());
		RMIUtils.mkdirs(_ssa.getTmpDir());
		RMIUtils.mkdirsForFile(_ssa.getOutData());
		RMIUtils.mkdirsForFile(_ssa.getRawOut());
//		RMIUtils.mkdirsForFile(_ssa.getRawOutOp());

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		_log.log(Level.INFO, "...connecting to peers...");

		try {
			_rmic = new RMIClient<RMIEconsSimInterface>(_servers, this, stubName);
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
		;
	}

	public int getServerID(){
		return _serverID;
	}

	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
		RMIEconsSimServer rmi = new RMIEconsSimServer();
		
		Remote stub = UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public static class PredStats implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		private int occurrenceCount = 0;
		private int triples = 0;
		private double average = 0;
		
		public double getAverage(){
			if(average==0){
				average = (double)triples/(double)occurrenceCount;
			}
			return average;
		}
		
		public int getOccurrenceCount(){
			return occurrenceCount;
		}
		
		public int getTriples(){
			return triples;
		}
		
		public String toString(){
			StringBuffer buf = new StringBuffer();
			buf.append("Count "+occurrenceCount+" Triples "+triples+" Average "+getAverage());
			return buf.toString();
		}
		
		public void addPredStats(PredStats ps){
			occurrenceCount += ps.occurrenceCount;
			triples += ps.triples;
			average = 0;
		}
		
		public void adjust(double avgOccurrence, double averageTriple){
			average = ((double)triples +  averageTriple) / ((double)occurrenceCount + avgOccurrence);
		}
	}
	
	public static HashMap<Node,PredStats> extractPredStatistics(Iterator<Node[]> iter){
		boolean es = false, ep = false, eo = false;
		Node[] old = null;
		
		int pv = 0;
		
		HashMap<Node,PredStats> predStats = new HashMap<Node,PredStats>();
		boolean bpv = false;
		PredStats cps = null;
		
		int count = 0;
		
		PredStats all = new PredStats();
		
		while(iter.hasNext()){
			Node[] next = iter.next();
			count++;
			
			if(count%TICKS==0){
				_log.info("...read "+count+" stmts... found "+predStats.size()+" preds...");
			}
			
//			if(next[1].equals(RDFS.ISDEFINEDBY)){
//				System.err.println(Nodes.toN3(next));
//			}
			
			if(old!=null){
				es = next[0].equals(old[0]);
				ep = next[1].equals(old[1]);
				eo = next[2].equals(old[2]);
				
				if(!es || !ep){
					if(pv!=0){
						cps = predStats.get(old[1]);
						if(cps==null){
							cps = new PredStats();
							predStats.put(old[1], cps);
						}
						
						cps.occurrenceCount++;
						cps.triples+=pv;
						
						all.occurrenceCount++;
						all.triples+=pv;
						
						pv = 0;
					}
					bpv = false;
				} else if(!eo){
					bpv = false;
				}
				
				if(!bpv){
					pv++;
					bpv = true;
				}
			}
			
			old = next;
		}
		
		cps = predStats.get(old[1]);
		if(cps==null){
			cps = new PredStats();
			predStats.put(old[1], cps);
		}
	
		cps.occurrenceCount++;
		cps.triples+=pv;
		
		all.occurrenceCount++;
		all.triples+=pv;
		
		predStats.put(ALL, all);
		
		_log.info("...read "+count+" stmts... found "+predStats.size()+" preds...");
		
		return predStats;
	}
	
	public static int generateSimilarity(Iterator<Node[]> iter, Callback cb, HashMap<Node,PredStats> pred, int limit, Node suffix){
		boolean es = false, ep = false, eo = false;
		Node[] old = null;
		
		TreeSet<Node> simClass = new TreeSet<Node>();
		boolean full = false;
		
		boolean bpv = false;
		
		int count = 0, skip = 0, out = 0;
		
		int lit = 0;
		
		double conf = 0;
		
		while(iter.hasNext()){
			Node[] next = iter.next();
			count++;
			
			if(count%TICKS==0){
				_log.info("...read "+count+" stmts... skipped "+skip+" classes larger than "+limit+"... output "+out+"...");
			}
			
			if(old!=null){
				es = next[0].equals(old[0]);
				ep = next[1].equals(old[1]);
				eo = next[2].equals(old[2]);
				
				if(!es || !ep){
					if(!simClass.isEmpty()){
						PredStats predStat = pred.get(old[1]);
						if(predStat!=null){
							double av = predStat.average;
							conf = 1d / (av * (double)(simClass.size()+lit));
							out += output(simClass, new Node[]{old[0], old[1], suffix, new Literal(Double.toString(conf))}, cb);
							simClass.clear();
						} else{
							_log.severe("No stats for "+old[1]+"!!!");
						}
					}
					full = false;
					bpv = false;
					lit = 0;
				} else if(!eo){
					bpv = false;
				}
				
				if(!bpv){
					if(!full){
						if(simClass.size()+lit==limit){
							full = true;
							simClass.clear();
							skip++;
						} else if(next[2] instanceof Literal){
							lit++;
						} else{
							simClass.add(next[2]);
						}
					}
				}
			}
			
			old = next;
		}
		
		if(!simClass.isEmpty()){
			PredStats predStat = pred.get(old[1]);
			if(predStat!=null){
				double av = predStat.average;
				conf = 1d / (av * (double)(simClass.size()+lit));
				out += output(simClass, new Node[]{old[0], old[1], suffix, new Literal(Double.toString(conf))}, cb);
				simClass.clear();
			} else{
				_log.severe("No stats for "+old[1]+"!!!");
			}
		}
		
		_log.info("...read "+count+" stmts... skipped "+skip+" classes larger than "+limit+"... output "+out+"...");
		
		return out;
	}
	
	public static void aggregateSimilarity(Iterator<Node[]> iter, Callback cb, ArrayList<HashMap<Node,PredStats>> pred, Stats<Double> stats){
		long start = System.currentTimeMillis();
		
		Nodes curS1S2 = null, prevS1S2 = null;
		Node[] nodes = null;
		
		HashMap<Nodes, ArrayList<Double>> pconfs = new HashMap<Nodes,ArrayList<Double>>();
		
		HashMap<Nodes, Double> pmeans = new HashMap<Nodes,Double>();
		
		Node curO = null, prevO = null;
		
		Nodes maxPI = null;
		double maxC = 0, maxM = 0;
		
		double lit = 0;
		
		int count=0;
		while(iter.hasNext()){
			count++;
			if(count%TICKS==0) {
				_log.info("...read "+count+" tuples... "+(System.currentTimeMillis()-start));
				stats.logShortStats();
			}
			
			nodes = iter.next();
			if(!(nodes[0] instanceof Literal) && !(nodes[1] instanceof Literal)){
				curS1S2 = new Nodes(nodes[0], nodes[1]);
				
				curO = nodes[2];

				if(prevS1S2!=null && !prevS1S2.equals(curS1S2)){
					ArrayList<Double> pcs = pconfs.get(maxPI);
					if(pcs==null){
						pcs = new ArrayList<Double>();
						pconfs.put(maxPI, pcs);
						pmeans.put(maxPI, maxM);
					}
					pcs.add(maxC);
					
					double pconf = getOverallAggregateValue(pconfs, pmeans);
					
					update(prevS1S2, pconf, cb, stats);
					
					maxPI = new Nodes(nodes[3], nodes[4]);
					maxC = Double.parseDouble(nodes[5].toString());
					if(nodes[4].equals(SP_MARKER)){
						maxM = pred.get(0).get(nodes[3]).getAverage();
					} else{
						maxM = pred.get(1).get(nodes[3]).getAverage();
					}
					pconfs = new HashMap<Nodes,ArrayList<Double>>();
					pmeans = new HashMap<Nodes,Double>();
				} else if(prevO!=null && !prevO.equals(nodes[2])){
					ArrayList<Double> pcs = pconfs.get(maxPI);
					if(pcs==null){
						pcs = new ArrayList<Double>();
						pconfs.put(maxPI, pcs);
						pmeans.put(maxPI, maxM);
					}
					pcs.add(maxC);
					
					maxPI = new Nodes(nodes[3], nodes[4]);
					maxC = Double.parseDouble(nodes[5].toString());
					if(nodes[4].equals(SP_MARKER)){
						maxM = pred.get(0).get(nodes[3]).getAverage();
					} else{
						maxM = pred.get(1).get(nodes[3]).getAverage();
					}
					prevO = nodes[2];
				} else{
					double c = Double.parseDouble(nodes[5].toString());
					double m = 0;
					if(nodes[4].equals(SP_MARKER)){
						m = pred.get(0).get(nodes[3]).getAverage();
					} else{
						m = pred.get(1).get(nodes[3]).getAverage();
					}
					
					if(c>maxC){
						maxC = c;
						maxPI = new Nodes(nodes[3], nodes[4]);
						maxM = m;
					}
				}
				
				prevO = curO;
				prevS1S2 = curS1S2;
			} else {
				lit++;
			}
		}
		if(prevS1S2!=null){
			ArrayList<Double> pcs = pconfs.get(maxPI);
			if(pcs==null){
				pcs = new ArrayList<Double>();
				pconfs.put(maxPI, pcs);
				pmeans.put(maxPI, maxM);
			}
			pcs.add(maxC);
			
			double pconf = getOverallAggregateValue(pconfs, pmeans);
			
			update(prevS1S2, pconf, cb, stats);
		}

		long end = System.currentTimeMillis();
		_log.info("total time elapsed for aggregation: "+(end-start)+" ms! Skipped "+lit+" literal sims.");	
		
		_log.info("...read "+count+" stmts... skipped "+lit+" literals... output "+stats.getN()+"...");
		stats.logShortStats();
	}
	
	public static int output(TreeSet<Node> simclass, Node[] suffix, Callback cb){
		if(simclass.size()<2)
			return 0;
		Node[] array  = new Node[simclass.size()];
		simclass.toArray(array);
		
		int out = 0;
		for(int i=0; i<array.length-1; i++){
			for(int j=i+1; j<array.length; j++){
				//new nodearray each time, just in case
				//callback is storing them...
				Node[] tmpl = new Node[2+suffix.length];
				System.arraycopy(suffix, 0, tmpl, 2, suffix.length);
				
				tmpl[0] = array[i];
				tmpl[1] = array[j];
				cb.processStatement(tmpl);
				
				out++;
			}
		}
		
		return out;
	}

	public ArrayList<HashMap<Node,PredStats>> predicateStatistics() throws RemoteException {
		String spoc = _ssa.getInSpoc();
		String opsc = _ssa.getInOpsc();

		InputStream spocIs = null;
		Iterator<Node[]> spocInput = null;
		try{
			spocIs = new FileInputStream(spoc);
			spocIs = new GZIPInputStream(spocIs); 

			spocInput = new NxParser(spocIs);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+spoc+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+spoc+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+spoc);
		
		_log.info("...scanning ...");
		
		HashMap<Node,PredStats> spocPS = extractPredStatistics(spocInput);

		_log.info("...scanned subject ordered data ...");
		
		InputStream opscIs = null;
		Iterator<Node[]> opscInput = null;
		try{
			opscIs = new FileInputStream(opsc);
			opscIs = new GZIPInputStream(opscIs);

			opscInput = new NxParser(opscIs);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+opsc+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+opsc+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+opsc);

		_log.info("...scanning ...");
		opscInput = new ReorderIterator(opscInput, OPSC_ORDER);
		
		HashMap<Node,PredStats> opscPS = extractPredStatistics(opscInput);

		_log.info("...scanned object ordered data ...");
		
		ArrayList<HashMap<Node,PredStats>> ans = new ArrayList<HashMap<Node,PredStats>>();
		ans.add(spocPS);
		ans.add(opscPS);
		
		_log.info("Returning results...");

		return ans;
	}
	
	public ArrayList<Integer> generateSimilarity(ArrayList<HashMap<Node,PredStats>> predStats) throws RemoteException {
		_predStats = predStats;
		
		OutputStream os = null;
		try{
			os = new FileOutputStream(_ssa.getRawOut());
			if(_ssa.getGzRawOut()){
				os = new GZIPOutputStream(os);
			}
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating similarity output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating similarity output file on server "+_serverID+"\n"+e);
		}
		
		_log.info("...output to "+_ssa.getRawOut());
		CallbackNxOutputStream cb = new CallbackNxOutputStream(os);
		
		String spoc = _ssa.getInSpoc();
		String opsc = _ssa.getInOpsc();
		
		InputStream spocIs = null;
		Iterator<Node[]> spocInput = null;
		try{
			spocIs = new FileInputStream(spoc);
			spocIs = new GZIPInputStream(spocIs); 

			spocInput = new NxParser(spocIs);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+spoc+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+spoc+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+spoc);
		
		_log.info("...generating similarity file...");
		int outs = generateSimilarity(spocInput, cb, predStats.get(0), _ssa.getLimit(), SP_MARKER);
		
		_log.info("...generated similarity from "+spoc+".");
		
		InputStream opscIs = null;
		Iterator<Node[]> opscInput = null;
		try{
			opscIs = new FileInputStream(opsc);
			opscIs = new GZIPInputStream(opscIs);

			opscInput = new NxParser(opscIs);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+opsc+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+opsc+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+opsc);
		
		opscInput = new ReorderIterator(opscInput, OPSC_ORDER);
		_log.info("...generating similarity file...");
		int outo = generateSimilarity(opscInput, cb, predStats.get(1), _ssa.getLimit(), OP_MARKER);
		
		_log.info("...generated similarity from "+opsc+".");

		try{
			os.close();
			spocIs.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing files on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing files on server "+_serverID+"\n"+e);
		}
		
		ArrayList<Integer> sizes = new ArrayList<Integer>();
		sizes.add(outs);
		sizes.add(outo);
		
		return sizes;
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
	
	public void scatterSimilarity() throws RemoteException {
		_log.info("Sorting and scattering triples...");
		String data = _ssa.getRawOut();

		long b4 = System.currentTimeMillis();

		InputStream is = null;
		Iterator<Node[]> input = null;
		try{
			is = new FileInputStream(data);
			if(_ssa.getGzRawOut())
				is = new GZIPInputStream(is); 

			input = new NxParser(is);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+data+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+data+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+data);

		SortArgs sa = new SortArgs(input);
		sa.setTicks(TICKS);
		sa.setTmpDir(_ssa.getTmpDir());

		try{
			SortIterator si = new SortIterator(sa);

			RMIUtils.mkdirs(_ssa.getOutScatterDir());
			RemoteScatter.scatter(si, _rmic, _ssa.getOutScatterDir(), _ssa.getRemoteGatherDir());
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error scanning/sorting/scattering triple ranks on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error scanning/sorting/scattering triple ranks on server "+_serverID+"\n"+e);
		}

		_log.info("...data sorted and scattered in "+(System.currentTimeMillis()-b4)+" ms.");
	}
	
	public Stats<Double> aggregateSimilarity() throws RemoteException {
		
		if(_predStats==null){
			_predStats = _ssa.getPredStats();
			if(_predStats==null){
				_log.log(Level.SEVERE, "Predicate stats not set on server "+_serverID+"\n");
				throw new RemoteException("Predicate stats not set on server "+_serverID+"\n");
			}
		}
		
		if(_toGatherFn==null || _toGatherFn.isEmpty()){
			_toGatherFn = new Vector<String>();
			
			String dir = _ssa.getLocalGatherDir();
			if(dir==null){
				_log.log(Level.SEVERE, "Nothing to gather on server "+_serverID+"\n");
				throw new RemoteException("Nothing to gather on server "+_serverID+"\n");
			}
			
			File d = new File(dir);
			for(File f:d.listFiles()){
				if(f.getName().endsWith(RemoteScatter.GATHER)){
					_toGatherFn.add(f.getAbsolutePath());
					_log.info("Adding "+f.getAbsoluteFile()+" to gather...");
				}
			}
			
			if(_toGatherFn.isEmpty()){
				_log.log(Level.SEVERE, "Nothing to gather on server (nothing in dir) "+_serverID+"\n");
				throw new RemoteException("Nothing to gather on server (nothing in dir) "+_serverID+"\n");
			}
		}

		_log.info("Aggregating similarity triples...");
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

		_toGatherFn.clear();
		
		String outFN = RMIUtils.getLocalName(_ssa.getOutData(), _serverID);;//_saa.getLocalGatherDir()+GATHER_PREFIX+gather+GATHER_SUFFIX;
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
		Callback cb = new CallbackNxOutputStream(os);

		_log.info("...starting aggregation...");
		MergeSortArgs msa = new MergeSortArgs(nxps);
		msa.setTicks(TICKS);
//		msa.setLinesPerBatch(MemoryManager.estimateMaxStatements(4));

		MergeSortIterator msi = new MergeSortIterator(msa);

		Stats<Double> stats = new Stats<Double>(SlaveEconsSimArgs.TOP_K, SlaveEconsSimArgs.RAND_K);
		aggregateSimilarity(msi, cb, _predStats, stats);
		
		try{
			os.close();
			for(InputStream is:iss)
				is.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing streams on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing streams on server "+_serverID+"\n"+e);
		}

		_log.info("...output "+stats.getN()+" aggregated ranked triples.");
		_log.info("...triples aggregated in "+(System.currentTimeMillis()-b4)+" ms.");
		
		stats.logStats();
		
		return stats;
	}
	
	
	private static void update(Nodes prevS1S2, double aggV, Callback cb, Stats<Double> stats){
		Literal aggVl = new Literal(Double.toString(aggV));
		Node[] e1e2c = new Node[]{prevS1S2.getNodes()[0], prevS1S2.getNodes()[1], aggVl};
		stats.addValue(aggV, new Nodes(e1e2c));
		cb.processStatement(new Node[]{prevS1S2.getNodes()[0], prevS1S2.getNodes()[1], aggVl});
	}
	
	private static double getOverallAggregateValue(HashMap<Nodes,ArrayList<Double>> pconfs, HashMap<Nodes,Double> pmeans){
		ArrayList<Double> pc = new ArrayList<Double>();
		for(Nodes n:pconfs.keySet()){
			ArrayList<Double> confs = pconfs.get(n);
			double mean = pmeans.get(n);
			
			pc.add(getAggregateValue(confs, 1D/mean));
		}
		
		return getAggregateValue(pc, 1D);
	}

	private static double getAggregateValue(ArrayList<Double> confs, double max){
		double agg = 0;
//		for(double d:confs){
//			double r = max - agg;
//			double c = r * (d/max);
//			agg+=c;
//		}
		for(double d:confs){
			double r = 1 - agg;
			double c = r * d;
			agg+=c;
		}
		return agg * max;
	}
}

