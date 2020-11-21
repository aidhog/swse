package org.semanticweb.swse.econs.incon;
//import java.rmi.RemoteException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.rmi.AlreadyBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.deri.idrank.pagerank.OnDiskPageRank.AppendIterator;
import org.semanticweb.saorr.auth.AuthoritativeResource;
import org.semanticweb.saorr.engine.Reasoner;
import org.semanticweb.saorr.engine.ReasonerEnvironment;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.engine.input.NxaInput;
import org.semanticweb.saorr.index.MapTripleStore;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.reason.utils.ResetableFlyweightNodeIterator;
import org.semanticweb.swse.cons.utils.SameAsIndex;
import org.semanticweb.swse.cons.utils.SameAsIndex.SameAsList;
import org.semanticweb.swse.econs.ercons.utils.ConsolidationIterator;
import org.semanticweb.swse.econs.incon.utils.ConsolidatedDataIterator;
import org.semanticweb.swse.econs.incon.utils.InconLogger;
import org.semanticweb.swse.econs.incon.utils.MoreInfoProcessor;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer.PredStats;
import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.NodeComparator;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.reorder.ReorderIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator;
import org.semanticweb.yars.nx.sort.MergeSortIterator.MergeSortArgs;
import org.semanticweb.yars.util.CallbackNxOutputStream;
import org.semanticweb.yars.util.ResetableIterator;
import org.semanticweb.yars2.query.algebra.filter.bool.IsBlank;
import org.semanticweb.yars2.query.algebra.filter.noop.Noop;

import com.ontologycentral.ldspider.hooks.content.CallbackDummy;


/**
 * Takes calls from the stub and translates into ranking actions.
 * Also co-ordinates some inter-communication between servers for
 * scattering data.
 * 
 * @author aidhog
 */
public class RMIEconsInconServer implements RMIEconsInconInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3753950315603407560L;

	private final static Logger _log = Logger.getLogger(RMIEconsInconServer.class.getSimpleName());
	public final static String TEMP_DIR = "tmp";

	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveEconsInconArgs _sia;

	public final static int TICKS = 10000000;
	
	public final static int[] ALIAS_POS = {4};
	
	public static final Node SPOC_SUFFIX = new Literal("0");
	public static final Node OPSC_SUFFIX = new Literal("1");
	public static final int[] OPSC_REORDER = new int[]{2,1,0,3,5,4};
	public static final int[] OPS_ORDER = new int[]{2,1,0};

	//	private transient RMIClient<RMIEconsInconInterface> _rmic;

	public RMIEconsInconServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveEconsInconArgs saa, String stubName) throws RemoteException {
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

		_sia = saa;

		RMIUtils.mkdirs(_sia.getOutDir());
		RMIUtils.mkdirs(_sia.getTmpDir());
		RMIUtils.mkdirsForFile(_sia.getOutData());
		RMIUtils.mkdirsForFile(_sia.getOutInconsistencies());

		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);

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
		RMIEconsInconServer rmi = new RMIEconsInconServer();

		Remote stub = UnicastRemoteObject.exportObject(rmi, 0);

		// Bind the remote object's stub in the registry
		Registry registry;
		if(hostname==null)
			registry = LocateRegistry.getRegistry(port);
		else
			registry = LocateRegistry.getRegistry(hostname, port);

		registry.bind(stubname, stub);
	}

	public Map<Node,Map<Node,SameAsIndex.SameAsList>> findInconsistencies() throws RemoteException {
		_log.log(Level.INFO, "Loading local T-Box...");
		LinkedRuleIndex<Rule> lri = null;
		long time = System.currentTimeMillis();
		HashSet<Node> ifps = new HashSet<Node>();
		HashSet<Node> fps = new HashSet<Node>();
		
		try{
			ResetableIterator<Node[]> tbox = null;
			FileInput fi = null; 
			if(_sia.getGzTboxIn())
				fi = new NxaGzInput(new File(_sia.getTboxIn()),4);
			else fi = new NxaInput(new File(_sia.getTboxIn()),4);
			
			_log.info("Looking in TBox for IFPs and FPs");
			while(fi.hasNext()){
				Node[] next = fi.next();
				if(next[1].equals(RDF.TYPE) && next[0] instanceof AuthoritativeResource){
					if(next[2].equals(OWL.INVERSEFUNCTIONALPROPERTY)){
						ifps.add(((AuthoritativeResource)next[0]).toResource());
					} else if(next[2].equals(OWL.FUNCTIONALPROPERTY)){
						fps.add(((AuthoritativeResource)next[0]).toResource());
					}
				}
			}
			_log.info("Done. Found "+ifps.size()+" auth IFPs and "+fps.size()+" FPs");
			
			fi.reset();
			
			tbox = new ResetableFlyweightNodeIterator(1000, fi);

			Rules rls = new Rules(_sia.getRules());
			rls.setAuthoritative();

			Callback cbd = new CallbackDummy(); 

			ReasonerSettings rs = new ReasonerSettings();
			rs.setAuthorativeReasoning(true);
			rs.setFragment(rls.getRulesArray());
			rs.setMergeRules(true);
			rs.setPrintContexts(false);
			rs.setSaturateRules(true);
			rs.setSkipABox(true);
			rs.setSkipAxiomatic(false);
			rs.setSkipTBox(false);
			rs.setTBoxRecursion(false);
			rs.setTemplateRules(true);
			rs.setUseAboxRuleIndex(true);

			ReasonerEnvironment re = new ReasonerEnvironment(null, tbox, cbd);

			Reasoner r = new Reasoner(rs, re);
			r.reason();

			lri = re.getAboxRuleIndex();

			lri.freeResources();

			//hack to fix dangling AUTH filter which shouldn't be there
			for(LinkedRuleIndex.LinkedRule<Rule> tmplRule : lri.getAllLinkedRules()){
				if(tmplRule.getRule().getID().startsWith("tmp_prp-ifp-c")){
					_log.info(tmplRule.getRule()+" (links "+tmplRule.linksCount()+")");
					_log.info(" "+tmplRule.getRule().getFilter().getArguments().remove(1));
					tmplRule.getRule().getFilter().getArguments().add(1, new IsBlank(new Noop(new BNode("yes"))));
					_log.info(" fixed: "+tmplRule.getRule().getFilter());
				}
			}
			
			fi.close();
		} catch (Exception e){
			_log.log(Level.SEVERE, "Error opening input tbox file "+_sia.getTboxIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input tbox file "+_sia.getTboxIn()+" on server "+_serverID+"\n"+e);
		}
		_log.log(Level.INFO, "...loaded tbox and created template rules in "+(System.currentTimeMillis()-time)+" ms");

		String spocF = _sia.getInSpoc();
		String opscF = _sia.getInOpsc();

		InputStream spocIs = null;
		Iterator<Node[]> spoc = null;
		try{
			spocIs = new FileInputStream(spocF);
			spocIs = new GZIPInputStream(spocIs); 

			spoc = new NxParser(spocIs);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+spocF+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+spocF+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+spocF);

		spoc = new AppendIterator(spoc, new Node[]{SPOC_SUFFIX});

		InputStream opscIs = null;
		Iterator<Node[]> opsc = null;
		try{
			opscIs = new FileInputStream(opscF);
			opscIs = new GZIPInputStream(opscIs);

			opsc = new NxParser(opscIs);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+opscF+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+opscF+" on server "+_serverID+"\n"+e);
		}
		_log.info("...input from "+opscF);
		
		opsc = new ReorderIterator(opsc, OPSC_REORDER);
		opsc = new AppendIterator(opsc, new Node[]{OPSC_SUFFIX});
		
		_log.info("Reading pred stats file "+_sia.getPredStatsIn());
		HashMap<Node,Double> predSpoc = new HashMap<Node,Double>();
		HashMap<Node,Double> predOpsc = new HashMap<Node,Double>();
		try{
//			ArrayList<HashMap<Node,PredStats>>
			InputStream is = new FileInputStream(_sia.getPredStatsIn());
			if(_sia.getGzPredStatsIn()) is = new GZIPInputStream(is);
			
			ObjectInputStream ois = new ObjectInputStream(is);
			ArrayList<HashMap<Node,PredStats>> pall = (ArrayList<HashMap<Node,PredStats>>)ois.readObject();
			
			for(Map.Entry<Node, PredStats> e:pall.get(0).entrySet()){
				if(!fps.contains(e.getKey()) && !e.getKey().equals(OWL.SAMEAS))
					predSpoc.put(e.getKey(), 1d/e.getValue().getAverage());
			}
			
			for(Map.Entry<Node, PredStats> e:pall.get(1).entrySet()){
				if(!ifps.contains(e.getKey()) && !e.getKey().equals(OWL.SAMEAS))
					predOpsc.put(e.getKey(), 1d/e.getValue().getAverage());
			}
			is.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sia.getPredStatsIn()+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sia.getPredStatsIn()+" on server "+_serverID+"\n"+e);
		}
		_log.info("...read "+predSpoc.size()+" predicate stats for SPOC.");
		_log.info("...read "+predOpsc.size()+" predicate stats for OPSC.");

		OutputStream os = null;
		try{
			os = new FileOutputStream(_sia.getOutInconsistencies());
			if(_sia.getGzInconsistencies()){
				os = new GZIPOutputStream(os);
			}
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating inconsistency output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating inconsistency output file on server "+_serverID+"\n"+e);
		}
		_log.info("...output to "+_sia.getOutInconsistencies());

		Map<Node,Map<Node,SameAsIndex.SameAsList>> repair = null;
		try{
			MergeSortArgs msa = new MergeSortArgs(spoc, opsc);
			msa.setComparator(new NodeComparator(true, true));
	
			MergeSortIterator msi = new MergeSortIterator(msa);
			ConsolidatedDataIterator cdi = new ConsolidatedDataIterator(msi, ALIAS_POS, _sia.getTmpDir(), _sia.getBufSize());
			
			//debug
//			while(cdi.hasNext()){
//				Node[] next = cdi.next();
//				if(next[0].equals(new Resource("http://acm.rkbexplorer.com/id/person-682206-760726a52a4d40b394f1fe35d623ddb0")))
//				System.err.println(Nodes.toN3(next));
//			}
			
			MapTripleStore aBox = new MapTripleStore();

			MoreInfoProcessor mii = new MoreInfoProcessor(cdi, aBox, ifps, fps, predSpoc, predOpsc);
			
			InconLogger il = new InconLogger(mii, new PrintWriter(os));
			mii.setInconsistencyLogger(il);
			
			ReasonerEnvironment re = new ReasonerEnvironment(mii, mii);
			re.setAboxRuleIndex(lri);
			re.setInconsistencyHandler(il);
			re.setABox(aBox);
			re.setUniqueStatementFilter(mii);

			ReasonerSettings rs = new ReasonerSettings();
			rs.setSkipABox(false);
			rs.setSkipAxiomatic(true);
			rs.setSkipTBox(true);
			rs.setUseAboxRuleIndex(true);
			rs.setPrintContexts(false);
			rs.setTicks(60*1000);

			Reasoner r = new Reasoner(rs, re);
			
			_log.info("Starting reasoning on server "+_serverID);
			r.reason();
			_log.info("...finished reasoning on server "+_serverID);

			mii.logStats();
			il.logStats();
			
			repair = mii.getRepair();
			
			_log.info("=== Repair ===");
//			_log.info(mii.getRepair().toString());
			for(Map.Entry<Node,Map<Node,SameAsIndex.SameAsList>> e: mii.getRepair().entrySet()){
				TreeSet<SameAsList> sals = new TreeSet<SameAsList>();
				sals.addAll(e.getValue().values());
				
				TreeSet<Node> all = new TreeSet<Node>();
				for(SameAsList sal:sals)
					all.addAll(sal);
				
				System.err.print("Repair "+e.getKey()+" partition "+sals.size()+" IDs "+all.size());
				for(SameAsList sal:sals){
					System.err.print(" {");
					for(Node n:sal)
						System.err.print(" "+n.toN3());
					System.err.print(" }");
				}
				System.err.println();
			}
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error creating inconsistency output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating inconsistency output file on server "+_serverID+"\n"+e);
		}
		
		try{
			os.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing inconsistency output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing inconsistency output file on server "+_serverID+"\n"+e);
		}
		
		return repair;
	}

	public void repairInconsistencies(Map<Node, Map<Node, SameAsList>> repair)
			throws RemoteException {
		String spocF = _sia.getInSpoc();

		InputStream spocIs = null;
		Iterator<Node[]> spoc = null;
		try{
			spocIs = new FileInputStream(spocF);
			spocIs = new GZIPInputStream(spocIs); 

			spoc = new NxParser(spocIs);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+spocF+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+spocF+" on server "+_serverID+"\n"+e);
		}
		_log.info("...repairing input from "+spocF);
		
		
		String out = _sia.getOutData();
		Callback cb = null;
		OutputStream os = null;
		try{
			os = new FileOutputStream(out);
			if(_sia.getGzData())
				os = new GZIPOutputStream(os);
			
			cb = new CallbackNxOutputStream(os);
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening output file "+out+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening output file "+out+" on server "+_serverID+"\n"+e);
		}
		_log.info("...output to "+out);
		
		
		int repairN = 0, repairL = 0, done = 0;
		while(spoc.hasNext()){
			boolean rs = false;
			
			Node[] next = spoc.next();
			done++;
			
			if(done%1000000==0){
				_log.info("Read "+done+"; repaired line "+repairL+"; repaired node "+repairN);
			}
			Map<Node, SameAsList> newr = repair.get(next[0]);
			if(newr!=null){
				Node old = next[0];
				if(!next[4].equals(ConsolidationIterator.NO_REWRITE)) old = next[4];
				
				Node newp = null;
				if(newr.isEmpty()){
					//complete split
					newp = old;
				} else{
					SameAsList sal = newr.get(old);
					if(sal!=null){
						newp = sal.getPivot();
					} else{
						_log.severe("Could not find pivot for "+old);
						newp = old;
					}
				}
				
				if(!newp.equals(next[0])){
					rs = true;
					next[0] = newp;
					repairL++;
					repairN++;
					
					if(next[0].equals(next[4])) next[4] = ConsolidationIterator.NO_REWRITE;
				}
			}
			
			if(!(next[2] instanceof Literal)){
				newr = repair.get(next[2]);
				if(newr!=null){
					Node old = next[2];
					if(!next[5].equals(ConsolidationIterator.NO_REWRITE)) old = next[5];
					
					Node newp = null;
					if(newr.isEmpty()){
						//complete split
						newp = old;
					} else{
						SameAsList sal = newr.get(old);
						if(sal!=null){
							newp = sal.getPivot();
						} else{
							_log.severe("Could not find pivot for "+old);
							newp = old;
						}
					}
					
					if(!newp.equals(next[2])){
						next[2] = newp;
						if(!rs) repairL++;
						repairN++;
						
						if(next[2].equals(next[5])) next[5] = ConsolidationIterator.NO_REWRITE; 
					}
				}
			}
			
			cb.processStatement(next);
		}
		
		_log.info("Finished! Read "+done+"; repaired line "+repairL+"; repaired node "+repairN);

		try{
			os.close();
			spocIs.close();
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error closing file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing file on server "+_serverID+"\n"+e);
		}
		
	}
}

