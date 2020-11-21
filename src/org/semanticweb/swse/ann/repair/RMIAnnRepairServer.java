package org.semanticweb.swse.ann.repair;
//import java.rmi.RemoteException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.saorr.Statement;
import org.semanticweb.saorr.ann.AnnotatedStatement;
import org.semanticweb.saorr.ann.domains.RankAnnotation;
import org.semanticweb.saorr.ann.domains.RankAnnotationFactory;
import org.semanticweb.saorr.ann.engine.AnnotationReasoner;
import org.semanticweb.saorr.ann.engine.AnnotationReasonerEnvironment;
import org.semanticweb.saorr.ann.engine.unique.UniqueAnnotatedStatementFilter;
import org.semanticweb.saorr.ann.engine.unique.UniquingAnnotationHashset;
import org.semanticweb.saorr.ann.index.StatementAnnotationMap;
import org.semanticweb.saorr.ann.rules.AnnotationRule;
import org.semanticweb.saorr.engine.ReasonerSettings;
import org.semanticweb.saorr.engine.ih.InconsistencyException;
import org.semanticweb.saorr.engine.input.FileInput;
import org.semanticweb.saorr.engine.input.NxGzInput;
import org.semanticweb.saorr.engine.input.NxaGzInput;
import org.semanticweb.saorr.rules.LinkedRuleIndex;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.saorr.rules.SortedRuleSet;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.reason.utils.ResetableFlyweightNodeIterator;
import org.semanticweb.swse.ann.repair.master.MasterAnnRepair;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.parser.Callback;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.ontologycentral.ldspider.hooks.content.CallbackDummy;


/**
 * Takes calls from the stub and translates into consolidation actions.
 * 
 * @author aidhog
 */
public class RMIAnnRepairServer implements RMIAnnRepairInterface {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4983859112724015479L;
	private final static Logger _log = Logger.getLogger(RMIAnnRepairServer.class.getSimpleName());
	public final static int TICKS = 1000000;
	
	private transient int _serverID = -1;
	private transient RMIRegistries _servers;

	private transient SlaveAnnRepairArgs _sra;
	
	LinkedRuleIndex<AnnotationRule<RankAnnotation>> _tmplRules;

	public RMIAnnRepairServer(){
		;
	}

	public void init(int serverId, RMIRegistries servers, SlaveAnnRepairArgs sra) throws RemoteException {
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
		
		RMIUtils.mkdirsForFile(sra._outFinal);
		
		_servers = servers;
		_serverID = serverId;
		_servers.setThisServer(serverId);
		
		_log.log(Level.INFO, "Loading tbox...");
		
		_log.info("Loading tbox...");
		long time = System.currentTimeMillis();
		try{
			
			FileInput tboxIn = new NxaGzInput(new File(sra._inTbox),4);
	
			ResetableFlyweightNodeIterator fwiter = new ResetableFlyweightNodeIterator(1000, tboxIn);
	
			Rules rs = new Rules(MasterAnnRepair.DEFAULT_RULES);
			Rules abox = new Rules(rs.getAboxRules());
			abox.setAuthoritative();
	
			CallbackDummy cb = new CallbackDummy();
	
			_tmplRules = MasterAnnRepair.buildTbox(fwiter, abox, cb, true);
			_tmplRules.freeResources();
			
			tboxIn.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error loading tbox file "+_sra._inTbox+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error loading tbox file "+_sra._inTbox+" on server "+_serverID+"\n"+e);
		}
		_log.log(Level.INFO, "...loaded tbox and created template rules in "+(System.currentTimeMillis()-time)+" ms");

		
		_log.log(Level.INFO, "Connected.");
	}

	public int getServerID(){
		return _serverID;
	}
	
	public static void startRMIServer(String hostname, int port, String stubname) throws IOException, ClassNotFoundException, AlreadyBoundException{
    	RMIAnnRepairServer rmi = new RMIAnnRepairServer();
    	
    	RMIAnnRepairInterface stub = (RMIAnnRepairInterface) UnicastRemoteObject.exportObject(rmi, 0);

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
		_tmplRules = null;
	}

	public StatementAnnotationMap<RankAnnotation> getDeltaPlus(
			StatementAnnotationMap<RankAnnotation> delta)
			throws RemoteException {
		FileInput aboxIn = null;
		try{
			aboxIn = new NxGzInput(new File(_sra._inFinal));
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
		}
		
		
		HashSet<Node> dataNodes = new HashSet<Node>();
		for(Map.Entry<Statement, RankAnnotation> e:delta.entrySet()){
			dataNodes.add(e.getKey().subject);
			if(!RDF.TYPE.equals(e.getKey().predicate)){
				dataNodes.add(e.getKey().object);
			}
		}
		
		ReasonerSettings rset = new ReasonerSettings();
		rset.setSkipTBox(true);
		rset.setSkipAxiomatic(true);
		rset.setPrintContexts(true);
		
		CallbackDummy cs = new CallbackDummy();
		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(aboxIn, RankAnnotationFactory.SINGLETON, cs);
		re.setAboxRuleIndex(_tmplRules);
		rset.setUseAboxRuleIndex(true);

		UniqueAnnotatedStatementFilter<RankAnnotation> uasf = new UniquingAnnotationHashset<RankAnnotation>();
		re.setUniqueStatementFilter(uasf);
		
		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rset, re);
		RankAnnotationFactory raf = RankAnnotationFactory.SINGLETON;
		
		SortedRuleSet<AnnotationRule<RankAnnotation>> aboxRules = new SortedRuleSet<AnnotationRule<RankAnnotation>>();
		
		StatementAnnotationMap<RankAnnotation>  deltaPlus = new StatementAnnotationMap<RankAnnotation>();
		int count =0, inf = 0, skip1 = 0, skip2 = 0, notskip = 0;
		while(aboxIn.hasNext()){
			Node[] next = aboxIn.next();
			
//			if(next[0].equals(new Resource("http://dunken69.myopenid.com/")) && next[1].equals(FOAF.TOPIC)){
//				System.err.println(Nodes.toN3(next));
//			}
//			
//			if(next[0].equals(new Resource("http://sw.deri.org/~knud/knudfoaf.rdf")) && next[1].equals(new Resource("http://xmlns.com/foaf/0.1/openid"))){
//				System.err.println(Nodes.toN3(next));
//			}
			
			count++;
			if(count%TICKS==0){
				_log.info("Read "+count+" input and "+inf+" inferred and skipped1 "+skip1+" skipped2 "+skip2+" but not skipped "+notskip+"...");
			}
			
			if(!MasterAnnRepair.intersects(next, dataNodes)){
				skip1++;
				continue;
			} else{
				notskip++;
			}
			
			AnnotatedStatement<RankAnnotation> as = raf.fromNodes(next);
			if(delta.getAnnotation(as)!=null){
				skip2++;
				continue;
			}
			HashSet<AnnotatedStatement<RankAnnotation>> set = new HashSet<AnnotatedStatement<RankAnnotation>>();
			
			try{
				r.reasonAbox(as, aboxRules, set);
			} catch(InconsistencyException ie){
				_log.severe(ie.getMessage());
			}
		
//			System.err.println(_serverID+"  "+as);
			boolean dp = false;
			for(AnnotatedStatement<RankAnnotation> s:set){
				inf++;
//				System.err.println(" "+_serverID+"  "+s);
				RankAnnotation ra = delta.getAnnotation(s);
				if(ra!=null){
					deltaPlus.putAnnotation(as);
					dp = true;
				}
			}
			
			if(dp){
				uasf.clear();
//				for(AnnotatedStatement<RankAnnotation> s:set){
//					uasf.remove(s);
//				}
//				uasf.remove(as);
			}
		}
		
		_log.info("Read "+count+" input and "+inf+" inferred and skipped1 "+skip1+" skipped2 "+skip2+" but not skipped "+notskip+"...");
		
		try{
			aboxIn.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
		}
		
		return deltaPlus;
	}

	public ArrayList<StatementAnnotationMap<RankAnnotation>> getDeltaDMinus(
			StatementAnnotationMap<RankAnnotation> delta,
			StatementAnnotationMap<RankAnnotation> deltaPlus)
			throws RemoteException {
		FileInput aboxIn = null;
		try{
			aboxIn = new NxGzInput(new File(_sra._inRaw));
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
		}
		
//		System.err.println("Delta "+delta.entrySet());
//		System.err.println("Delta+ "+deltaPlus.entrySet());
		
		
		ReasonerSettings rset = new ReasonerSettings();
		rset.setSkipTBox(true);
		rset.setSkipAxiomatic(true);
		rset.setPrintContexts(true);
		
		CallbackDummy cs = new CallbackDummy();
		AnnotationReasonerEnvironment<RankAnnotation> re = new AnnotationReasonerEnvironment<RankAnnotation>(aboxIn, RankAnnotationFactory.SINGLETON, cs);
		re.setAboxRuleIndex(_tmplRules);
		rset.setUseAboxRuleIndex(true);

		UniqueAnnotatedStatementFilter<RankAnnotation> uasf = new UniquingAnnotationHashset<RankAnnotation>();
		re.setUniqueStatementFilter(uasf);
		
		AnnotationReasoner<RankAnnotation> r = new AnnotationReasoner<RankAnnotation>(rset, re);
		RankAnnotationFactory raf = RankAnnotationFactory.SINGLETON;
		
		SortedRuleSet<AnnotationRule<RankAnnotation>> aboxRules = new SortedRuleSet<AnnotationRule<RankAnnotation>>();
		
		StatementAnnotationMap<RankAnnotation> deltaD = new StatementAnnotationMap<RankAnnotation>();
		StatementAnnotationMap<RankAnnotation> removeDeltaPlus = new StatementAnnotationMap<RankAnnotation>();
		
		
		int skip = 0, skipI = 0, skip1 = 0, notskip = 0;
		int remove = 0, removeDD = 0, fresh = 0, update = 0, stale = 0;
		
		int count = 0, inf = 0;
		
		HashSet<Node> dataNodes = new HashSet<Node>();
		for(Map.Entry<Statement, RankAnnotation> e:deltaPlus.entrySet()){
			dataNodes.add(e.getKey().subject);
			if(!RDF.TYPE.equals(e.getKey().predicate)){
				dataNodes.add(e.getKey().object);
			}
		}
		
		while(aboxIn.hasNext()){
			Node[] next = aboxIn.next();
			count++;
			if(count%TICKS==0){
				_log.info("Read "+count+" input and "+inf+" inferred "+skip+" skipDelta "+skip1+" skipSchema "+notskip+" notSkip ");
			}
			
			if(!MasterAnnRepair.intersects(next, dataNodes)){
				skip1++;
				continue;
			} else{
				notskip++;
			}
			
			AnnotatedStatement<RankAnnotation> as = raf.fromNodes(next);
			HashSet<AnnotatedStatement<RankAnnotation>> set = new HashSet<AnnotatedStatement<RankAnnotation>>();
			set.add(as);
			
			if(delta.getAnnotation(as)!=null){
				skip++;
				continue;
			}
			try{
				r.reasonAbox(as, aboxRules, set);
			} catch(InconsistencyException ie){
				_log.severe(ie.getMessage());
			}
		
			
			for(AnnotatedStatement<RankAnnotation> s:set){
				inf++;
				
				if(delta.getAnnotation(s)!=null){
					skipI++;
					break;
				}
				
//				if(s.subject.equals(new Resource("http://dunken69.myopenid.com/")) && s.predicate.equals(FOAF.TOPIC)){
//					System.err.println(Nodes.toN3(next));
//				}
//				
//				if(s.subject.equals(RDFS.RESOURCE) && s.predicate.equals(RDF.TYPE) && s.object.equals(RDFS.RESOURCE)){
//					System.err.println(Nodes.toN3(next));
//				}
				
				RankAnnotation dp = deltaPlus.getAnnotation(s);
				if(dp!=null){
					if(s.getAnnotation().invalidates(dp)){
						if(deltaD.removeAnnotation(s)!=null)
							removeDD++;
						removeDeltaPlus.putAnnotation(s);
						remove++;
					} else{
						RankAnnotation dd = deltaD.getAnnotation(s);
						if(dd==null){
							deltaD.putAnnotation(s);
							fresh++;
						} else if(!dd.invalidates(s.getAnnotation())){
							deltaD.putAnnotation(s);
							update++;
						} else{
							stale++;
						}
					}
				}
			}
//			uasf.clear();
		}
		_log.info("Read "+count+" input and "+inf+" inferred.");
		_log.info("Removed from DeltaPlus "+remove);
		_log.info("Removed from DeltaD "+removeDD);
		_log.info("Fresh to DeltaD "+fresh);
		_log.info("Update to DeltaD "+update);
		_log.info("Stale from DeltaD "+stale);
		_log.info("Skipped correctly "+skip);
		_log.info("Skipped incorrectly "+skipI);
		_log.info("Skipped irrelevant "+skip1);
		
		try{
			aboxIn.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
		}
		
		ArrayList<StatementAnnotationMap<RankAnnotation>> ans = new ArrayList<StatementAnnotationMap<RankAnnotation>>();
		ans.add(removeDeltaPlus);
		ans.add(deltaD);
		
		return ans;
	}

	public double[] repair(StatementAnnotationMap<RankAnnotation> deltaAll,
			StatementAnnotationMap<RankAnnotation> deltaD) throws RemoteException{
		FileInput aboxIn = null;
		try{
			aboxIn = new NxGzInput(new File(_sra._inFinal));
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error opening input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error opening input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
		}
		
		Callback cb = null;
		OutputStream os = null;
		try{
			os = new GZIPOutputStream(new FileOutputStream(_sra._outFinal));
			cb = new CallbackNxOutputStream(os);
		}catch(Exception e){
			_log.log(Level.SEVERE, "Error creating reasoning output file on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error creating reasoning output file on server "+_serverID+"\n"+e);
		}
		
		RankAnnotationFactory raf =  RankAnnotationFactory.SINGLETON;
		
		int remove = 0, update = 0, buffer = 0, wrote = 0, read = 0;
		double rankRemoved = 0, maxRankRemoved = 0, rankDiff = 0;
		while(aboxIn.hasNext()){
			Node[] next = aboxIn.next();
			read++;
			
			if(read%TICKS==0){
				_log.info("Read "+read+"...");
			}
			
			AnnotatedStatement<RankAnnotation> as = raf.fromNodes(next);
			
			if(deltaAll.getAnnotation(as)!=null){
				remove++;
				double removeRank = as.getAnnotation().getRank();
				if(removeRank>maxRankRemoved)
					maxRankRemoved = removeRank;
				rankRemoved+=removeRank;
				continue;
			} 
			
			RankAnnotation dd = deltaD.getAnnotation(as);
			if(dd!=null){
				if(!as.getAnnotation().invalidates(dd)){
					_log.severe("Huh? upgrading rank? "+dd+" "+as);
				} else{
					update++;
					double diffRank = as.getAnnotation().getRank() - dd.getRank();
					rankDiff+=diffRank;
					as = new AnnotatedStatement<RankAnnotation>(as, dd);
				}
			} else{
				buffer++;
			}
			
			wrote++;
			cb.processStatement(as.toAnnotatedNodeArray());
		}
		_log.info("Read "+read);
		_log.info("Wrote "+wrote);
		_log.info("Buffer "+buffer);
		_log.info("Update "+update);
		_log.info("Total diff in rank updated "+rankDiff);
		_log.info("Remove "+remove);
		_log.info("Total rank removed "+rankRemoved);
		_log.info("Max rank removed "+maxRankRemoved);
		
		
		try{
			aboxIn.close();
			os.close();
		} catch(Exception e){
			_log.log(Level.SEVERE, "Error closing input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
			e.printStackTrace();
			throw new RemoteException("Error closing input file "+_sra._inFinal+" on server "+_serverID+"\n"+e);
		}
		
		
		double[] ans = new double[8];
		ans[0] = read;
		ans[1] = wrote;
		ans[2] = buffer;
		ans[3] = update;
		ans[4] = rankDiff;
		ans[5] = remove;
		ans[6] = rankRemoved;
		ans[7] = maxRankRemoved;
		return ans;
	}
}
