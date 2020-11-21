package org.semanticweb.swse.ldspider.master;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.semanticweb.swse.RMIClient;
import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIThreads;
import org.semanticweb.swse.RMIThread.VoidRMIThread;
import org.semanticweb.swse.ldspider.RMICrawlerConstants;
import org.semanticweb.swse.ldspider.RMICrawlerInterface;
import org.semanticweb.swse.ldspider.RMICrawlerServer;
import org.semanticweb.swse.ldspider.remote.RemoteCrawlerSetup;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.util.CallbackNxOutputStream;

import com.ontologycentral.ldspider.CrawlerConstants;

/**
 * Code for conducting the distributed crawl
 * @author aidhog
 *
 */
public class MasterCrawler {
	private final static Logger _log = Logger.getLogger(MasterCrawler.class.getSimpleName());

	private RMIRegistries _servers;
	private RMIClient<RMICrawlerInterface> _rmic;

	public MasterCrawler(RMIRegistries servers) throws RemoteException, NotBoundException{
		_servers = servers;
		_rmic = new RMIClient<RMICrawlerInterface>(servers, RMICrawlerConstants.DEFAULT_STUB_NAME);
	}

	public void start(Collection<String> seeds, String redirsFn, boolean redirsGz, RemoteCrawlerSetup[] rcs) throws Exception{
		start(seeds, redirsFn, redirsGz, rcs, CrawlerConstants.DEFAULT_MAX_URIS, CrawlerConstants.DEFAULT_NB_ROUNDS);
	}

	public void start(Collection<String> seeds, String redirsFn, boolean redirsGz, RemoteCrawlerSetup[] rcs, int targeturis, int rounds) throws Exception{
		Collection<RMICrawlerInterface> stubs = _rmic.getAllStubs();
		VoidRMIThread[] ibts = new VoidRMIThread[stubs.size()];
//		long idletime;
		
		OutputStream os = new FileOutputStream(redirsFn);
		if(redirsGz){
			os = new GZIPOutputStream(os);
		}
		CallbackNxOutputStream cbr = new CallbackNxOutputStream(os);

//		Iterator<RMICrawlerInterface> stubIter = null;
		_log.log(Level.INFO, "Initialising remote crawlers...");
		Iterator<RMICrawlerInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteCrawlerInitThread(stubIter.next(), i, _servers, rcs[i]);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" initialised...");
		}
		_log.log(Level.INFO, "...remote crawlers initialised.");
		long idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on init "+idletime+"...");
		_log.info("Average idle time for co-ordination on init "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Scattering seeds...");
		Collection<String>[] scatter = RMICrawlerServer.split(seeds, _servers);

		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteCrawlerScatterSeedsThread(stubIter.next(), i, scatter[i]);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "...seeds sent to "+i+"...");
		}
		_log.log(Level.INFO, "...seeds scattered.");
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on scattering "+idletime+"...");
		_log.info("Average idle time for co-ordination on scattering "+(double)idletime/(double)(ibts.length)+"...");


		int targetURIsPerMachine = Math.round((float)targeturis/(float)_servers.getServerCount());
		int totalsum = 0;
		int todo = targeturis;
		int r = 0;
		for(r=0; r<rounds && todo>0; r++){
			targetURIsPerMachine = (int)Math.ceil((double)(todo)/(double)_servers.getServerCount());
			
			_log.log(Level.INFO, "Running crawl round...");

			RemoteCrawlerRunRoundThread[] rcrrs = new RemoteCrawlerRunRoundThread[ibts.length];
			stubIter = stubs.iterator();
			EndRoundCallback erc = new EndRoundCallback(stubs);
			for(int i=0; i<ibts.length; i++){
				rcrrs[i] = new RemoteCrawlerRunRoundThread(stubIter.next(), i, targetURIsPerMachine);
				rcrrs[i].setFinishedCallback(erc);
				rcrrs[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			int roundsum = 0;
			for(int i=0; i<rcrrs.length; i++){
				rcrrs[i].join();
				if(!rcrrs[i].successful()){
					throw rcrrs[i].getException();
				}
				roundsum+=rcrrs[i].getResult();
				_log.log(Level.INFO, "...round "+r+" complete on "+i+" with "+rcrrs[i].getResult()+" polled...");
			}
			
			_log.log(Level.INFO, "...remote crawl round done -- polled "+roundsum+" uris.");
			totalsum+=roundsum;
			todo-=roundsum;
			
			idletime = RMIThreads.idleTime(rcrrs);
			_log.info("Total idle time for co-ordination on run round "+idletime+"...");
			_log.info("Average idle time for co-ordination on run round "+(double)idletime/(double)(rcrrs.length)+"...");
			
			_log.log(Level.INFO, "Getting redirects...");

			RemoteCrawlerGetRoundRedirects[] rcgrrs = new RemoteCrawlerGetRoundRedirects[ibts.length];
			stubIter = stubs.iterator();
			for(int i=0; i<ibts.length; i++){
				rcgrrs[i] = new RemoteCrawlerGetRoundRedirects(stubIter.next(), i);
				rcgrrs[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			int totalRedirs = 0;
			for(int i=0; i<rcrrs.length; i++){
				rcgrrs[i].join();
				if(!rcgrrs[i].successful()){
					throw rcgrrs[i].getException();
				}
				Map<URI,URI> redirs = rcgrrs[i].getResult();
				totalRedirs+=redirs.size();
				
				for(Map.Entry<URI,URI> redir:redirs.entrySet()){
					try{
						cbr.processStatement(
								new Node[]{
										new Resource(redir.getKey().toASCIIString()), 
										new Resource(redir.getValue().toASCIIString())
								}
						);
					} catch(NullPointerException e){
						_log.severe("Error parsing redirect URI pair "+redir.getKey()+" - "+redir.getValue()+"\n"+e.getMessage());
						e.printStackTrace();
					}
				}
				
				_log.log(Level.INFO, "...received "+redirs.size()+" redirects from "+i+"...");
			}
			
			os.flush();
			
			_log.log(Level.INFO, "...got total "+totalRedirs+" redirects.");
			
			idletime = RMIThreads.idleTime(rcgrrs);
			_log.info("Total idle time for co-ordination on run round "+idletime+"...");
			_log.info("Average idle time for co-ordination on run round "+(double)idletime/(double)(rcgrrs.length)+"...");
			
			_log.log(Level.INFO, "Scattering...");

			stubIter = stubs.iterator();
			for(int i=0; i<ibts.length; i++){
				ibts[i] = new RemoteCrawlerScatterThread(stubIter.next(), i);
				ibts[i].start();
			}

			_log.log(Level.INFO, "...awaiting thread return...");
			for(int i=0; i<ibts.length; i++){
				ibts[i].join();
				if(!ibts[i].successful()){
					throw ibts[i].getException();
				}
				_log.log(Level.INFO, "..."+i+" scattered...");
			}
			_log.log(Level.INFO, "...remote crawlers have scattered URIs.");
			
			idletime = RMIThreads.idleTime(ibts);
			_log.info("Total idle time for co-ordination on scattering "+idletime+"...");
			_log.info("Average idle time for co-ordination on scattering "+(double)idletime/(double)(ibts.length)+"...");
		}
		
		if(todo<=0)
			_log.info("Reached target URIs of "+targeturis+": crawled "+totalsum);
		if(r==rounds)
			_log.info("Reached target rounds of "+r+".");
		
		os.close();
		
		_log.log(Level.INFO, "Finishing...");

		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteCrawlerFinishThread(stubIter.next(), i);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" finished...");
		}
		_log.log(Level.INFO, "...remote crawlers have finished.");
		_log.log(Level.INFO, "Distributed crawl done!!!!");
		
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination "+idletime+"...");
		_log.info("Average idle time for co-ordination "+(double)idletime/(double)(ibts.length)+"...");
		
		_rmic.clear();
	}
	
	public void abort() throws Exception{
		Collection<RMICrawlerInterface> stubs = _rmic.getAllStubs();
		VoidRMIThread[] ibts = new VoidRMIThread[stubs.size()];

		_log.log(Level.INFO, "Ending round on remote crawlers...");
		Iterator<RMICrawlerInterface> stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteCrawlerEndRoundThread(stubIter.next(), i, true);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" round ended...");
		}
		_log.log(Level.INFO, "...remote crawlers round ended.");
		long idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on endRound "+idletime+"...");
		_log.info("Average idle time for co-ordination on endRound "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Scattering...");

		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteCrawlerScatterThread(stubIter.next(), i);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" scattered...");
		}
		_log.log(Level.INFO, "...remote crawlers have scattered URIs.");
			
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination on scattering "+idletime+"...");
		_log.info("Average idle time for co-ordination on scattering "+(double)idletime/(double)(ibts.length)+"...");
		
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination "+idletime+"...");
		_log.info("Average idle time for co-ordination "+(double)idletime/(double)(ibts.length)+"...");

		_log.log(Level.INFO, "Finishing...");

		stubIter = stubs.iterator();
		for(int i=0; i<ibts.length; i++){
			ibts[i] = new RemoteCrawlerFinishThread(stubIter.next(), i);
			ibts[i].start();
		}

		_log.log(Level.INFO, "...awaiting thread return...");
		for(int i=0; i<ibts.length; i++){
			ibts[i].join();
			if(!ibts[i].successful()){
				throw ibts[i].getException();
			}
			_log.log(Level.INFO, "..."+i+" finished...");
		}
		_log.log(Level.INFO, "...remote crawlers aborted.");
		_log.log(Level.INFO, "Distributed crawl done!!!!");
		
		idletime = RMIThreads.idleTime(ibts);
		_log.info("Total idle time for co-ordination "+idletime+"...");
		_log.info("Average idle time for co-ordination "+(double)idletime/(double)(ibts.length)+"...");
	}
}
