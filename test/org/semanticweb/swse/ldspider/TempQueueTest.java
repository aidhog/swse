package org.semanticweb.swse.ldspider;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.semanticweb.swse.cli.RunRemoteCrawl;
import org.semanticweb.swse.ldspider.remote.queue.TempQueue;
import org.semanticweb.swse.ldspider.remote.utils.ErrorHandlerCounter;
import org.semanticweb.swse.ldspider.remote.utils.LinkFilter;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;

import com.ontologycentral.ldspider.hooks.error.ErrorHandlerDummy;
import com.ontologycentral.ldspider.http.ConnectionManager;
import com.ontologycentral.ldspider.tld.TldManager;

public class TempQueueTest {
	private static final String SEEDS_FILE = "testdata/uris.txt.gz";
	private static final boolean SEEDS_GZIPPED = true;
	private static final boolean SCORE_STRUCTURED = true;
	
	
//	public void testRemoteCrawl() throws IOException, ClassNotFoundException, AlreadyBoundException{
	public static void main(String[] args) throws Exception{
		
//		System.err.println(LinkFilter.normalise(new URI("http://140.203.154.209/~vit/foaf.rdf")));
		TldManager tldm = new TldManager(new ConnectionManager(null, -1, null, null, 20));
//		System.err.println(tldm.getPLD(new URI("http://140.203.154.209/~vit/foaf.rdf")));
		
		Logger log = Logger.getLogger(LinkFilter.class.getName());
		log.setLevel(Level.WARNING);
		
		Set<String> seeds = null;
		File seedList = new File(SEEDS_FILE);
		if(!seedList.exists()) 
			throw new FileNotFoundException("No file found at "+seedList.getAbsolutePath());
		seeds = RunRemoteCrawl.readSeeds(seedList, SEEDS_GZIPPED);
		
		PldManager pldm = new PldManager();
		
		for(String s:seeds){
			String pld = tldm.getPLD(new URI(s));
			Random r = new Random();
			
			if(pld==null){
				System.err.println(" "+null+" "+s);
			}
			
			pldm.incrementUseful(pld, r.nextInt(100));
			pldm.incrementUseless(pld, r.nextInt(100));
		}
		
//		pldm.logStats();
		pldm.newRound();
		
		TempQueue q = new TempQueue(100, 10, 5000, 500l, pldm, new PldManager(), SCORE_STRUCTURED, new ErrorHandlerCounter(new ErrorHandlerDummy(), null));

		for(String s:seeds){
			q.tryAdd(new URI(s), tldm.getPLD(new URI(s)));
		}
		
		System.err.println(pldm.size());
		System.err.println("Scheduled "+q.size()+" URIs from "+q.domains()+" domains");
		
		URI u;
		Random r = new Random();
		
		PrintWriter pw = new PrintWriter(new FileWriter("testdata/uris.10pld.txt"));
		while((u = q.poll())!=null){
			String pld = tldm.getPLD(u);
			pw.println(u.toString());
			synchronized (TempQueueTest.class)
			{
				int ri = r.nextInt(10);
				if(ri!=0){
//					TempQueueTest.class.wait(ri);
				}
			}
			System.err.println(pld+" "+pldm.getScore(pld)+" "+q.getPLDQueueSize(pld));
		}
		pw.close();
	}
}
