package org.semanticweb.swse.qp;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.TestCase;

import org.semanticweb.swse.RMIRegistries;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ldspider.remote.utils.PldManager;
import org.semanticweb.swse.qp.RMIQueryConstants;
import org.semanticweb.swse.qp.RMIQueryServer;
import org.semanticweb.swse.qp.master.MasterQuery;
import org.semanticweb.swse.qp.utils.QueryProcessor;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Nodes;
import org.semanticweb.yars.nx.Resource;

public class RMIRemoteQueryTest extends TestCase{
//	private static final String LUCENE = "/data/webdb/crawl/big3/index/lucene/";

//	private static final String SPOC = "/data/webdb/crawl/big3/index/spoc.nqz";

//	private static final String SPARSE = "/data/webdb/crawl/big3/index/spoc.sparse.nqz";

//	private static final String LUCENE = "testdata/index%/lucene/";
//
//	private static final String SPOC = "testdata/index%/spoc.nqz";
//
//	private static final String SPARSE = "testdata/index%/spoc.sparse.nxz";
	
	private static final String LUCENE = "testdata/swse%/index/lucene/";

	private static final String SPOC = "testdata/swse%/index/spoc.nqz";

	private static final String SPARSE = "testdata/swse%/index/spoc.sparse.nxz";

	private static final String KEYWORDQ = "aidan hogan";

	private static final String LANG = QueryProcessor.DEFAULT_LANG;

	private static final Resource FOCUSQ = new Resource("http://xmlns.com/foaf/0.1/Person");

	private static final int FROM = 1;

	private static final int TO = 5;

	private static String[] SERVERS = new String[]{
//		"deri-srvgal21.nuigalway.ie",
//		"deri-srvgal22.nuigalway.ie",
//		"deri-srvgal23.nuigalway.ie",
//		"deri-srvgal24.nuigalway.ie",
//		"deri-srvgal25.nuigalway.ie",
//		"deri-srvgal26.nuigalway.ie",
//		"deri-srvgal27.nuigalway.ie",
//		"deri-srvgal28.nuigalway.ie"
		"localhost:1801",
		"localhost:1802",
		"localhost:1803",
		"localhost:1804"
	};

	public static void main(String[] args) throws Exception{
		Logger log = Logger.getLogger(PldManager.class.getName());
		log.setLevel(Level.WARNING);

		for(String s:SERVERS){
			System.err.println("Setting up server "+s+"...");
			if(s.contains(":")){
				String[] sp = s.split(":");
				String host = sp[0];
				int port = Integer.parseInt(sp[1]);

				if(sp[0].equals("localhost")){
					RMIUtils.startRMIRegistry(port);
					RMIQueryServer.startRMIServer(host, port, RMIQueryConstants.DEFAULT_STUB_NAME);
				}
			}
			else{
				if(s.equals("localhost")){
					RMIUtils.startRMIRegistry(RMIQueryConstants.DEFAULT_RMI_PORT);
					RMIQueryServer.startRMIServer(s, RMIQueryConstants.DEFAULT_RMI_PORT, RMIQueryConstants.DEFAULT_STUB_NAME);
				}

			}
			System.err.println("...set up server "+s);
		}


		RMIRegistries servers = new RMIRegistries(SERVERS, RMIQueryConstants.DEFAULT_RMI_PORT);

		MasterQuery mq = new MasterQuery(servers, LUCENE, SPOC, SPARSE);

		Iterator<Node[]> iter = null;

		//test caching
//		for(int i=0; i<2; i++){
			System.out.println("=======================KEYWORD=======================");
			iter = mq.keywordQuery(KEYWORDQ, FROM, TO, LANG);
		
			while(iter.hasNext()){
				System.out.println(Nodes.toN3(iter.next()));
			}
		
			System.out.println("=======================FOCUS=======================");
			iter = mq.focus(FOCUSQ, LANG);
		
			while(iter.hasNext()){
				System.out.println(Nodes.toN3(iter.next()));
			}
//		}

		System.err.println("done");
	}
}
