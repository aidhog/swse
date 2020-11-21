package org.semanticweb.swse.lucene.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.lucene.RMILuceneServer;
import org.semanticweb.swse.lucene.SlaveLuceneArgs;

public class MasterLuceneArgs extends MasterArgs<MasterLuceneArgs>{
	public static final RMILuceneServer DEFAULT_RI = new RMILuceneServer();
	public static final MasterLucene DEFAULT_MASTER = new MasterLucene();
	
	private SlaveLuceneArgs _sla = null;
	
	public MasterLuceneArgs(SlaveLuceneArgs sla){
		_sla = sla;
	}
	
	public SlaveLuceneArgs getSlaveArgs(int server){
		return _sla.instantiate(server);
	}
	
	public String toString(){
		StringBuffer buf = new StringBuffer();
		buf.append("Master:: N/A\n");
		buf.append("Slave:: "+_sla);
		
		return buf.toString();
	}
	
	public RMILuceneServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterLucene getTaskMaster() {
		return DEFAULT_MASTER;
	}
}