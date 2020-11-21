package org.semanticweb.swse.lucene;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveLuceneArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = 2366745716801778239L;
	
	private String _outDir;
	private String _ranks;
	private String _index;
	private String _redirs;
	
	private boolean _gzranks = MasterArgs.DEFAULT_GZ_IN;
	private boolean _gzredirs = MasterArgs.DEFAULT_GZ_IN;
	
	public SlaveLuceneArgs(String index, String ranks, String redirs, String outDir){
		_index = index;
		_ranks = ranks;
		_redirs = redirs;
		_outDir = outDir;
	}
	
	public String getRedirects(){
		return _redirs;
	}
	
	public boolean getGzRanks(){
		return _gzranks;
	}
	
	public void setGzRanks(boolean gzranks){
		_gzranks = gzranks;
	}
	
	public boolean getGzRedirects(){
		return _gzredirs;
	}
	
	public void setGzRedirects(boolean gzredirs){
		_gzredirs = gzredirs;
	}
	
	public String getOutDir(){
		return _outDir;
	}
	
	public String getIndex(){
		return _index;
	}
	
	public String getRanks(){
		return _ranks;
	}
	
	public SlaveLuceneArgs instantiate(int server) {
		String index = RMIUtils.getLocalName(_index, server);
		String ranks = RMIUtils.getLocalName(_ranks, server);
		String outDir = RMIUtils.getLocalName(_outDir, server);
		
		SlaveLuceneArgs sla = new SlaveLuceneArgs(index, ranks, _redirs, outDir);
		sla.setGzRanks(_gzranks);
		sla.setGzRedirects(_gzredirs);
		sla.setSlaveLog(RMIUtils.getLocalName(_logFile, server));
		return sla;
	}
	
	public String toString(){
		return "outDir:"+_outDir+" redirs:"+_redirs+"-gz:"+_gzredirs+" index:"+_index+" ranks:"+_ranks+"-gz:"+_gzranks;
	}	
}