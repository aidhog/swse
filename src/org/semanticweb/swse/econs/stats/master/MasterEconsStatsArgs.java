package org.semanticweb.swse.econs.stats.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.rank.RMIAnnRankingServer;
import org.semanticweb.swse.econs.stats.SlaveEconsStatsArgs;

public class MasterEconsStatsArgs extends MasterArgs<MasterEconsStatsArgs>{
	public static final RMIAnnRankingServer DEFAULT_RI = new RMIAnnRankingServer();
	public static final MasterEconsStats DEFAULT_MASTER = new MasterEconsStats();
	
	public static final int[] SPOC_ORDER = new int[]{0,1,2,3};
	public static final int[] OPSC_ORDER = new int[]{2,1,0,3};
	
	public static final String DEFAULT_TEMP_DIR = "tmp/";
	
	private String _rOutSpoc;
	private String _rOutOpsc;
	private String _statsOut;
	
	private boolean _skipBuild = false;
	private boolean _ignoreSameAs = false;
	
	private int[] _opscOrder = OPSC_ORDER;
	private int[] _spocOrder = SPOC_ORDER;
	
	private SlaveEconsStatsArgs _sesa = null;
	
	public MasterEconsStatsArgs(String outdir, SlaveEconsStatsArgs sra){
		_sesa = sra;
		initDefault(outdir);
	}
	
	private void initDefault(String outdir){
		_rOutSpoc = getDefaultRemoteSpocFile(outdir);
		_rOutOpsc = getDefaultRemoteOpscFile(outdir);
		_statsOut = getDefaultStatsFile(outdir);
	}
	
	public String getRemoteOutSpoc(){
		return _rOutSpoc;
	}
	
	public String getRemoteOutOpsc(){
		return _rOutOpsc;
	}
	
	public String getStatsOut(){
		return _statsOut;
	}
	
	public void setOpscOrder(int[] opsc){
		_opscOrder = opsc;
	}
	
	public void setSpocOrder(int[] spoc){
		_spocOrder = spoc;
	}
	
	public int[] getOpscOrder(){
		return _opscOrder;
	}
	
	public int[] getSpocOrder(){
		return _spocOrder;
	}
	
	public void setSkipBuild(boolean skipBuild){
		_skipBuild = skipBuild;
	}
	
	public boolean getSkipBuild(){
		return _skipBuild;
	}
	
	public void setIgnoreSameAs(boolean ignoreSameas){
		_ignoreSameAs = ignoreSameas;
	}
	
	public boolean getIgnoreSameAs(){
		return _ignoreSameAs;
	}
	
	public SlaveEconsStatsArgs getSlaveArgs(int server){
		return _sesa.instantiate(server);
	}
	
	public SlaveEconsStatsArgs getSlaveArgs(){
		return _sesa;
	}
	
	public String toString(){
		return "Master:: igsa:"+_ignoreSameAs+" sb:"+_skipBuild+" statsOut:"+_statsOut+" routSpoc:"+_rOutSpoc+" routOpsc:"+_rOutOpsc
			+"Slave:: "+_sesa;
	}
	
	public RMIAnnRankingServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterEconsStats getTaskMaster() {
		return DEFAULT_MASTER;
	}
	
	public static final String getDefaultTmpDir(String outdir){
		return RMIUtils.getLocalName(outdir)+"/"+DEFAULT_TEMP_DIR;
	}
	
	public static final String getDefaultRemoteSpocFile(String outdir){
		return outdir+"/spoc.s.nq.gz";
	}
	
	public static final String getDefaultRemoteOpscFile(String outdir){
		return outdir+"/opsc.s.nq.gz";
	}
	
	public static final String getDefaultStatsFile(String outdir){
		return RMIUtils.getLocalName(outdir)+"/stats.jo.gz";
	}
}