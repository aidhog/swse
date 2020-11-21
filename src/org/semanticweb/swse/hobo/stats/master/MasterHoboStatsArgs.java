package org.semanticweb.swse.hobo.stats.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.hobo.stats.RMIHoboStatsServer;
import org.semanticweb.swse.hobo.stats.SlaveHoboStatsArgs;

public class MasterHoboStatsArgs extends MasterArgs<MasterHoboStatsArgs>{
	public static final RMIHoboStatsServer DEFAULT_RI = new RMIHoboStatsServer();
	public static final MasterHoboStats DEFAULT_MASTER = new MasterHoboStats();
	
	public static final int[] SPOC_ORDER = new int[]{0,1,2,3};
	public static final int[] OPSC_ORDER = new int[]{2,1,0,3};
	
	public static final String DEFAULT_TEMP_DIR = "tmp/";
	
	private String _rOutSpoc;
	private String _rOutOpsc;
	private String _statsOut;
	
	private boolean _skipBuild = false;
	private boolean _ignoreSameAs = false;
	
	private SlaveHoboStatsArgs _sesa = null;
	
	public MasterHoboStatsArgs(String outdir, SlaveHoboStatsArgs sra){
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
	
	public SlaveHoboStatsArgs getSlaveArgs(int server){
		return _sesa.instantiate(server);
	}
	
	public SlaveHoboStatsArgs getSlaveArgs(){
		return _sesa;
	}
	
	public String toString(){
		return "Master:: igsa:"+_ignoreSameAs+" sb:"+_skipBuild+" statsOut:"+_statsOut+" routSpoc:"+_rOutSpoc+" routOpsc:"+_rOutOpsc
			+"Slave:: "+_sesa;
	}
	
	public RMIHoboStatsServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterHoboStats getTaskMaster() {
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