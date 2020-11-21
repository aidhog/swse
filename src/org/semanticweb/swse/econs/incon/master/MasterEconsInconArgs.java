package org.semanticweb.swse.econs.incon.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.econs.incon.RMIEconsInconServer;
import org.semanticweb.swse.econs.incon.SlaveEconsInconArgs;

public class MasterEconsInconArgs extends MasterArgs<MasterEconsInconArgs>{
	public static final RMIEconsInconServer DEFAULT_RI = new RMIEconsInconServer();
	public static final MasterEconsIncon DEFAULT_MASTER = new MasterEconsIncon();
	
	public static final String DEFAULT_TEMP_DIR = "tmp/";
	
	private SlaveEconsInconArgs _sesa = null;
	
	public MasterEconsInconArgs(SlaveEconsInconArgs sra){
		_sesa = sra;
//		initDefault(outdir);
	}
	
//	private void initDefault(String outdir){
//		_rOutSpoc = getDefaultRemoteSpocFile(outdir);
//		_rOutOpsc = getDefaultRemoteOpscFile(outdir);
//		_statsOut = getDefaultStatsFile(outdir);
//	}
	
	public SlaveEconsInconArgs getSlaveArgs(int server){
		return _sesa.instantiate(server);
	}
	
	public SlaveEconsInconArgs getSlaveArgs(){
		return _sesa;
	}
	
	public String toString(){
		return "Master:: \n"
			+"Slave:: "+_sesa;
	}
	
	public RMIEconsInconServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterEconsIncon getTaskMaster() {
		return DEFAULT_MASTER;
	}
	
//	public static final String getDefaultTmpDir(String outdir){
//		return RMIUtils.getLocalName(outdir)+"/"+DEFAULT_TEMP_DIR;
//	}
//	
//	public static final String getDefaultRemoteSpocFile(String outdir){
//		return outdir+"/spoc.s.nq.gz";
//	}
//	
//	public static final String getDefaultRemoteOpscFile(String outdir){
//		return outdir+"/opsc.s.nq.gz";
//	}
//	
//	public static final String getDefaultStatsFile(String outdir){
//		return RMIUtils.getLocalName(outdir)+"/stats.jo.gz";
//	}
}