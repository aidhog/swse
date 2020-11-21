package org.semanticweb.swse.econs.sim.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.econs.sim.RMIEconsSimServer;
import org.semanticweb.swse.econs.sim.SlaveEconsSimArgs;

public class MasterEconsSimArgs extends MasterArgs<MasterEconsSimArgs>{
	public static final RMIEconsSimServer DEFAULT_RI = new RMIEconsSimServer();
	public static final MasterEconsSim DEFAULT_MASTER = new MasterEconsSim();
	
	public static final String DEFAULT_TEMP_DIR = "tmp/";
	
	private String _psFn;
	
	private boolean _aggOnly;
	
	private SlaveEconsSimArgs _ssa = null;
	
	public MasterEconsSimArgs(SlaveEconsSimArgs sra){
		_ssa = sra;
	}
	
	public boolean getSkipToAgg(){
		return _aggOnly;
	}
	
	public void setSkipToAgg(boolean aggOnly){
		_aggOnly = aggOnly;
	}
	
	public void setPredicateStatsFilename(String psFn){
		_psFn = psFn;
	}
	
	public String getPredicateStatsFilename(){
		return _psFn;
	}
	
	public SlaveEconsSimArgs getSlaveArgs(int server){
		return _ssa.instantiate(server);
	}
	
	public SlaveEconsSimArgs getSlaveArgs(){
		return _ssa;
	}
	
	public String toString(){
		return "Master:: ps:" + _psFn + " aggOnly"+_aggOnly+" \n"
			+"Slave:: "+_ssa;
	}
	
	public RMIEconsSimServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterEconsSim getTaskMaster() {
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