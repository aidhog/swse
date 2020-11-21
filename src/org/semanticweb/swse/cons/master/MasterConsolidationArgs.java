package org.semanticweb.swse.cons.master;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.cons.RMIConsolidationServer;
import org.semanticweb.swse.cons.SlaveConsolidationArgs;

public class MasterConsolidationArgs extends MasterArgs<MasterConsolidationArgs>{
	
	public static final String DEFAULT_SAMEAS_FILENAME_GZ = "sameas.nx.gz";
	public static final String DEFAULT_SAMEAS_FILENAME_NGZ = "sameas.nx";
	
	public static final RMIConsolidationServer DEFAULT_RI = new RMIConsolidationServer();
	public static final MasterConsolidation DEFAULT_MASTER = new MasterConsolidation();
	
	private boolean _gzoutsa = DEFAULT_GZ_OUT;
	private String _outsa = null;
	private SlaveConsolidationArgs _sca = null;
	
	private String _ranks = null;
	private boolean _gzranks; 
	
	public MasterConsolidationArgs(String outsameas, SlaveConsolidationArgs sca){
		_outsa = outsameas;
		_sca = sca;
	}
	
	public void setGzSameAsOut(boolean gzoutsa){
		_gzoutsa = gzoutsa;
	}
	
	public boolean getGzSameAsOut(){
		return _gzoutsa;
	}
	
	public String getSameAsOut(){
		return _outsa;
	}
	
	public void setRanks(String ranks, boolean gz){
		_ranks = ranks;
		_gzranks = gz;
	}
	
	public boolean getGzRanks(){
		return _gzranks;
	}
	
	public String getRanks(){
		return _ranks;
	}
	
	public SlaveConsolidationArgs getSlaveArgs(int server){
		return _sca.instantiate(server);
	}
	
	public String toString(){
		return "Master:: outsa:"+_outsa+" gzoutsa:"+_gzoutsa+" ranks:"+_ranks+" gzranks:"+_gzranks+"\n"
			+"Slave:: "+_sca;
	}

	public RMIConsolidationServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterConsolidation getTaskMaster() {
		return DEFAULT_MASTER;
	}
	
	public static final String getDefaultSameasOut(String outdir){
		return getDefaultSameasOut(outdir, DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultSameasOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_SAMEAS_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_SAMEAS_FILENAME_NGZ;
	}
}