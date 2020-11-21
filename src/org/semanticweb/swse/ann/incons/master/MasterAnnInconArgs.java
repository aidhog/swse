package org.semanticweb.swse.ann.incons.master;

import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.constraints.OWL2RL_C_T_SPLIT;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.incon.SlaveAnnInconArgs;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerServer;
import org.semanticweb.yars.nx.mem.MemoryManager;

public class MasterAnnInconArgs extends MasterArgs<MasterAnnInconArgs>{
	public static final RMIAnnReasonerServer DEFAULT_RI = new RMIAnnReasonerServer();
	public static final MasterAnnIncon DEFAULT_MASTER = new MasterAnnIncon();
	
	public static final String DEFAULT_INCONSISTENCY_FILENAME_NGZ = "inconsist.dat";
	public static final String DEFAULT_INCONSISTENCY_FILENAME_GZ = DEFAULT_INCONSISTENCY_FILENAME_NGZ+GZ;
	
	public static final String DEFAULT_NEW_TBOX_FILENAME_NGZ = "newtbox.nx";
	public static final String DEFAULT_NEW_TBOX_FILENAME_GZ = DEFAULT_NEW_TBOX_FILENAME_NGZ+GZ;

	public static final Rule[] DEFAULT_RULES = Fragment.getRules(OWL2RL_C_T_SPLIT.class);
	
	public static final int STMT_BUDGET = MemoryManager.estimateMaxStatements(3);
	
	private String _outdir = null;
	
	private String _intbox = null;
	private boolean _gzintbox = DEFAULT_GZ_IN;
	
	private String _outic = null;
	private boolean _gzoutic = DEFAULT_GZ_OUT;
	
	private String _outnt = null;
	private boolean _gzoutnt = DEFAULT_GZ_OUT;
	
	private Rule[] _rules = DEFAULT_RULES;
	
	private SlaveAnnInconArgs _sia = null;
	
	private int _budget = STMT_BUDGET;
	
	public MasterAnnInconArgs(String intbox, String outdir, SlaveAnnInconArgs sia){
		_outdir = outdir;
		_sia = sia;
		_intbox = intbox;
		initDefaults(outdir);
	}
	
	private void initDefaults(String outdir){
		_outic = getDefaultInconsistencyOut(outdir);
		_outnt = getDefaultNewTboxOut(outdir);
	}
	
	public void setRules(Rule[] rules){
		_rules = rules;
	}
	
	public Rule[] getRules(){
		return _rules;
	}
	
	public int getBudget(){
		return _budget;
	}
	
	public void setBudget(int budget){
		_budget = budget;
	}
	
	public void setNewTboxOut(String outnt){
		_outnt = outnt;
	}
	
	public String getNewTboxOut(){
		return _outnt;
	}
	
	public void setGzNewTboxOut(boolean gzoutnt){
		_gzoutnt = gzoutnt;
	}
	
	public boolean getGzNewTboxOut(){
		return _gzoutnt;
	}
	
	public void setInconsistencyOut(String outic){
		_outic = outic;
	}
	
	public String getInconsistencyOut(){
		return _outic;
	}
	
	public void setGzInconsistencyOut(boolean gzoutic){
		_gzoutic = gzoutic;
	}
	
	public boolean getGzInconsistencyOut(){
		return _gzoutic;
	}
	
	public void setGzTboxIn(boolean gzintbox){
		_gzintbox = gzintbox;
	}
	
	public boolean getGzTboxIn(){
		return _gzintbox;
	}
	
	public String getTboxIn(){
		return _intbox;
	}
	
	public SlaveAnnInconArgs getSlaveArgs(int server){
		return _sia.instantiate(server);
	}
	
	public String toString(){
		return "Master:: tboxin:"+_intbox+" gztboxin:"+_gzintbox+" outinconsist"+_outic+" gzinconsistout:"+_gzoutic+" outnewtbox"+_outnt+" gznewtbox:"+_gzoutnt+" rules (size):"+_rules.length+"\n"
			+"Slave:: "+_sia;
	}
	
	public static final String getDefaultInconsistencyOut(String outdir){
		return getDefaultInconsistencyOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultInconsistencyOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_INCONSISTENCY_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_INCONSISTENCY_FILENAME_NGZ;
	}
	
	public static final String getDefaultNewTboxOut(String outdir){
		return getDefaultNewTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultNewTboxOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_NEW_TBOX_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_NEW_TBOX_FILENAME_NGZ;
	}

	public RMIAnnReasonerServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterAnnIncon getTaskMaster() {
		return DEFAULT_MASTER;
	}
}