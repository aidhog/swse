package org.semanticweb.swse.econs.ercons.master;

import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.owl2rl.OWL2RL_T_SPLIT;
import org.semanticweb.saorr.fragments.owlhogan.ADHOC_T_SPLIT;
import org.semanticweb.saorr.fragments.owlhogan.WOL_T_SPLIT;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.Rules;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.econs.ercons.RMIConsolidationServer;
import org.semanticweb.swse.econs.ercons.SlaveConsolidationArgs;

public class MasterConsolidationArgs extends MasterArgs<MasterConsolidationArgs>{
	public static final RMIConsolidationServer DEFAULT_RI = new RMIConsolidationServer();
	public static final MasterConsolidation DEFAULT_MASTER = new MasterConsolidation();
	
	public static final String DEFAULT_TBOX_FILENAME_NGZ = "tbox.all.nq";
	public static final String DEFAULT_TBOX_FILENAME_GZ = DEFAULT_TBOX_FILENAME_NGZ + GZ;
	
	public static final String DEFAULT_SAMEAS_FILENAME = "sameas.final.nx.gz";
	
	
	public static final int[] SPOC_ORDER = new int[]{0,1,2,3};
	public static final int[] OPSC_ORDER = new int[]{2,1,0,3};
	
	public static final String DEFAULT_TEMP_DIR = "tmp/";
	
	public static final boolean DEFAULT_REASON_EXTRACT = true;
	public static final boolean DEFAULT_SAMEAS_ONLY = false;
	
	public static final Rules CONSOLIDATION_ONLY_RULES = new Rules(
		new Rule[]{
				OWL2RL_T_SPLIT.PRP_FP,
				OWL2RL_T_SPLIT.PRP_IFP,
				OWL2RL_T_SPLIT.CLS_MAXC1,
				ADHOC_T_SPLIT.CLS_C2
		}
	);
	
	public static final Rules GENERAL_REASONING_RULES = new Rules(
		 new Rule[]{
				WOL_T_SPLIT.CAX_EQC1,
				WOL_T_SPLIT.CAX_EQC2,
				WOL_T_SPLIT.CAX_SCO,
				WOL_T_SPLIT.CLS_HV1,
				WOL_T_SPLIT.CLS_HV2,
				WOL_T_SPLIT.CLS_INT2,
				WOL_T_SPLIT.CLS_SVF2,
				WOL_T_SPLIT.CLS_UNI,
				WOL_T_SPLIT.PRP_DOM,
				WOL_T_SPLIT.PRP_EQP1,
				WOL_T_SPLIT.PRP_EQP2,
				WOL_T_SPLIT.PRP_INV1,
				WOL_T_SPLIT.PRP_INV2,
				WOL_T_SPLIT.PRP_RNG,
				WOL_T_SPLIT.PRP_SPO1,
				WOL_T_SPLIT.PRP_SYMP,
		 }
	);
	
	private boolean _gzouttbox = DEFAULT_GZ_OUT;
	private String _outtbox = null;
	
	private String _outsa = null;
	
	private boolean _reasonExtract = DEFAULT_REASON_EXTRACT;
	private boolean _sameasOnly = DEFAULT_SAMEAS_ONLY;
	
	private String _tmpdir;
	
	private SlaveConsolidationArgs _sca = null;
	
	public MasterConsolidationArgs(String outdir, SlaveConsolidationArgs sra){
		_sca = sra;
		initDefault(outdir);
	}
	
	private void initDefault(String outdir){
		_tmpdir = getDefaultTmpDir(outdir);
		_outtbox = getDefaultTboxOut(outdir);
		_outsa = getDefaultSameAsOut(outdir);
	}
	
	public void setGzTboxOut(boolean gzouttbox){
		_gzouttbox = gzouttbox;
	}
	
	public boolean getGzTboxOut(){
		return _gzouttbox;
	}
	
	public void setReasonExtract(boolean reasonextract){
		_reasonExtract = reasonextract;
	}
	
	public boolean getReasonExtract(){
		return _reasonExtract;
	}
	
	public void setSameasOnly(boolean sameasOnly){
		_sameasOnly = sameasOnly;
	}
	
	public boolean getSameasOnly(){
		return _sameasOnly;
	}
	
	public String getTboxOut(){
		return _outtbox;
	}
	
	public void setTboxOut(String outtbox){
		_outtbox = outtbox;
	}
	
	public String getSameAsOut(){
		return _outsa;
	}
	
	public void setSameAsOut(String outsa){
		_outsa = outsa;
	}
	
	public SlaveConsolidationArgs getSlaveArgs(int server){
		return _sca.instantiate(server);
	}
	
	public SlaveConsolidationArgs getSlaveArgs(){
		return _sca;
	}
	
	public String getTmpDir(){
		return _tmpdir;
	}
	
	public String toString(){
		return "Master:: tboxout:"+_outtbox+" gztboxout:"+_gzouttbox+" tmpdir:"+_tmpdir+" reasonExtract:"+_reasonExtract+" sameasOnly:"+_sameasOnly
			+"\nSlave:: "+_sca;
	}
	
	public RMIConsolidationServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterConsolidation getTaskMaster() {
		return DEFAULT_MASTER;
	}
	
	public static final String getDefaultTboxOut(String outdir){
		return getDefaultTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultSameAsOut(String outdir){
		String outdirl = RMIUtils.getLocalName(outdir);
		return outdirl+"/"+DEFAULT_SAMEAS_FILENAME;
	}
	
	public static final String getDefaultTboxOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_TBOX_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_TBOX_FILENAME_NGZ;
	}
	
	public static final String getDefaultTmpDir(String outdir){
		return RMIUtils.getLocalName(outdir)+"/"+DEFAULT_TEMP_DIR;
	}
}