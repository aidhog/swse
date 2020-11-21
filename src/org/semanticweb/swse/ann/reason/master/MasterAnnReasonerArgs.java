package org.semanticweb.swse.ann.reason.master;

import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.reason.RMIAnnReasonerServer;
import org.semanticweb.swse.ann.reason.SlaveAnnReasonerArgs;

public class MasterAnnReasonerArgs extends MasterArgs<MasterAnnReasonerArgs>{
	public static final RMIAnnReasonerServer DEFAULT_RI = new RMIAnnReasonerServer();
	public static final MasterAnnReasoner DEFAULT_MASTER = new MasterAnnReasoner();
	
	public static final String DEFAULT_TBOX_FILENAME_NGZ = "tbox.nq";
	public static final String DEFAULT_TBOX_FILENAME_GZ = DEFAULT_TBOX_FILENAME_NGZ+GZ;
	
	public static final String DEFAULT_REASONED_TBOX_FILENAME_NGZ = "tbox.reason.nq";
	public static final String DEFAULT_REASONED_TBOX_FILENAME_GZ = DEFAULT_REASONED_TBOX_FILENAME_NGZ+GZ;
	
	public static final String DEFAULT_RANKED_TBOX_FILENAME_NGZ = "tbox.rank.nq";
	public static final String DEFAULT_RANKED_TBOX_FILENAME_GZ = DEFAULT_RANKED_TBOX_FILENAME_NGZ+GZ;
	
	public static final boolean DEFAULT_AUTH = true;
	
	private boolean _auth = DEFAULT_AUTH;
	
	private boolean _gzouttbox = DEFAULT_GZ_OUT;
	private String _outtbox = null;
	
	private boolean _gzoutreasontbox = DEFAULT_GZ_OUT;
	private String _outreasontbox = null;
	
	private boolean _gzoutranktbox = DEFAULT_GZ_OUT;
	private String _outranktbox = null;
	
	private String _outdir = null;
	
	private String _inranks = null;
	private boolean _gzinranks = DEFAULT_GZ_IN;
	
	private SlaveAnnReasonerArgs _sra = null;
	
	public MasterAnnReasonerArgs(String inranks, String outdir, SlaveAnnReasonerArgs sra){
		_outdir = outdir;
		_sra = sra;
		_inranks = inranks;
		initDefaults(outdir);
	}
	
	private void initDefaults(String outdir){
		_outtbox = getDefaultTboxOut(outdir);
		_outreasontbox = getDefaultReasonedTboxOut(outdir);
		_outranktbox = getDefaultRankedTboxOut(outdir);
	}
	
	public void setGzTboxOut(boolean gzouttbox){
		_gzouttbox = gzouttbox;
	}
	
	public boolean getGzTboxOut(){
		return _gzouttbox;
	}
	
	public void setGzRanksIn(boolean gzinranks){
		_gzinranks = gzinranks;
	}
	
	public boolean getAuth(){
		return _auth;
	}
	
	public void setAuth(boolean auth){
		_auth = auth;
	}
	
	public boolean getGzRanksIn(){
		return _gzinranks;
	}
	
	public String getRanksIn(){
		return _inranks;
	}
	
	public void setGzReasonedTboxOut(boolean gzoutrtbox){
		_gzoutreasontbox = gzoutrtbox;
	}
	
	public boolean getGzReasonedTboxOut(){
		return _gzoutreasontbox;
	}
	
	public void setGzRankedTboxOut(boolean gzoutranktbox){
		_gzoutranktbox = gzoutranktbox;
	}
	
	public boolean getGzRankedTboxOut(){
		return _gzoutranktbox;
	}
	
	public void setTboxOut(String outtbox){
		_outtbox = outtbox;
	}
	
	public String getTboxOut(){
		return _outtbox;
	}
	
	public void setReasonedTboxOut(String outreasontbox){
		_outreasontbox = outreasontbox;
	}
	
	public String getReasonedTboxOut(){
		return _outreasontbox;
	}
	
	public void setRankedTboxOut(String outranktbox){
		_outranktbox = outranktbox;
	}
	
	public String getRankedTboxOut(){
		return _outranktbox;
	}
	
	public Rule[] getRules(){
		return _sra.getRules();
	}
	
	public SlaveAnnReasonerArgs getSlaveArgs(int server){
		return _sra.instantiate(server);
	}
	
	public String toString(){
		return "Master:: tboxout:"+_outtbox+" gztboxout:"+_gzouttbox+" outranktbox"+_outranktbox+" gzranktboxout:"+_gzoutranktbox+" tboxreasonout:"+_outreasontbox+" gzreasontboxout:"+_gzoutreasontbox+"\n"
			+"Slave:: "+_sra;
	}
	
	public static final String getDefaultTboxOut(String outdir){
		return getDefaultTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultTboxOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_TBOX_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_TBOX_FILENAME_NGZ;
	}
	
	public static final String getDefaultReasonedTboxOut(String outdir){
		return getDefaultReasonedTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultReasonedTboxOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_REASONED_TBOX_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_REASONED_TBOX_FILENAME_NGZ;
	}
	
	public static final String getDefaultRankedTboxOut(String outdir){
		return getDefaultRankedTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultRankedTboxOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_RANKED_TBOX_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_RANKED_TBOX_FILENAME_NGZ;
	}
	
	public RMIAnnReasonerServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterAnnReasoner getTaskMaster() {
		return DEFAULT_MASTER;
	}
}