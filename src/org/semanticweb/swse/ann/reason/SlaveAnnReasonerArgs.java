package org.semanticweb.swse.ann.reason;

import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.owlhogan.WOLC_T_SPLIT;
import org.semanticweb.saorr.fragments.owlhogan.WOL_T_SPLIT;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveAnnReasonerArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8759673213202538048L;

	public static final boolean DEFAULT_HANDLE_COLLECTIONS = true;
	
	public static final Rule[] DEFAULT_RULES = Fragment.getRules(WOL_T_SPLIT.class);
	public static final Rule[] DEFAULT_TBOX_EXTRACT_RULES = Fragment.getRules(WOLC_T_SPLIT.class);
	
	private String _inT, _inA;
	
	private String _outTbox;
	private boolean _gzOutT = MasterArgs.DEFAULT_GZ_OUT;
	
	private String _outReasoned;
	private boolean _gzOutR = MasterArgs.DEFAULT_GZ_OUT;
	
	private boolean _gzInT = MasterArgs.DEFAULT_GZ_IN;
	private boolean _gzInA = MasterArgs.DEFAULT_GZ_IN;
	
	private boolean _handleCollections = DEFAULT_HANDLE_COLLECTIONS;
	
	private Rule[] _rules = DEFAULT_RULES;
	private Rule[] _terules = DEFAULT_TBOX_EXTRACT_RULES;
	
	private String _redirs;
	private boolean _gzRedirs = MasterArgs.DEFAULT_GZ_IN;
	
	public static final String DEFAULT_TBOX_FILENAME_GZ = "tbox.nq.gz";
	public static final String DEFAULT_TBOX_FILENAME_NGZ = "tbox.nq";
	
	public static final String DEFAULT_REASONED_FILENAME_GZ = "data.r.nq.gz";
	public static final String DEFAULT_REASONED_FILENAME_NGZ = "data.r.nq";
	
	public SlaveAnnReasonerArgs(String inT, String inA, String redirs, String outTbox, String outReasoned){
		_outTbox = outTbox;
		_outReasoned = outReasoned;
		_redirs = redirs;
		_inT = inT;
		_inA = inA;
	}
	
	public String getOutTbox(){
		return _outTbox;
	}
	
	public String getOutReasoned(){
		return _outReasoned;
	}
	
	public String getTboxInput(){
		return _inT;
	}
	
	public void setGzInTbox(boolean gzinT){
		_gzInT = gzinT;
	}
	
	public boolean getGzInTbox(){
		return _gzInT;
	}
	
	public String getAboxInput(){
		return _inA;
	}
	
	public void setGzInAbox(boolean gzinA){
		_gzInA = gzinA;
	}
	
	public boolean getGzInAbox(){
		return _gzInA;
	}
	
	public void setGzRedirects(boolean gzredirects){
		_gzRedirs = gzredirects;
	}
	
	public boolean getGzRedirects(){
		return _gzRedirs;
	}
	
	public void setGzOutTbox(boolean gzoutt){
		_gzOutT = gzoutt;
	}
	
	public boolean getGzOutTbox(){
		return _gzOutT;
	}
	
	public void setGzOutReasoned(boolean gzoutr){
		_gzOutR = gzoutr;
	}
	
	public boolean getGzOutReasoned(){
		return _gzOutR;
	}
	
	public void setHandleCollecitons(boolean hc){
		_handleCollections = hc;
	}
	
	public boolean getHandleCollections(){
		return _handleCollections;
	}
	
	public void setTboxExtractRules(Rule[] rules){
		_terules = rules;
	}
	
	public Rule[] getTboxExtractRules(){
		return _terules;
	}
	
	public void setRules(Rule[] rules){
		_rules = rules;
	}
	
	public Rule[] getRules(){
		return _rules;
	}
	
	public String getRedirects(){
		return _redirs;
	}
	
	public SlaveAnnReasonerArgs instantiate(int server) {
		String inT = RMIUtils.getLocalName(_inT, server);
		String inA = RMIUtils.getLocalName(_inA, server);
		String outr = RMIUtils.getLocalName(_outReasoned, server);
		String outt = RMIUtils.getLocalName(_outTbox, server);
		
		String log = RMIUtils.getLocalName(_logFile, server);
		
		SlaveAnnReasonerArgs sca = new SlaveAnnReasonerArgs(inT, inA, _redirs, outt, outr);
		
		sca.setSlaveLog(log);
		sca.setGzInTbox(_gzInT);
		sca.setGzInAbox(_gzInA);
		sca.setGzOutReasoned(_gzOutR);
		sca.setGzOutTbox(_gzOutT);
		sca.setRules(_rules);
		sca.setTboxExtractRules(_terules);
		sca.setHandleCollecitons(_handleCollections);
		sca.setGzRedirects(_gzRedirs);

		return sca;
	}
	
	public String toString(){
		return "inTbox:"+_inT+"inAbox:"+_inA+" gzinTbox:"+_gzInT+" gzinAbox:"+_gzInA+" redirects:"+_redirs+" gzred:"+_gzRedirs+" rules(size):"+_rules.length+" handlecol:"+_handleCollections+" outtbox:"+_outTbox+" gzoutTbox:"+_gzOutT+" outreas:"+_outReasoned+" gzreas:"+_gzOutR;
	}
	
	public static final String getDefaultTboxOut(String outdir){
		return getDefaultTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultTboxOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_TBOX_FILENAME_GZ;
		return outdir+"/"+DEFAULT_TBOX_FILENAME_NGZ;
	}
	
	public static final String getDefaultReasonedOut(String outdir){
		return getDefaultReasonedOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultReasonedOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_REASONED_FILENAME_GZ;
		return outdir+"/"+DEFAULT_REASONED_FILENAME_NGZ;
	}
}
