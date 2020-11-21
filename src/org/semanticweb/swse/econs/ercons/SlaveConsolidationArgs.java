package org.semanticweb.swse.econs.ercons;

import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.fragments.owlhogan.WOLC_T_SPLIT;
import org.semanticweb.saorr.fragments.owlhogan.WOL_T_SPLIT;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.saorr.rules.SortedRuleSet;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;
import org.semanticweb.swse.econs.ercons.master.MasterConsolidationArgs;

public class SlaveConsolidationArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2077478594392035258L;

	public static final Rule[] DEFAULT_RULES = Fragment.getRules(WOL_T_SPLIT.class);
	
	private static final SortedRuleSet<Rule> ALL_RULES = new SortedRuleSet<Rule>();
	static{
		for(Rule r:Fragment.getRules(WOLC_T_SPLIT.class))
			ALL_RULES.add(r);
		for(Rule r: MasterConsolidationArgs.CONSOLIDATION_ONLY_RULES.getRulesArray()){
			ALL_RULES.add(r);
		}
	}
	
	public static final Rule[] DEFAULT_TBOX_EXTRACT_RULES = new Rule[ALL_RULES.size()];
	static{
		ALL_RULES.toArray(DEFAULT_TBOX_EXTRACT_RULES);
	}
	
	public static final boolean DEFAULT_HANDLE_COLLECTIONS = true;
	
	public static final String DEFAULT_TMP_DIR = "tmp/";
	public static final String DEFAULT_SCATTER_DIR = DEFAULT_TMP_DIR+"scatter/";
	
	public static final String DEFAULT_OUT_FINAL_FILENAME_NGZ = "data.final.nx";
	public static final String DEFAULT_OUT_FINAL_FILENAME_GZ = DEFAULT_OUT_FINAL_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_REMOTE_GATHER_DIR = "gather/";
	
	public static final String DEFAULT_LOCAL_GATHER_DIR = "gall/";
	
	public static final String DEFAULT_TBOX_FILENAME_GZ = "tbox.nq.gz";
	public static final String DEFAULT_TBOX_FILENAME_NGZ = "tbox.nq";
	
	public static final String DEFAULT_CONSOLIDATED_FILENAME_GZ = "data.ercons.nq.gz";
	public static final String DEFAULT_CONSOLIDATED_FILENAME_NGZ = "data.ercons.nq";
	
	public static final String DEFAULT_ABOX_FILENAME_GZ = "consabox.nq.gz";
	public static final String DEFAULT_ABOX_FILENAME_NGZ = "consabox.nq";
	
	private String _in;
	private boolean _gzIn = MasterArgs.DEFAULT_GZ_IN;
	
	private String _outTbox;
	private boolean _gzOutT = MasterArgs.DEFAULT_GZ_OUT;
	
	private String _outAbox;
	private boolean _gzOutA = MasterArgs.DEFAULT_GZ_OUT;
	
	private String _outC;
	private boolean _gzOutC = MasterArgs.DEFAULT_GZ_OUT;
	
	private boolean _handleCollections = DEFAULT_HANDLE_COLLECTIONS;
	
	private Rule[] _rules = DEFAULT_RULES;
	private Rule[] _terules = DEFAULT_TBOX_EXTRACT_RULES;
	
	private String _redirs;
	private boolean _gzRed = MasterArgs.DEFAULT_GZ_IN;
	
	private String _tmpdir;
	
	private String _outdir = null;
	
	private String _scatterDir;
	
	private String _remoteGatherDir;
	
	private String _localGatherDir;
	
	public SlaveConsolidationArgs(String in, String redirs, String outdir){
		_in = in;
		_outdir = outdir;
		_redirs = redirs;
		initDefaults(outdir);
	}
	
	public void setOutTbox(String outTbox){
		_outTbox = outTbox;
	}
	
	public String getOutTbox(){
		return _outTbox;
	}
	
	public void setOutAbox(String outAbox){
		_outAbox = outAbox;
	}
	
	public String getOutAbox(){
		return _outAbox;
	}
	
	public void setOutConsolidated(String outConsolidated){
		_outC = outConsolidated;
	}
	
	public String getOutConsolidated(){
		return _outC;
	}
	
	public String getOutDir(){
		return _outdir;
	}
	
	public String getIn(){
		return _in;
	}
	
	public void setGzIn(boolean gzin){
		_gzIn = gzin;
	}
	
	public boolean getGzIn(){
		return _gzIn;
	}
	
	public String getRedirects(){
		return _redirs;
	}
	
	public void setGzRedirects(boolean gzredirects){
		_gzRed = gzredirects;
	}
	
	public boolean getGzRedirects(){
		return _gzRed;
	}
	
	public void setGzOutTbox(boolean gzoutt){
		_gzOutT = gzoutt;
	}
	
	public boolean getGzOutTbox(){
		return _gzOutT;
	}
	
	public void setGzOutAbox(boolean gzouta){
		_gzOutA = gzouta;
	}
	
	public boolean getGzOutAbox(){
		return _gzOutA;
	}
	
	public void setGzOutConsolidated(boolean gzoutc){
		_gzOutC = gzoutc;
	}
	
	public boolean getGzOutConsolidated(){
		return _gzOutC;
	}
	
	public void setHandleCollections(boolean hc){
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
	
	public void setTmpDir(String tmpDir){
		_tmpdir = tmpDir;
	}
	
	public String getTmpDir(){
		return _tmpdir;
	}
	
	public void setScatterDir(String sDir){
		_scatterDir = sDir;
	}
	
	public String getOutScatterDir(){
		return _scatterDir;
	}

	public void setRemoteGatherDir(String gDir){
		_remoteGatherDir = gDir;
	}
	
	public String getRemoteGatherDir(){
		return _remoteGatherDir;
	}
	
	public void setLocalGatherDir(String gDir){
		_localGatherDir = gDir;
	}
	
	public String getLocalGatherDir(){
		return _localGatherDir;
	}
	
	private void initDefaults(String outdir){
		_tmpdir = getDefaultTmpDir(outdir);
		_scatterDir = getDefaultScatterDir(outdir);
		_remoteGatherDir = getDefaultRemoteGatherDir(outdir);
		_localGatherDir = getDefaultLocalGatherDir(outdir);
		_outTbox = getDefaultTboxOut(outdir);
		_outAbox = getDefaultAboxOut(outdir);
		_outC = getDefaultConsolidatedOut(outdir);
	}
	
	public SlaveConsolidationArgs instantiate(int server) {
		String in = RMIUtils.getLocalName(_in, server);
		String tmpdir = RMIUtils.getLocalName(_tmpdir, server);
		String out = RMIUtils.getLocalName(_outdir, server);
		String scatterDir = RMIUtils.getLocalName(_scatterDir, server);
		String localGatherDir = RMIUtils.getLocalName(_localGatherDir, server);
		String log = RMIUtils.getLocalName(_logFile, server);
		String redirs = RMIUtils.getLocalName(_redirs, server);
		String tboxOut = RMIUtils.getLocalName(_outTbox, server);
		String aboxOut = RMIUtils.getLocalName(_outAbox, server);
		String consOut = RMIUtils.getLocalName(_outC, server);
		
		SlaveConsolidationArgs sca = new SlaveConsolidationArgs(in, redirs, out);
		
		sca.setScatterDir(scatterDir);
		sca.setSlaveLog(log);
		sca.setTmpDir(tmpdir);
		sca.setLocalGatherDir(localGatherDir);
		sca.setOutTbox(tboxOut);
		sca.setOutConsolidated(consOut);
		sca.setOutAbox(aboxOut);
		
		sca.setGzIn(_gzIn);
		sca.setGzOutTbox(_gzOutT);
		sca.setGzOutAbox(_gzOutA);
		sca.setGzOutConsolidated(_gzOutC);		
		sca.setGzRedirects(_gzRed);
		sca.setHandleCollections(_handleCollections);
		sca.setRules(_rules);
		sca.setTboxExtractRules(_terules);
		
		//don't localise
		sca.setRemoteGatherDir(_remoteGatherDir);
		return sca;
	}
	
	public String toString(){
		return "in:"+_in+" gzIn:"+_gzIn+" tmpdir:"+_tmpdir+" outdir:"+_outdir+" scatterDir:"+_scatterDir+" remoteGatherDir:"+_remoteGatherDir+
		" localGatherDir:"+_localGatherDir+" outTbox:"+_outTbox+" gzOutT"+_gzOutT+" outAbox:"+_outAbox+" gzOutA"+_gzOutA +
		" outCons:"+_outC+" gzOutC:"+_gzOutC+" handleColls:"+_handleCollections+" rules (size):"+_rules.length+" terules (size):"+_terules.length +
		" redirs:"+_redirs+" gzRed:"+_gzRed;
	}
	
	public static final String getDefaultOutFinalFilename(String outdir){
		return getDefaultOutFinalFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultOutFinalFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_GZ;
		return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_NGZ;
	}
	
	public static final String getDefaultTmpDir(String outdir){
		return outdir+"/"+DEFAULT_TMP_DIR;
	}
	
	public static final String getDefaultScatterDir(String outdir){
		return outdir+"/"+DEFAULT_SCATTER_DIR;
	}
	
	private static String getDefaultRemoteGatherDir(String outdir) {
		return outdir+"/"+DEFAULT_REMOTE_GATHER_DIR;
	}
	
	private static String getDefaultLocalGatherDir(String outdir) {
		return outdir+"/"+DEFAULT_LOCAL_GATHER_DIR;
	}
	
	public static final String getDefaultTboxOut(String outdir){
		return getDefaultTboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultTboxOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_TBOX_FILENAME_GZ;
		return outdir+"/"+DEFAULT_TBOX_FILENAME_NGZ;
	}
	
	public static final String getDefaultAboxOut(String outdir){
		return getDefaultAboxOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultAboxOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_ABOX_FILENAME_GZ;
		return outdir+"/"+DEFAULT_ABOX_FILENAME_NGZ;
	}
	
	public static final String getDefaultConsolidatedOut(String outdir){
		return getDefaultConsolidatedOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultConsolidatedOut(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_CONSOLIDATED_FILENAME_GZ;
		return outdir+"/"+DEFAULT_CONSOLIDATED_FILENAME_NGZ;
	}
}
