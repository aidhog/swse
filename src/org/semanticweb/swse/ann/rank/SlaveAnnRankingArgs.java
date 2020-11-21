package org.semanticweb.swse.ann.rank;

import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveAnnRankingArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2077478594392035258L;

	public static final boolean DEFAULT_PLD = false;
	public static final boolean DEFAULT_TBOX = false;
	
	public static final String DEFAULT_TMP_DIR = "tmp/";
	public static final String DEFAULT_SCATTER_DIR = DEFAULT_TMP_DIR+"scatter/";
	
	public static final String DEFAULT_SORTED_BY_CONTEXT_NGZ = "data.c.s.nq";
	public static final String DEFAULT_SORTED_BY_CONTEXT_GZ = DEFAULT_SORTED_BY_CONTEXT_NGZ+".gz";
	
	public static final String DEFAULT_CONTEXTS_FILENAME_NGZ = "contexts.s.nx";
	public static final String DEFAULT_CONTEXTS_FILENAME_GZ = DEFAULT_CONTEXTS_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_GRAPH_FILENAME_NGZ = "graph.f.nx";
	public static final String DEFAULT_GRAPH_FILENAME_GZ = DEFAULT_GRAPH_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_INV_GRAPH_FILENAME_NGZ = "invgraph.f.nx";
	public static final String DEFAULT_INV_GRAPH_FILENAME_GZ = DEFAULT_INV_GRAPH_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_RAW_INV_GRAPH_FILENAME_NGZ = "invgraph.r.nx";
	public static final String DEFAULT_RAW_INV_GRAPH_FILENAME_GZ = DEFAULT_RAW_INV_GRAPH_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_OUT_FINAL_FILENAME_NGZ = "data.nq.s.r.nx";
	public static final String DEFAULT_OUT_FINAL_FILENAME_GZ = DEFAULT_OUT_FINAL_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_REMOTE_GATHER_DIR = "gather/";
	
	private String _r;
	private boolean _gzR = MasterArgs.DEFAULT_GZ_IN;
	
	private String _in;
	private boolean _gzIn = MasterArgs.DEFAULT_GZ_IN;
	
	private boolean _tbox = DEFAULT_TBOX;
	
	private String _tmpdir;
	
	private String _sortedCon = null;
	
	private String _cons = null;
	
	private String _graph = null;
	
	private String _igraph = null;
	
	private String _irgraph = null;
	
	private String _out = null;
	
	private String _scatterDir;
	
	private String _remoteGatherDir;
	
	private String _outFinal = null;
	
	public SlaveAnnRankingArgs(String in, String redirects, String outdir){
		_in = in;
		_r = redirects;
		_out = outdir;
		initDefaults(outdir);
	}
	
	public String getRedirects(){
		return _r;
	}
	
	public String getOutDir(){
		return _out;
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
	
	public void setTbox(boolean tbox){
		_tbox = tbox;
	}
	
	public boolean getTbox(){
		return _tbox;
	}
	
	public void setGzRedirects(boolean gzr){
		_gzR = gzr;
	}
	
	public boolean getGzRedirects(){
		return _gzR;
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
	
	public void setSortedByContext(String sc){
		_sortedCon = sc;
	}
	
	public String getSortedByContext(){
		return _sortedCon;
	}
	
	public void setContexts(String c){
		_cons = c;
	}
	
	public String getContexts(){
		return _cons;
	}
	
	public void setOutFinal(String of){
		_outFinal = of;
	}
	
	public String getOutFinal(){
		return _outFinal;
	}
	
	public void setRemoteGatherDir(String gDir){
		_remoteGatherDir = gDir;
	}
	
	public String getRemoteGatherDir(){
		return _remoteGatherDir;
	}
	
	public void setRawInvGraphFragment(String rig){
		_irgraph = rig;
	}
	
	public String getRawInvGraphFragment(){
		return _irgraph;
	}
	
	public void setInvGraphFragment(String ig){
		_igraph = ig;
	}
	
	public String getInvGraphFragment(){
		return _igraph;
	}
	
	public void setGraphFragment(String g){
		_graph = g;
	}
	
	public String getGraphFragment(){
		return _graph;
	}
	
	private void initDefaults(String outdir){
		_tmpdir = getDefaultTmpDir(outdir);
		_graph = getDefaultGraphFilename(outdir);
		_igraph = getDefaultInvGraphFilename(outdir);
		_irgraph = getDefaultRawInvGraphFilename(outdir);
		_cons = getDefaultContextsFilename(outdir);
		_sortedCon = getDefaultSortedByContextFilename(outdir);
		_scatterDir = getDefaultScatterDir(outdir);
		_outFinal = getDefaultOutFinalFilename(outdir);
		_remoteGatherDir = getDefaultRemoteGatherDir(outdir);
	}
	
	public SlaveAnnRankingArgs instantiate(int server) {
		String in = RMIUtils.getLocalName(_in, server);
		String r = RMIUtils.getLocalName(_r, server);
		String tmpdir = RMIUtils.getLocalName(_tmpdir, server);
		String sortedCon = RMIUtils.getLocalName(_sortedCon, server);
		String cons = RMIUtils.getLocalName(_cons, server);
		String graph = RMIUtils.getLocalName(_graph, server);
		String igraph = RMIUtils.getLocalName(_igraph, server);
		String rgraph = RMIUtils.getLocalName(_irgraph, server);
		String out = RMIUtils.getLocalName(_out, server);
		String scatterDir = RMIUtils.getLocalName(_scatterDir, server);
		String outfinal = RMIUtils.getLocalName(_outFinal, server);
		
		String log = RMIUtils.getLocalName(_logFile, server);
		
		SlaveAnnRankingArgs sca = new SlaveAnnRankingArgs(in, r, out);
		sca.setContexts(cons);
		sca.setGraphFragment(graph);
		sca.setInvGraphFragment(igraph);
		sca.setRawInvGraphFragment(rgraph);
		sca.setGzIn(_gzIn);
		sca.setGzRedirects(_gzR);
		sca.setSlaveLog(log);
		sca.setSortedByContext(sortedCon);
		sca.setTbox(_tbox);
		sca.setTmpDir(tmpdir);
		sca.setScatterDir(scatterDir);
		sca.setOutFinal(outfinal);
		//don't localise
		sca.setRemoteGatherDir(_remoteGatherDir);
		return sca;
	}
	
	public String toString(){
		return "in:"+_in+" gzIn:"+_gzIn+" redirs:"+_r+" gzRed:"+_gzR+" tbox:"+_tbox+" tmpdir:"+_tmpdir+" sortedC:"+_sortedCon+" cons:"+_cons+" graph:"+_graph+" igraph:"+_igraph+" rawgraph:"+_irgraph+" outdir:"+_out+" scatterDir:"+_scatterDir+" remoteGatherDir:"+_remoteGatherDir;
	}
	
	public static final String getDefaultSortedByContextFilename(String outdir){
		return getDefaultSortedByContext(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultSortedByContext(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_SORTED_BY_CONTEXT_GZ;
		return outdir+"/"+DEFAULT_SORTED_BY_CONTEXT_NGZ;
	}
	
	public static final String getDefaultContextsFilename(String outdir){
		return getDefaultContextsFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultContextsFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_CONTEXTS_FILENAME_GZ;
		return outdir+"/"+DEFAULT_CONTEXTS_FILENAME_NGZ;
	}
	
	public static final String getDefaultOutFinalFilename(String outdir){
		return getDefaultOutFinalFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultOutFinalFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_GZ;
		return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_NGZ;
	}
	
	public static final String getDefaultRawInvGraphFilename(String outdir){
		return getDefaultRawInvGraphFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultRawInvGraphFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_RAW_INV_GRAPH_FILENAME_GZ;
		return outdir+"/"+DEFAULT_RAW_INV_GRAPH_FILENAME_NGZ;
	}
	
	public static final String getDefaultInvGraphFilename(String outdir){
		return getDefaultInvGraphFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultInvGraphFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_INV_GRAPH_FILENAME_GZ;
		return outdir+"/"+DEFAULT_INV_GRAPH_FILENAME_NGZ;
	}
	
	public static final String getDefaultGraphFilename(String outdir){
		return getDefaultGraphFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultGraphFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_GRAPH_FILENAME_GZ;
		return outdir+"/"+DEFAULT_GRAPH_FILENAME_NGZ;
	}
	
	public static final String getDefaultTmpDir(String outdir){
		return outdir+"/"+DEFAULT_TMP_DIR;
	}
	
	public static final String getDefaultScatterDir(String outdir){
		return outdir+"/"+DEFAULT_SCATTER_DIR;
	}
	
	private String getDefaultRemoteGatherDir(String outdir) {
		return outdir+"/"+DEFAULT_REMOTE_GATHER_DIR;
	}
}
