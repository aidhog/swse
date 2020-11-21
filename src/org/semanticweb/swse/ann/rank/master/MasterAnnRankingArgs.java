package org.semanticweb.swse.ann.rank.master;

import org.deri.idrank.RankGraph;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.ann.rank.RMIAnnRankingServer;
import org.semanticweb.swse.ann.rank.SlaveAnnRankingArgs;

public class MasterAnnRankingArgs extends MasterArgs<MasterAnnRankingArgs>{
	public static final RMIAnnRankingServer DEFAULT_RI = new RMIAnnRankingServer();
	public static final MasterAnnRanker DEFAULT_MASTER = new MasterAnnRanker();
	
	public static final String DEFAULT_RANKS_FILENAME_NGZ = "ranks.all.nx";
	public static final String DEFAULT_RANKS_FILENAME_GZ = DEFAULT_RANKS_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_CONTEXTS_FILENAME_NGZ = "contexts.s.all.nx";
	public static final String DEFAULT_CONTEXTS_FILENAME_GZ = DEFAULT_CONTEXTS_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_GRAPH_FILENAME_NGZ = "graph.f.all.nx";
	public static final String DEFAULT_GRAPH_FILENAME_GZ = DEFAULT_GRAPH_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_INVGRAPH_FILENAME_NGZ = "invgraph.f.all.nx";
	public static final String DEFAULT_INVGRAPH_FILENAME_GZ = DEFAULT_INVGRAPH_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_TEMP_DIR = "tmp/";
	
	private boolean _gzranks = DEFAULT_GZ_OUT;
	private String _outdir = null;
	private String _contexts = null;
	private String _ranks = null;
	private String _graph = null;
	private String _invgraph = null;
	private String _tmpdir = null;
	private SlaveAnnRankingArgs _sra = null;
	private int _iters = 10;
	private double _damping = 0.85d;
	
	public MasterAnnRankingArgs(String outdir, SlaveAnnRankingArgs sra){
		_outdir = outdir;
		_sra = sra;
		initDefaults(outdir);
	}
	
	private void initDefaults(String outdir){
		_ranks = getDefaultRanksOut(outdir);
		_contexts = getDefaultContextsOut(outdir);
		_graph = getDefaultGraphOut(outdir);
		_invgraph = getDefaultInvGraphOut(outdir);
		_tmpdir = getDefaultTmpDir(outdir);
	}
	
	public void setGzRanks(boolean gzoutranks){
		_gzranks = gzoutranks;
	}
	
	public boolean getGzRanks(){
		return _gzranks;
	}
	
	public String getLocalRanks(){
		return _ranks;
	}
	
	public String getLocalContexts(){
		return _contexts;
	}
	
	public void setLocalContexts(String outcontexts){
		_contexts = outcontexts;
	}
	
	public String getLocalGraph(){
		return _graph;
	}
	
	public void setLocalGraph(String graphFn){
		_graph = graphFn;
	}
	
	public String getLocalInvGraph(){
		return _invgraph;
	}
	
	public void setLocalInvGraph(String invGraphFn){
		_invgraph = invGraphFn;
	}
	
	public void setIterations(int iters){
		_iters = iters;
	}
	
	public int getIterations(){
		return _iters;
	}
	
	public void setDamping(double damping){
		_damping = damping;
	}
	
	public double getDamping(){
		return _damping;
	}
	
	public void setTmpDir(String tmpDir){
		_tmpdir = tmpDir;
	}
	
	public String getTmpDir(){
		return _tmpdir;
	}
	
	public SlaveAnnRankingArgs getSlaveArgs(int server){
		return _sra.instantiate(server);
	}
	
	public SlaveAnnRankingArgs getSlaveArgs(){
		return _sra;
	}
	
	
	public String toString(){
		return "Master:: ranks:"+_ranks+" gzranks:"+_gzranks+" iters:"+_iters+" damp:"+_damping+" cons:"+_contexts+" graph:"+_graph+" invgraph:"+_invgraph+"\n"
			+"Slave:: "+_sra;
	}
	
	public static final String getDefaultRanksOut(String outdir){
		return getDefaultRanksOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultRanksOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_RANKS_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_RANKS_FILENAME_NGZ;
	}
	
	public static final String getDefaultContextsOut(String outdir){
		return getDefaultContextsOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultContextsOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_CONTEXTS_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_CONTEXTS_FILENAME_NGZ;
	}
	
	public static final String getDefaultGraphOut(String outdir){
		return getDefaultGraphOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultGraphOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_GRAPH_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_GRAPH_FILENAME_NGZ;
	}
	
	public static final String getDefaultInvGraphOut(String outdir){
		return getDefaultInvGraphOut(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultInvGraphOut(String outdir, boolean gz){
		String outdirl = RMIUtils.getLocalName(outdir);
		if(gz)
			return outdirl+"/"+DEFAULT_INVGRAPH_FILENAME_GZ;
		return outdirl+"/"+DEFAULT_INVGRAPH_FILENAME_NGZ;
	}
	
	public RMIAnnRankingServer getRMIInterface() {
		return DEFAULT_RI;
	}

	public MasterAnnRanker getTaskMaster() {
		return DEFAULT_MASTER;
	}
	
	public static final String getDefaultTmpDir(String outdir){
		return outdir+"/"+DEFAULT_TEMP_DIR;
	}
}