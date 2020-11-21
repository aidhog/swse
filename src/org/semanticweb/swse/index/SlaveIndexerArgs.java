package org.semanticweb.swse.index;

import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;

public class SlaveIndexerArgs extends SlaveArgs {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -5970985808565350317L;
	
	public static final String TMP_DIR = "idxtmp/";
	public static final String DEFAULT_SCATTER_DIR = TMP_DIR+"scatter/";
	public static final String DEFAULT_GATHER_DIR = TMP_DIR+"gather/";
	public static final String DEFAULT_INDEX_FILE = "spoc.nqz";
	public static final String DEFAULT_SPARSE_FILE = "spoc.sparse.nxz";
	
	private String _outGatherDir;
	private String _outScatterDir;
	
	private String _index;
	private String _sparse;
	
	public SlaveIndexerArgs(String outScatterDir, String outGatherDir, String index, String sparse){
		_outScatterDir = outScatterDir;
		_outGatherDir = outGatherDir;
		_index = index;
		_sparse = sparse;
	}
	
	public String getOutGatherDir(){
		return _outGatherDir;
	}
	
	public String getOutScatterDir(){
		return _outScatterDir;
	}
	
	public String getOutIndex(){
		return _index;
	}
	
	public String getOutSparse(){
		return _sparse;
	}
	
	public SlaveIndexerArgs instantiate(int server) {
		String outScatterDir = RMIUtils.getLocalName(_outScatterDir, server);
		String outGatherDir = RMIUtils.getLocalName(_outGatherDir, server);
		
		String index = RMIUtils.getLocalName(_index, server);
		String sparse = RMIUtils.getLocalName(_sparse, server);
		
		SlaveIndexerArgs sca = new SlaveIndexerArgs(outScatterDir, outGatherDir, index, sparse);
		sca.setSlaveLog(RMIUtils.getLocalName(_logFile, server));
		return sca;
	}
	
	public String toString(){
		return "outScatterDir:"+_outScatterDir+" outGatherDir:"+_outGatherDir+" index:"+_index+" sparse:"+_sparse;
	}
	
	public static final String getDefaultScatterDir(String outdir){
		return outdir+DEFAULT_SCATTER_DIR;
	}
	
	public static final String getDefaultGatherDir(String outdir){
		return outdir+DEFAULT_GATHER_DIR;
	}
	
	public static final String getDefaultIndexFile(String outdir){
		return outdir+DEFAULT_INDEX_FILE;
	}
	
	public static final String getDefaultSparseFile(String outdir){
		return outdir+DEFAULT_SPARSE_FILE;
	}
}
