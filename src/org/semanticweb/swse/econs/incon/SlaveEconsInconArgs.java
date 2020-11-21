package org.semanticweb.swse.econs.incon;

import java.util.ArrayList;

import org.semanticweb.saorr.fragments.Fragment;
import org.semanticweb.saorr.rules.Rule;
import org.semanticweb.swse.MasterArgs;
import org.semanticweb.swse.RMIUtils;
import org.semanticweb.swse.SlaveArgs;
import org.semanticweb.swse.econs.incon.utils.WOLC_Slim_T_SPLIT;
import org.semanticweb.yars.nx.BooleanLiteral;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.dt.DatatypeParseException;
import org.semanticweb.yars.nx.dt.XSDDatatypeMap;
import org.semanticweb.yars2.query.algebra.filter.BinaryOperator;
import org.semanticweb.yars2.query.algebra.filter.FilterOperator;
import org.semanticweb.yars2.query.algebra.filter.UnsupportedArgumentException;

public class SlaveEconsInconArgs extends SlaveArgs {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2077478594392035258L;

	public static final String DEFAULT_TMP_DIR = "tmp/";
	
	public static final String DEFAULT_OUT_FINAL_FILENAME_NGZ = "data.final.nx";
	public static final String DEFAULT_OUT_FINAL_FILENAME_GZ = DEFAULT_OUT_FINAL_FILENAME_NGZ+".gz";
	
	public static final String DEFAULT_INCONSISTENCIES_FILENAME_NGZ = "inconsist.dat";
	public static final String DEFAULT_INCONSISTENCIES_FILENAME_GZ = DEFAULT_INCONSISTENCIES_FILENAME_NGZ+".gz";
	
	public static final int DEFAULT_BUFFER_SIZE = 50000;
	
	public static Rule[] DEFAULT_RULES = Fragment.getRules(WOLC_Slim_T_SPLIT.class);
	
	private Rule[] _rules = DEFAULT_RULES;
	
	private String _inSpoc;
	private boolean _gzInSpoc = MasterArgs.DEFAULT_GZ_IN;
	
	private String _inTbox = null;
	private boolean _gzInTbox = MasterArgs.DEFAULT_GZ_IN;
	
	private String _inPredStats = null;
	private boolean _gzInPredStats = MasterArgs.DEFAULT_GZ_IN;
	
	private String _inOpsc;
	private boolean _gzInOpsc = MasterArgs.DEFAULT_GZ_IN;
	
	private String _outData;
	private boolean _gzOutData = MasterArgs.DEFAULT_GZ_OUT;
	
	private String _outIc;
	private boolean _gzOutIc = MasterArgs.DEFAULT_GZ_OUT;
	
	private String _tmpdir;
	
	private String _outdir = null;
	
	private int _buf = DEFAULT_BUFFER_SIZE; 
	
	public SlaveEconsInconArgs(String inSpoc, String inOpsc, String inTbox, String inPredStats, String outdir){
		_inSpoc = inSpoc;
		_inOpsc = inOpsc;
		_inTbox = inTbox;
		_inPredStats = inPredStats;
		_outdir = outdir;
		initDefaults(outdir);
	}
	
	public String getOutDir(){
		return _outdir;
	}
	
	public String getOutData(){
		return _outData;
	}
	
	public String getOutInconsistencies(){
		return _outIc;
	}
	
	public String getInOpsc(){
		return _inOpsc;
	}
	
	public void setGzInOpsc(boolean gzin){
		_gzInOpsc = gzin;
	}
	
	public int getBufSize(){
		return _buf;
	}
	
	public void setBufSize(int buf){
		_buf = buf;
	}
	
	public boolean getGzInOpsc(){
		return _gzInOpsc;
	}
	
	public String getInSpoc(){
		return _inSpoc;
	}
	
	public void setGzInSpoc(boolean gzin){
		_gzInSpoc = gzin;
	}
	
	public boolean getGzInSpoc(){
		return _gzInSpoc;
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
	
	public void setGzInconsistencies(boolean gzic){
		_gzOutIc = gzic;
	}
	
	public boolean getGzInconsistencies(){
		return _gzOutIc;
	}
	
	public void setGzData(boolean gzdata){
		_gzOutData = gzdata;
	}
	
	public boolean getGzData(){
		return _gzOutData;
	}
	
	private void initDefaults(String outdir){
		_tmpdir = getDefaultTmpDir(outdir);
		_outData = getDefaultOutFinalFilename(outdir);
		_outIc = getDefaultOutInconsistencies(outdir);
	}
	
	public void setGzTboxIn(boolean gzInTbox){
		_gzInTbox = gzInTbox;
	}
	
	public boolean getGzTboxIn(){
		return _gzInTbox;
	}
	
	public String getTboxIn(){
		return _inTbox;
	}
	
	public void setGzPredStatsIn(boolean gzInPredStats){
		_gzInPredStats = gzInPredStats;
	}
	
	public boolean getGzPredStatsIn(){
		return _gzInPredStats;
	}
	
	public String getPredStatsIn(){
		return _inPredStats;
	}
	
	public SlaveEconsInconArgs instantiate(int server) {
		String inOpsc = RMIUtils.getLocalName(_inOpsc, server);
		String inSpoc = RMIUtils.getLocalName(_inSpoc, server);
		String inTbox = RMIUtils.getLocalName(_inTbox, server);
		String inPredStats = RMIUtils.getLocalName(_inPredStats, server);
		String tmpdir = RMIUtils.getLocalName(_tmpdir, server);
		String out = RMIUtils.getLocalName(_outdir, server);
		String log = RMIUtils.getLocalName(_logFile, server);
		String incon = RMIUtils.getLocalName(_outIc, server);
		String outdata = RMIUtils.getLocalName(_outData, server);
		
		SlaveEconsInconArgs saa = new SlaveEconsInconArgs(inSpoc, inOpsc, inTbox, inPredStats, out);
		saa._outData = outdata;
		saa._outIc = incon;
		saa.setGzData(_gzOutData);
		saa.setGzInOpsc(_gzInOpsc);
		saa.setGzInSpoc(_gzInSpoc);
		saa.setGzTboxIn(_gzInTbox);
		saa.setGzPredStatsIn(_gzInPredStats);
		saa.setGzInconsistencies(_gzOutIc);
		saa.setSlaveLog(log);
		saa.setTmpDir(tmpdir);
		saa.setRules(_rules);
		
		return saa;
	}
	
	public String toString(){
		return "inPS:"+_inPredStats+" gzInPS:"+_gzInPredStats+"inT:"+_inTbox+" gzInT:"+_gzInTbox+"inS:"+_inSpoc+" gzInS:"+_gzInSpoc+"inO:"+_inOpsc+" gzInO:"+_gzInOpsc+" tmpdir:"+_tmpdir+" outdir:"+_outdir+" incon:"+_outIc+" incongz:"+_gzOutIc+" outdata:"+_outData+" outdatagz:"+_gzOutData;
	}
	
	public static final String getDefaultOutFinalFilename(String outdir){
		return getDefaultOutFinalFilename(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultOutFinalFilename(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_GZ;
		return outdir+"/"+DEFAULT_OUT_FINAL_FILENAME_NGZ;
	}
	
	public static final String getDefaultOutInconsistencies(String outdir){
		return getDefaultOutInconsistencies(outdir, MasterArgs.DEFAULT_GZ_OUT);
	}
	
	public static final String getDefaultOutInconsistencies(String outdir, boolean gz){
		if(gz)
			return outdir+"/"+DEFAULT_INCONSISTENCIES_FILENAME_GZ;
		return outdir+"/"+DEFAULT_INCONSISTENCIES_FILENAME_GZ;
	}
	
	public static final String getDefaultTmpDir(String outdir){
		return outdir+"/"+DEFAULT_TMP_DIR;
	}
	
	public static class FuzzyMatchOperator extends BinaryOperator{
		
		public FuzzyMatchOperator() {
			super();
		}
		
		public FuzzyMatchOperator(ArrayList<FilterOperator> args) throws UnsupportedArgumentException {
			super(args);
		}
		
		public FuzzyMatchOperator(FilterOperator arg1, FilterOperator arg2){
			super(arg1, arg2);
		}
		
		protected Node executeOperation(ArrayList<Node> ns)
				throws UnsupportedArgumentException {
			Node n1 = ns.get(0);
			Node n2 = ns.get(1);
			if(n1==null || n2==null)
				return null;
			else if(n1.equals(n2))
				return BooleanLiteral.TRUE;
			else if(n1 instanceof Literal && n2 instanceof Literal){	
				Literal l1 = (Literal)n1;
				Literal l2 = (Literal)n2;
				
				if(n1.toString().equalsIgnoreCase(n2.toString())){
					return BooleanLiteral.TRUE;
				} 
				
				String ns1 = normalise(n1.toString());
				String ns2 = normalise(n2.toString());
				
				if(ns1.equalsIgnoreCase(ns2)){
					return BooleanLiteral.TRUE;
				}
				
				try{
					if(Double.compare(Double.parseDouble(ns1), Double.parseDouble(ns2))==0)
						return BooleanLiteral.TRUE;
				} catch(NumberFormatException nfe){
					;
				}
				
				try {
					Literal canon1 = XSDDatatypeMap.getCanonicalLiteral(l1);
					Literal canon2 = XSDDatatypeMap.getCanonicalLiteral(l2);
					
					if(canon1.equals(canon2))
						return BooleanLiteral.TRUE;
				} catch (DatatypeParseException e) {
					;
				}
				
				return BooleanLiteral.FALSE;
			}
			else return BooleanLiteral.TRUE;
		}
		
		public String toString(){
			return "FuzzyMatch( "+_args.get(0)+", "+_args.get(1)+" )";
		}
		
		private static String normalise(String s){
			return s.trim();
		}
		
	}
}
