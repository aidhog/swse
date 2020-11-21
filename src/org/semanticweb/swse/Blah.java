package org.semanticweb.swse;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class Blah {
	public static void main(String[] args) throws IOException{
		System.err.println(Float.parseFloat("4.9816413E-6"));
		
		String[] servers = new String[]{"srv0", "srv1", "srv2", "srv3", "srv4", "srv5", "srv6", "srv7"};
		RMIRegistries regs = new RMIRegistries(servers, 1099);
		
//		System.err.println(regs.getServerNo(new Resource("http://aigp.csres.utexas.edu/~aigp/researcher/show/2202")));
//		System.err.println(regs.getServerNo(new Resource("http://aigp.csres.utexas.edu/~aigp/researcher/show/2203")));
//		System.err.println(regs.getServerNo(new Resource("http://aigp.csres.utexas.edu/~aigp/researcher/show/2204")));
//		System.err.println(regs.getServerNo(new Resource("http://aigp.csres.utexas.edu/~aigp/researcher/show/2205")));
//		System.err.println(regs.getServerNo(new Resource("http://aigp.csres.utexas.edu/~aigp/researcher/show/563")));
//		System.err.println(regs.getServerNo(new Resource("http://aigp.csres.utexas.edu/~aigp/researcher/show/674")));
//		System.err.println(regs.getServerNo(new Resource("http://aigp.csres.utexas.edu/~aigp/researcher/show/9710")));
		
//		System.err.println(regs.getServerNo(new Resource("http://blog.corrib.deri.ie/index.php?sioc_type=post&sioc_id=32")));
//		System.err.println(regs.getServerNo(new Resource("http://blog.corrib.deri.ie/index.php?sioc_type=post&sioc_id=10")));
//		System.err.println(regs.getServerNo(new Resource("http://dbpedia.org/resource/Frank_Zappa")));
		
		Logger log = Logger.getLogger("org.semanticweb.rmi.Blah");
		System.err.println(log.getUseParentHandlers());
		
		File f = new File("asd/asdwe/asd/");
		System.err.println(f.isDirectory());
		System.err.println(f.isFile());
		System.err.println(f.getParentFile());
	}
}
