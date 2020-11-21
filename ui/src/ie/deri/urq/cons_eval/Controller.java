package ie.deri.urq.cons_eval;

import ie.deri.urq.cons_eval.SameAsIndex.SameAsList;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.semanticweb.yars.nx.BNode;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

public class Controller extends HttpServlet {
	private final Logger log = Logger.getLogger(Controller.class.getSimpleName());
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;



	private ServletContext _ctx;
	private File _data;
	private SameAsIndex _sal;
	private NS2Prefix _nsPrefix;


	/** private variables **/

	@Override
	public void init(ServletConfig config) throws ServletException {
		// initialise here all objects needed for the processing
		_ctx = config.getServletContext();
		_data = new File(_ctx.getInitParameter("DATA"));
		log.info("Data: "+_data);
		_sal = createSAL(new File(_data,"sameAs.nq.gz"));

		_nsPrefix = new NS2Prefix();

	}


	/**
	 * @param file
	 * @return
	 */
	private SameAsIndex createSAL(File file) {
		InputStream in;
		SameAsIndex sal = new SameAsIndex();
		try {
			in = new FileInputStream(file);
			if(file.getName().endsWith(".gz"))
				in = new GZIPInputStream(in);
			NxParser nxp = new NxParser(new InputStreamReader(in,"UTF-8"));
			Node [] n;
			while(nxp.hasNext()){
				n = nxp.next();
				sal.addSameAs(n[0], n[2]);
			}
			log.info("Loaded "+sal.size()+" same as statements");
			in.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return sal;
	}


	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		System.out.println("GET");
		doProcess(req, resp);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
	throws ServletException, IOException {
		System.out.println("POST");
		doProcess(req, resp);

	}

	private void doProcess(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		String op = request.getRequestURL().toString();
		op = op.substring(op.lastIndexOf("/")+1);
		response.setCharacterEncoding("UTF-8");
		request.setCharacterEncoding("UTF-8");
		
		Node curLeft = convertToNode(request.getParameter("leftURI"));
		Node curRight = convertToNode(request.getParameter("rightURI"));

		if(op.equals("next")){
			if(request.getParameter("class")!=null){
				//just pick two new URIs
				boolean run = true;
				do{
					if(curLeft.toString().length()==0){
						curLeft = _sal.getIndex().keySet().iterator().next();
						run = false;
						log.info("init: just select the first one");
					}else{
						int rand = (int) ((Math.random()* _sal.size()) +1);
						for(Entry<Node, SameAsList> ent : _sal.getIndex().entrySet()){
							if(rand == 1 && !ent.getValue().contains(curLeft)){
								curLeft = ent.getKey();
								run = false;
								break;
							}
							rand--;
						}
					}
				}while(run);

				run = true;
				do{
					int rand = (int) ((Math.random()* _sal.getSameAsList(curLeft).size()) +1);
					for(Node ent : _sal.getSameAsList(curLeft)){
						if(rand == 1 && !ent.equals(curLeft)){
							curRight = ent;
							run = false;
							break;
						}
						rand--;
					}
				}while(run);
			}
			else if(request.getParameter("left")!=null && curRight!=null){
				//just pick a new left URI
				boolean run = true;
				do{
					int rand = (int) ((Math.random()* _sal.getSameAsList(curRight).size()) +1);
					for(Node ent : _sal.getSameAsList(curRight)){
						if(rand == 1 && !ent.equals(curLeft)){
							curLeft = ent;
							run = false;
							break;
						}
						rand--;
					}
					
				}while(run);
			}else if(request.getParameter("right")!=null  && curLeft!=null){
				log.info("right change request");
				//just pick a new right URI
				boolean run = true;
				do{
					int rand = (int) ((Math.random()* _sal.getSameAsList(curLeft).size()) +1);
					for(Node ent : _sal.getSameAsList(curLeft)){
						if(rand == 1 && !ent.equals(curLeft)){
							curRight = ent;
							run = false;
							break;
						}
						rand--;
					}
					
				}while(run);
			} 
			else{
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Could not determine which instance should be changed!");
			}
		} 
		else{
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "operation "+op+" no supported");
		}
		printResponse(response.getWriter(), curLeft,curRight);

	}

	/**
	 * @param parameter
	 * @return
	 */
	private Node convertToNode(String parameter) {
		if(parameter==null) return null;
		try {
			URL u = new URL(parameter);
			return new Resource(new String(parameter.getBytes("UTF-8")));
		} catch (MalformedURLException e) {
			try {
				return new BNode(new String(parameter.getBytes("UTF-8")));
			} catch (UnsupportedEncodingException e1) {
				return new BNode(parameter);
			}
		} catch (UnsupportedEncodingException e) {
			return new Resource(parameter);
			
		}
	}


	/**
	 * @param writer
	 * @param curLeft
	 * @param curRight
	 */
	private void printResponse(PrintWriter writer, Node curLeft,
			Node curRight) {
		writer.write(getHeader(_sal.getSameAsList(curLeft).size()));

		writer.write(getStatements(curLeft,"left"));
		writer.write(getStatements(curRight,"right"));


		writer.write(getFooter());

	}

	/**
	 * @param curLeft
	 * @param string
	 * @return
	 */
	private String getStatements(Node curLeft, String string) {
		StringBuilder sb = new StringBuilder();
		sb.append("<div id='").append(string).append("'>\n<h2 class='rnd'>\n");
		if(curLeft instanceof Resource)
			sb.append("<a href='").append(curLeft.toString()).append("'>").append(curLeft.toString()).append("</a>\n</h2>\n");
		else
			sb.append(curLeft.toString()).append("'>").append(curLeft.toString()).append("\n</h2>\n");
		sb.append("<input class='hide' type='hidden' name='").append(string).append("URI' value='").append(curLeft.toString()).append("' />").
		append("<ul class='statements'>\n");
		//        
		//		for(Node [] stmt: outLinks.get(curLeft)){
		//		sb.append("<li class='out'>\n").
		//		append("<span class='prop rnd'><a href='").append(stmt[1]).append("'>").append(_nsPrefix.renameNamespace(stmt[1].toString())).append("</a></span>\n").
		//		append("<span class='value rnd'><a href='").append(stmt[2]).append("'>").append(_nsPrefix.renameNamespace(stmt[2].toString())).append("</a></span>\n").
		//		append("</li>\n");
		//		}
		//        
		//		for(Node [] stmt: inLinks.get(curLeft)){
		//			sb.append("<li class='in'>\n").
		//			append("<span class='prop rnd'><a href='").append(stmt[1]).append("'>").append(_nsPrefix.renameNamespace(stmt[1].toString())).append("</a></span>\n").
		//			append("<span class='value rnd'><a href='").append(stmt[0]).append("'>").append(_nsPrefix.renameNamespace(stmt[0].toString())).append("</a></span>\n").
		//			append("</li>\n");
		//			}
		//             
		sb.append("</ul>\n<input type='submit' value='Next Instance >>' name='").append(string).append("'/>\n</div>");    
		return sb.toString();
		
	}


	/**
	 * @return
	 */
	private String getFooter() {
		return "\n</div>\n</form>\n<div id='footer'>\nfooter\n</div>\n</div>\n</body>\n</html>";//.replaceAll("'","\"");
	}

	/**
	 * @param i 
	 * @return
	 */
	private String getHeader(int i) {
		StringBuilder sb = new StringBuilder();
		sb.append("<?xml version='1.0' encoding='UTF-8'?>\n")
		.append("<!DOCTYPE html PUBLIC '-//W3C//DTD XHTML+RDFa 1.0//EN' 'http://www.w3.org/MarkUp/DTD/xhtml-rdfa-1.dtd'>\n")
		.append("<html xmlns='http://www.w3.org/1999/xhtml'>\n")
		.append("<head>\n")
		.append("<META http-equiv='Content-Type' content='text/html; charset=UTF-8' />\n")
		.append("<title>Consolidation Evaluation UI</title>\n")
		.append("<link type='text/css' rel='Stylesheet' href='css/style.css' />\n")
		.append("</head>\n").append("<body>\n")
		.append("<div id='frame'>\n").append("<div id='head'>\n")
		.append("head\n").append("</div>\n")
		.append("<form name='input' action='next' method='get'>\n")
		.append("<div id='getNewClass'>\n")
		.append("<input type='submit' value='Next Class >>' name='class' />\n")
		.append("<p>This equivalence class contains ").append(i).append(" instances</p>\n")
		.append("</div>\n")
		.append("<div id='comp'>\n");
		
		
		return sb.toString();
		
	}


	/**
	 * 
	 * @param e
	 * @return
	 */
	private static String getStackTraceAsString(Exception e) {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		PrintWriter writer = new PrintWriter(bytes, true);
		e.printStackTrace(writer);
		return bytes.toString();
	}

	private String debugParameters(HttpServletRequest req) {

		String para = "requestedURI: " + req.getRequestURI() + "\n"
		+ "Session " + req.getSession().getId() + "\n";
		Enumeration paraNames = req.getParameterNames();
		para += "Parameters\n";
		while (paraNames.hasMoreElements()) {
			Object name = paraNames.nextElement();
			para += " " + name + ": " + req.getParameter(name.toString())
			+ "\n";
		}
		para += "Session beans\n";
		Enumeration sessionParaNames = req.getSession().getAttributeNames();
		while (sessionParaNames.hasMoreElements()) {
			Object name = sessionParaNames.nextElement();
			para += " " + name + ": "
			+ req.getSession().getAttribute((String) name) + "\n";
		}
		return para;
	}
}