package org.semanticweb.swse.lucene.utils;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;
import org.semanticweb.saorr.auth.RedirectsAuthorityInspector;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.namespace.DC;
import org.semanticweb.yars.nx.namespace.DCTERMS;
import org.semanticweb.yars.nx.namespace.GEO;
import org.semanticweb.yars.nx.namespace.OWL;
import org.semanticweb.yars.nx.namespace.RDF;
import org.semanticweb.yars.nx.namespace.RDFS;
import org.semanticweb.yars.nx.namespace.SIOC;
import org.semanticweb.yars.nx.namespace.XSD;
import org.semanticweb.yars.nx.parser.ParseException;
import org.semanticweb.yars.util.CheckSortedIterator;

import com.ontologycentral.ldspider.http.Headers;

public class LuceneIndexBuilder {
	static Logger _log = Logger.getLogger(LuceneIndexBuilder.class.getName());
	
	public static final int MERGE_FACTOR = 20;

	public static float MIN_RANK = 0.0000001f;

	public static final int MAX_LITERAL_LENGTH = 100000;
	
	public static final int TICKS = 1000000; 
	
	public static final String[] IMG_FILE_EXTENSIONS = new String[]{
		".jpg", ".jpeg", ".gif", ".png", ".bmp", ".tif",
	};
	
	public static final Node[] LABEL_PROPERTIES = new Node[]{
		RDFS.LABEL, DC.TITLE, DCTERMS.TITLE, new Resource("http://purl.org/rss/1.0/title"),
		SIOC.NAME
	};
	
	private static final Set<Node> LABEL_PROPERTIES_SET = new HashSet<Node>();
	
	static{
		for(Node n:LABEL_PROPERTIES){
			LABEL_PROPERTIES_SET.add(n);
		}
	}
	
	public static final Node[] COMMENT_PROPERTIES = new Node[]{
		RDFS.COMMENT, DC.DESCRIPTION, new Resource("http://purl.org/rss/1.0/description"),
		SIOC.CONTENT, DCTERMS.DESCRIPTION, new Resource("http://purl.org/vocab/bio/0.1/olb")
	};
	
	private static final Set<Node> COMMENT_PROPERTIES_SET = new HashSet<Node>();
	
	static{
		for(Node n:COMMENT_PROPERTIES){
			COMMENT_PROPERTIES_SET.add(n);
		}
	}
	
	public static final Node[] DATE_PROPERTIES = new Node[]{
		DC.DATE
	};
	
	private static final Set<Node> DATE_PROPERTIES_SET = new HashSet<Node>();
	
	static{
		for(Node n:DATE_PROPERTIES){
			DATE_PROPERTIES_SET.add(n);
		}
	}

	public static void buildLucene(Iterator<Node[]> in, Iterator<Node[]> ranks, RedirectsAuthorityInspector rai, String dir) throws CorruptIndexException, LockObtainFailedException, IOException, ParseException{
		Analyzer sa = new StandardAnalyzer(Version.LUCENE_CURRENT);
		File f = new File(dir);
		NIOFSDirectory fsd = new NIOFSDirectory(f);
		IndexWriter writer = new IndexWriter(fsd, sa, IndexWriter.MaxFieldLength.LIMITED);
		writer.setMergeFactor(MERGE_FACTOR);

		buildKeyword(in, ranks, rai, writer);

		_log.info("Optimising index...");
		long b4 = System.currentTimeMillis();
		writer.optimize();
		_log.info("optimised index in "+ (System.currentTimeMillis()-b4)+" ms.");
		_log.info("Closing index...");
		b4 = System.currentTimeMillis();
		writer.close();
		_log.info("closed index in "+ (System.currentTimeMillis()-b4)+" ms.");
		_log.info("...done!");
	}

	protected static void buildKeyword(Iterator<Node[]> it, Iterator<Node[]> ranks, RedirectsAuthorityInspector rai, IndexWriter writer) throws IOException, ParseException {
		Node oldSub = null;
		Node[] q = null;
		Node subject = null;
		
		_log.info("Performing keyword build...");
		
		DocumentRepresentation dr = null;
		
		float rank;

		long b4 = System.currentTimeMillis();
		
		Node[] rna = null;

		if (ranks.hasNext()) {
			rna = ranks.next();
		}
		
		int c=0;
		
//		TreeSet<Node> done = new TreeSet<Node>();

//		Resource debugA = new Resource("http://aidanhogan.com/");
//		Resource debugB = new Resource("http://harth.org/andreas/foaf#ah");
//		Resource debugF = new Resource("http://purl.org/dc/terms/Agent");
		
		CheckSortedIterator vs = new CheckSortedIterator(it);
		it = vs;
		
		boolean index = false;
		while (it.hasNext()) {	
			c++;
			if(c%TICKS==0){
				_log.info("...read "+c+" statements...");
			}
			
			
			
			q = it.next();
			
			if(!vs.isOkay())
				throw new RuntimeException(vs.getException());

			subject = q[0];
			
//			if(subject.equals(debugF)){
//				System.err.println("Agent "+c+" "+Nodes.toN3(q));
//			}
			
			// need to create a Lucene document with all information per subject
			if(oldSub==null){
				oldSub = subject;
				rank = MIN_RANK;
				
//				if(subject.equals(debugF)){
//					System.err.println("Agent null "+oldSub+" "+subject+" "+Nodes.toN3(rna)+" "+c);
//				} 
				
				while (rna!=null && rna[0].compareTo(subject) < 0 && ranks.hasNext()) {
					rna = ranks.next();
					
				}
				
//				if(subject.equals(debugF)){
//					System.err.println("Agent null "+oldSub+" "+subject+" "+Nodes.toN3(rna)+" "+c);
//				} 
				
				if(rna[0].equals(subject)){
					rank = getRank(Float.parseFloat(rna[1].toString()));
				}
				dr = new DocumentRepresentation(subject, rank);
				
				index = false;
			}
			else if(!subject.equals(oldSub)){
				rank = MIN_RANK;
				
//				if(subject.equals(debugA)){
//					System.err.println("Aidan "+oldSub+" "+subject+" "+Nodes.toN3(rna)+" "+index+" "+c);
//				} else if(subject.equals(debugB)){
//					System.err.println("Andreas "+oldSub+" "+subject+" "+Nodes.toN3(rna)+" "+index+" "+c);
//				} else if(subject.equals(debugF)){
//					System.err.println("Agent "+oldSub+" "+subject+" "+Nodes.toN3(rna)+" "+index+" "+c);
//				} 
//				
//				if(oldSub.equals(debugF)){
//					System.err.println("Agent old "+oldSub+" "+subject+" "+Nodes.toN3(rna)+" "+index+" "+c);
//				} 
				
				while (rna!=null && rna[0].compareTo(subject) < 0 && ranks.hasNext()) {
					rna = ranks.next();
//					if(subject.equals(debugA)){
//						System.err.println("Aidan  "+subject+" "+Nodes.toN3(rna)+" "+c);
//					} else if(subject.equals(debugB)){
//						System.err.println("Andreas "+subject+" "+Nodes.toN3(rna)+" "+c);
//					} else if(subject.equals(debugF)){
//						System.err.println("Agent "+subject+" "+Nodes.toN3(rna)+" "+c);
//					}
				}
				
//				if(oldSub.equals(debugF)){
//					System.err.println("Agent old "+oldSub+" "+subject+" "+Nodes.toN3(rna)+" "+index+" "+c);
//				} 
//				
//				if(subject.equals(debugA)){
//					System.err.println("Aidan  "+subject+" final: "+Nodes.toN3(rna)+" "+rna[0].equals(subject)+" "+c);
//				} else if(subject.equals(debugB)){
//					System.err.println("Andreas "+subject+" final: "+Nodes.toN3(rna)+" "+rna[0].equals(subject)+" "+c);
//				} else if(subject.equals(debugF)){
//					System.err.println("Agent "+subject+" final: "+Nodes.toN3(rna)+" "+rna[0].equals(subject)+" "+c);
//				}
				
				if(rna[0].equals(subject)){
					rank = getRank(Float.parseFloat(rna[1].toString()));
//					if(subject.equals(debugA)){
//						System.err.println("Aidan rank "+rank+" "+c);
//					} else if(subject.equals(debugB)){
//						System.err.println("Andreas rank "+rank+" "+c);
//					} else if(subject.equals(debugF)){
//						System.err.println("Agent rank "+rank+" "+c);
//					}
				}
					// index all URIs with some literals attached
				
				if(index)
					store(dr, writer);
				
				
				dr = new DocumentRepresentation(subject, rank);
				
//				if(subject.equals(debugF)){
//					System.err.println("Agent dr "+dr.getSub()+" "+dr.getRank());
//				}
//				if(oldSub.equals(debugF)){
//					System.err.println("Agent old "+oldSub+" "+subject+" "+Nodes.toN3(rna)+" "+index);
//				} 
				
				index = false;
			}
			
			if(!q[1].equals(Headers.HEADERINFO)
					&& !q[1].equals(OWL.SAMEAS)){
				index = true;
			}
			
			dr.addStatement(q, rai.checkAuthority(q[0], q[3]));

			oldSub = subject;
		}
		
		if(dr!=null && index)
			store(dr, writer);
		
		_log.info("...finished keyword build in "+(System.currentTimeMillis()-b4)+" ms.");
	}

	static float getRank(float rank){
		return MIN_RANK + (float)(1f-MIN_RANK)*rank;
	}

	static String toStr(Set<String> set) {
		StringBuffer sb = new StringBuffer();

		for (String s: set) {
			sb.append(s);
			sb.append(" ");
		}

		return sb.toString();
	}

	static StringBuffer tokenize(Node n) {
		StringBuffer sb = new StringBuffer();

		if (!(n instanceof Resource)) {
			return sb;
		}

		try {
			URL u = new URL(n.toString());
			StringTokenizer tok = new StringTokenizer(u.getHost());
			while (tok.hasMoreElements()) {
				sb.append(tok.nextToken());
				sb.append(" ");
			}
			tok = new StringTokenizer(u.getPath());
			while (tok.hasMoreElements()) {
				sb.append(tok.nextToken());
				sb.append(" ");
			}
		} catch (MalformedURLException mue) {
			System.err.println("no host for " + n.toString());
		}

		return sb;
	}

	static Set<String> tokenizeSet(Node n) {
		HashSet<String> sb = new HashSet<String>();

		if (!(n instanceof Resource)) {
			return sb;
		}

		try {
			URL u = new URL(n.toString());
			StringTokenizer tok = new StringTokenizer(u.getHost());
			while (tok.hasMoreElements()) {
				sb.add(tok.nextToken());
			}
			tok = new StringTokenizer(u.getPath());
			while (tok.hasMoreElements()) {
				sb.add(tok.nextToken());
			}
		} catch (MalformedURLException mue) {
			System.err.println("no host for " + n.toString());
		}

		return sb;
	}

	static void store(DocumentRepresentation dr, IndexWriter iw) throws CorruptIndexException, IOException {
//		Node sub = dr.getSub();
//		Resource debugF = new Resource("http://purl.org/dc/terms/Agent");
//		if(sub.equals(debugF)){
//			System.err.println("Agent storing.");
//		}
		
//		if(sub instanceof Resource){
//			try {
//				URL u = new URL(sub.toString());
//				StringTokenizer tok = new StringTokenizer(u.getHost());
//				while (tok.hasMoreElements()) {
//					dr.addText(tok.nextToken());
//				}
//				tok = new StringTokenizer(u.getPath());
//				while (tok.hasMoreElements()) {
//					dr.addText(tok.nextToken());
//				}
//			} catch (MalformedURLException mue) {
////				System.err.println("no host for " + sub.toString());
//			}
//		}

		Document doc = DocumentRepresentation.toDocument(dr);
		iw.addDocument(doc);
	}

	public static class DocumentRepresentation{
		
		public static final String SUBJECT = "subject";
		
		public static final String KEYWORDS = "keywords";
		
		public static final String LABEL_TEXT = "labeltext";
		
		public static final String LABELS = "labels";
		public static final String AUTH_LABELS = "authlabels";
		
		public static final String COMMENTS = "comments";
		public static final String AUTH_COMMENTS = "authcomments";
		
		public static final String TYPES = "types";
		public static final String AUTH_TYPES = "authtypes";
		
		public static final String DATES = "dates";
		public static final String AUTH_DATES = "authdates";
		
		public static final String RANK = "rank";
		
		public static final String SAMEAS = "sameas";
		
		public static final String LAT = "lat";
		public static final String LONG = "long";
		public static final String IMG = "img";
		
		public static final String DATE = "date";
		
		private Node _sub = null;
		private float _rank = MIN_RANK;
		private StringBuffer _text = null;
		private StringBuffer _labelText = null;

		private Set<Literal> _labels = null;
		private Set<Literal> _authLabels = null;

		private Set<Literal> _comments = null;
		private Set<Literal> _authComments = null;
		
		private Set<Literal> _authDates = null;
		private Set<Literal> _dates = null;
		
		private Set<Node> _authTypes = null;
		private Set<Node> _types = null;
		
		private Set<Resource> _sameas = null;

		private Set<Resource> _imgs = null;
		private Literal _lat = null, _lon = null;

		public DocumentRepresentation(Node sub, float rank){
			_sub = sub;
			_rank = rank;
			
//			Resource debugF = new Resource("http://purl.org/dc/terms/Agent");
//			if(sub.equals(debugF)){
//				System.err.println("Agent creating doc."+rank);
//			}
		}

		public Node getSub() {
			return _sub;
		}

		public float getRank() {
			return _rank;
		}

		public String getText() {
			if(_text==null)
				return null;
			return _text.toString().trim();
		}

		public void addText(Literal text) {
			if(_text==null){
				_text = new StringBuffer();
			}
			_text.append(text);
			_text.append(" ");
		}
		
		public void addText(String text) {
			if(_text==null){
				_text = new StringBuffer();
			}
			_text.append(text);
			_text.append(" ");
		}

		public String getLabelText() {
			if(_labelText==null)
				return null;
			return _labelText.toString().trim();
		}

		public void addLabelText(Literal label) {
			if(_labelText==null)
				_labelText = new StringBuffer();
			_labelText.append(label);
			_labelText.append(" ");
		}

		public Set<Literal> getLabels() {
			return _labels;
		}

		public void addLabel(Literal label) {
			if(_labels==null){
				_labels = new HashSet<Literal>();
			}
			_labels.add(label);
		}
		
		public Set<Node> getTypes() {
			return _types;
		}

		public void addType(Node type) {
			if(_types==null){
				_types = new HashSet<Node>();
			}
			_types.add(type);
		}
		
		public Set<Resource> getSameAs() {
			return _sameas;
		}

		public void addSameAs(Resource sameas) {
			if(_sameas==null){
				_sameas = new HashSet<Resource>();
			}
			_sameas.add(sameas);
		}
		
		public Set<Node> getAuthTypes() {
			return _authTypes;
		}

		public void addAuthType(Node type) {
			if(_authTypes==null){
				_authTypes = new HashSet<Node>();
			}
			_authTypes.add(type);
		}

		public Set<Literal> getAuthLabels() {
			return _authLabels;
		}

		public void addAuthLabel(Literal authLabel) {
			if(_authLabels==null){
				_authLabels = new HashSet<Literal>();
			}
			_authLabels.add(authLabel);
		}

		public Set<Literal> getComments() {
			return _comments;
		}

		public void addComment(Literal comment) {
			if(_comments==null){
				_comments = new HashSet<Literal>();
			}
			_comments.add(comment);
		}

		public Set<Literal> getAuthComments() {
			return _authComments;
		}

		public void addAuthComment(Literal authComment) {
			if(_authComments==null){
				_authComments = new HashSet<Literal>();
			}
			_authComments.add(authComment);
		}
		
		public Set<Literal> getDates() {
			return _dates;
		}

		public void addDate(Literal date) {
			if(_dates==null){
				_dates = new HashSet<Literal>();
			}
			_dates.add(date);
		}

		public Set<Literal> getAuthDates() {
			return _authDates;
		}

		public void addAuthDate(Literal authDate) {
			if(_authDates==null){
				_authDates = new HashSet<Literal>();
			}
			_authDates.add(authDate);
		}

		public Set<Resource> getImages() {
			return _imgs;
		}

		public void addImage(Resource img) {
			if(_imgs==null){
				_imgs = new HashSet<Resource>();
			}
			_imgs.add(img);
		}

		public Literal getLat() {
			return _lat;
		}

		public void setLat(Literal lat) {
			_lat = lat;
		}

		public Literal getLong() {
			return _lon;
		}

		public void setLong(Literal lon) {
			_lon = lon;
		}

		public void addStatement(Node[] na, boolean subjAuth){
			Node object = na[2];
			if(object instanceof Literal){
				String str = object.toString();
				if (str.length() < MAX_LITERAL_LENGTH) {
					Literal ol = (Literal) object;
					if(LABEL_PROPERTIES_SET.contains(na[1])){
						if(subjAuth){
							addAuthLabel(ol);
						} else{
							addLabel(ol);
						}
						addLabelText(ol);
					} else if(COMMENT_PROPERTIES_SET.contains(na[1])){
						if(subjAuth){
							addAuthComment(ol);
						} else{
							addComment(ol);
						}
						addText(ol);
					} else if(DATE_PROPERTIES_SET.contains(na[1]) || (ol.getDatatype()!=null
							&& (ol.getDatatype().equals(XSD.DATE) || ol.getDatatype().equals(XSD.DATETIME)
									|| ol.getDatatype().equals(XSD.DATETIMESTAMP)))){
						if(subjAuth){
							addAuthDate(ol);	
						} else{
							addDate(ol);
						}
						addText(ol);
					}else if(na[1].equals(GEO.LAT)){
						_lat = ol;
						addText(ol);
					} else if(na[1].equals(GEO.LONG)){
						_lon = ol;
						addText(ol);
					} else{
						addText(ol);
					}
				}
			} else if(object instanceof Resource){
				if(na[1].equals(RDF.TYPE)){
					if(subjAuth){
						addAuthType(na[2]);
					} else{
						addType(na[2]);
					}
				} else if(na[1].equals(OWL.SAMEAS)){
					addSameAs((Resource)na[2]);
				} else{
					String r = object.toString().toLowerCase();
					if(r.startsWith("http")){
						for(String fe:IMG_FILE_EXTENSIONS){
							if(r.endsWith(fe)){
								addImage((Resource)object);
							}
						}
					}
				}
			}
		}
		
		static Document toDocument(DocumentRepresentation dr) throws java.io.FileNotFoundException, UnsupportedEncodingException {
			// make a new, empty document
			Document doc = new Document();

			doc.setBoost(dr.getRank());
			doc.add(new Field(SUBJECT, dr.getSub().toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			
			String labelText = dr.getLabelText();
			if(labelText!=null && !labelText.isEmpty()){
				Field f = new Field(LABEL_TEXT, labelText, Field.Store.YES, Field.Index.ANALYZED);
				f.setBoost(10.0f);
				doc.add(f);
			}
			
			String text = dr.getText();
			if(text!=null && !text.isEmpty()){
				Field f = new Field(KEYWORDS, text, Field.Store.YES, Field.Index.ANALYZED);
				doc.add(f);
			}

			doc.add(new Field(RANK, Float.toString(dr.getRank()), Field.Store.YES, Field.Index.NOT_ANALYZED));
			
			Set<Literal> labels = dr.getLabels();
			if(labels!=null) for(Literal l:labels){
				doc.add(new Field(LABELS, l.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Literal> authlabels = dr.getAuthLabels();
			if(authlabels!=null) for(Literal l:authlabels){
				doc.add(new Field(AUTH_LABELS, l.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Literal> comments = dr.getComments();
			if(comments!=null) for(Literal l:comments){
				doc.add(new Field(COMMENTS, l.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Literal> authcomments = dr.getAuthComments();
			if(authcomments!=null) for(Literal l:authcomments){
				doc.add(new Field(AUTH_COMMENTS, l.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Literal> dates = dr.getDates();
			if(dates!=null) for(Literal l:dates){
				doc.add(new Field(DATES, l.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Literal> authdates = dr.getAuthDates();
			if(authdates!=null) for(Literal l:authdates){
				doc.add(new Field(AUTH_DATES, l.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Node> types = dr.getTypes();
			if(types!=null) for(Node t:types){
				doc.add(new Field(TYPES, t.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Node> authtypes = dr.getAuthTypes();
			if(authtypes!=null) for(Node t:authtypes){
				doc.add(new Field(AUTH_TYPES, t.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			if(dr.getLat()!=null && dr.getLong()!=null){
				doc.add(new Field(LAT, dr.getLat().toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
				doc.add(new Field(LONG, dr.getLong().toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Resource> imgs = dr.getImages();
			if(imgs!=null) for(Resource img:imgs){
				doc.add(new Field(IMG, img.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}
			
			Set<Resource> sameas = dr.getSameAs();
			if(sameas!=null) for(Resource sa:sameas){
				doc.add(new Field(SAMEAS, sa.toN3(), Field.Store.YES, Field.Index.NOT_ANALYZED));
			}

			// return the document
			return doc;
		}
	}
}
