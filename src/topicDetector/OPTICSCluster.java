package topicDetector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.xml.sax.SAXException;

import twitter.TweetParser;
import util.Configure;
import util.Document;
import util.DocumentRandomAccess;
import util.Parser;
import util.Document.Word;

/*
 * OPTICS is density based cluster algorithm.  Here we exploit to detect 
 * topics among tweets by clustering similar tweet together.
 * 
 */
public class OPTICSCluster implements Detector {
    static final Logger logger = Logger.getLogger(OPTICSCluster.class);

    // max epsilon for defining core object of OPTICS
    final float MaxEpsilon;

    // min points for defining core object
    final int MinPts;

    // epsilon for cluster determination
    final float Epsilon;

    // to initalize core distance and reachability distance
    final float UNDEF = 1f;

    // maintain top k most popular documents for each topic
    int TOPK = 10;

    Parser parser;

    public OPTICSCluster(Configure conf) {

	MaxEpsilon = conf.getMeps();
	Epsilon = conf.getEps();
	MinPts = conf.getMinPts();
    }

    public OPTICSCluster(Configure conf, Parser parser) throws IOException {
	MaxEpsilon = conf.getMeps();
	Epsilon = conf.getEps();
	MinPts = conf.getMinPts();

	this.parser = parser;
	this.parser.loadDictionary();
    }

    /*
     * load documents from file specified by path(without suffix) into vector
     * docs
     * 
     * @return the updated docs vector
     */
    public Vector<Document> loadDocuments(String path, Vector<Document> docs)
	    throws FileNotFoundException {
	logger.info("loading file " + path + ".txt");

	int k = 0;
	String line;
	long offset = 0, nextOffset = 0;

	// record offset of each document within the doc file
	DocumentRandomAccess docAccess = new DocumentRandomAccess();
	BufferedReader in = new BufferedReader(new FileReader(path + ".txt"));

	try {
	    while ((line = in.readLine()) != null) {// &&k<50000) {
		nextOffset += line.length() + 1;
		Document doc = parser.parseDocument(line);
		if (doc != null && doc.getWordVector().size() > 2) {
		    k++;
		    // record the offset of current document in the file
		    docAccess.addOffset(doc.getId(), offset);
		    docs.add(doc);
		}
		offset = nextOffset;

	    }

	    in.close();
	    docAccess.saveOffset(path);

	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return docs;
    }

    /*
     * load topics from file specified by path(without suffix) into vector
     * topics
     * 
     * @return updated topics vector
     */
    public Vector<Topic> loadTopics(String path, Vector<Topic> topics)
	    throws FileNotFoundException {

	String line;

	// topics are stored in file with suffix .topic
	BufferedReader in = new BufferedReader(new FileReader(path + ".topic"));
	long k = topics.size();
	try {
	    while ((line = in.readLine()) != null) {

		Topic topic = Topic.parse(line);

		// assign a id for each topic in increasing order
		topic.id = k++;
		if (topic != null)
		    topics.add(topic);
	    }

	    in.close();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}

	return topics;
    }

    /*
     * create index for a set of documents to accelerate topic detection. e.g.,
     * inverted index can help to cluster documents sharing one gram/word. other
     * clustering algorithm can then be conducted on these rough clusters.
     * 
     * @param docs a set of documents
     * 
     * @return a vector of inverted list
     */
    public Vector<InvertedList> createDocIndex(Vector<Document> docs)
	    throws IOException {

	HashMap<Integer, InvertedList> invertedIndex = new HashMap<Integer, InvertedList>(
		parser.dict.dict.size() / 100);

	for (Document doc : docs) {
	    for (Word word : doc.getWordVector()) {
		InvertedList list = invertedIndex.get(word.getId());

		if (list == null) {
		    // new a list if it does not exist
		    list = new InvertedList(word.getId());
		    invertedIndex.put(word.getId(), list);
		}
		list.addDoc(doc);
	    }
	}

	Vector<InvertedList> invertedLists = new Vector<InvertedList>(
		invertedIndex.values());

	// TODO: sort invertedlists in some order that would accelerate the
	// later clustering

	return invertedLists;
    }

    /*
     * similar as {@ createDocIndex}, but for topics
     */
    public Vector<InvertedList> createTopicIndex(Vector<Topic> docs) {

	HashMap<Integer, InvertedList> invertedIndex = new HashMap<Integer, InvertedList>();

	for (Document doc : docs) {
	    for (Word word : doc.getWordVector()) {
		InvertedList list = invertedIndex.get(word.getId());
		if (list == null) {
		    list = new InvertedList(word.getId());
		    invertedIndex.put(word.getId(), list);
		}
		list.addDoc(doc);
	    }
	}

	Vector<InvertedList> invertedLists = new Vector<InvertedList>(
		invertedIndex.values());

	return invertedLists;
    }

    /*
     * merge topics of selected time windows
     * 
     * @param paths each path contains the topics for that time window
     * 
     * @return merged topics
     */
    public Vector<Topic> mergeTopics(Vector<String> paths) throws IOException {

	Vector<Topic> docs = new Vector<Topic>(100 * paths.size());
	for (String path : paths)
	    docs = loadTopics(path, docs);

	if (paths.size() == 1)
	    return docs;

	// inverted list
	logger.info("creat inverted index...");
	Vector<InvertedList> invertedLists = createTopicIndex(docs);
	logger.info("finished creat inverted index.");
	logger.info(docs.size() + " topics have been indexed.");

	logger.info("cluser topics...");

	String tmpPath = "tmp/" + System.currentTimeMillis();
	BufferedWriter out = new BufferedWriter(new FileWriter(tmpPath
		+ ".orderedPoint"));
	for (InvertedList list : invertedLists)
	    doOPTICSCluster(list.docs, out);
	out.close();

	logger.info("Ordered points have been generated.");

	// determine clusters according to epsilon from ordered points
	Vector<Topic> topics = determineTopics(Epsilon, tmpPath);
	logger.info(topics.size() + " topics have been detected.");

	return topics;
    }

    /*
     * cluster documents from one time window(i.e., one file)
     * 
     * @path path to the file for document in one time window
     * 
     * @return a vector of detected topics
     */
    public Vector<Topic> detectTopics(String path) throws IOException {

	logger.info("creat inverted index...");

	Vector<Document> docs = new Vector<Document>(100000);
	docs = loadDocuments(path, docs);

	// inverted list for dictionary word
	Vector<InvertedList> invertedLists = createDocIndex(docs);
	logger.info(invertedLists.size() + " inverted index have been created.");
	logger.info(docs.size() + " documents have been indexed.");

	logger.info("cluser documents...");
	BufferedWriter out = new BufferedWriter(new FileWriter(path
		+ ".orderedPoint"));

	// cluster documents from the same inverted list
	for (InvertedList pl : invertedLists)
	    doOPTICSCluster(pl.docs, out);
	out.close();

	logger.info("Ordered points have been generated.");

	// determine clusters according to epsilon from ordered points
	Vector<Topic> topics = determineTopics(Epsilon, path);

	logger.info(topics.size() + " topics have been detected.");

	return topics;
    }

    /*
     * apply OPTICS clustering algorithm, save ordered points to tmp file
     * 
     * @param docs documents to be clustered
     * 
     * @param orderedPointWriter writer for saving the ordered points
     */
    void doOPTICSCluster(Vector<Document> docs,
			 BufferedWriter orderedPointWriter) throws IOException {

	// initial a point for each docuemnt
	int k = 0;
	Vector<Point> points = new Vector<Point>(docs.size());
	for (Document doc : docs) {
	    Point p = new Point(doc, k++);
	    points.add(p);
	}

	Vector<Point> neighbor = new Vector<Point>();
	Vector<Float> neighborDist = new Vector<Float>();
	for (Point p : points) {

	    if (!hasProcessed(p)) {

		int size = getEpsNeighbor(p, points, neighbor, neighborDist);

		// noisy points won't be saved
		if (p.coreDist != UNDEF || p.reachDist != UNDEF)
		    p.save(orderedPointWriter);

		setProcessed(p);

		if (size >= MinPts && !neighbor.isEmpty()) {
		    MinHeap orderedSeed = new MinHeap(neighbor.size());
		    updateOrderedSeed(p, neighbor, neighborDist, orderedSeed);

		    while (!orderedSeed.isEmpty()) {
			Point q = orderedSeed.getMinPoint();

			size = getEpsNeighbor(q, points, neighbor, neighborDist);
			if (q.coreDist != UNDEF || q.reachDist != UNDEF)
			    q.save(orderedPointWriter);
			setProcessed(q);

			if (size >= MinPts && !neighbor.isEmpty()) {
			    updateOrderedSeed(q, neighbor, neighborDist,
				    orderedSeed);
			}
		    }
		}
	    }
	}

	orderedPointWriter.flush();
    }

    /*
     * get the epsilon neighborhood and set the core distance if it exist;
     * 
     * @param p current point
     * 
     * @param vec the set of all points
     * 
     * @param neighbor all epsilon neighbors, to be inserted
     * 
     * @param neighborDist distance of epsilon neighbor
     * 
     * @return the size of the epsilon neighbor
     */
    public int getEpsNeighbor(Point p, Vector<Point> set,
			      Vector<Point> neighbor, Vector<Float> neighborDist) {
	int size = p.getSize();
	float dist;

	// to sort distances, and get the core distance
	Vector<DistSizePair> distSize = new Vector<DistSizePair>();

	neighbor.clear();
	neighborDist.clear();

	for (Point q : set) {
	    if (hasProcessed(q) || p == q)
		continue;
	    dist = p.distanceTo(q);
	    if (dist < MaxEpsilon) {
		neighbor.add(q);
		neighborDist.add(dist);
		size += q.getSize();

		distSize.add(new DistSizePair(q.getSize(), dist));
	    }
	}

	if (size < MinPts)
	    return size;

	Collections.sort(distSize);

	// the cluster size is sum of supports of its documents
	int k = p.getSize();
	int i = 0;
	while (k < MinPts)
	    k += distSize.get(i++).size;

	if (i > 0)
	    p.coreDist = distSize.get(i - 1).dist;
	else
	    p.coreDist = 0f;

	return size;
    }

    /*
     * update the ordered seed set by updating reachability distance for each
     * neighbor sort neighbors according to reachability distance in ascending
     * order
     * 
     * @param curr the current core object point
     * 
     * @param neighbor neighbor points of the curr point
     * 
     * @param neighborDist distance to each neighbor point
     * 
     * @param orderedSeed to be updated
     */
    void updateOrderedSeed(Point curr, Vector<Point> neighbor,
			   Vector<Float> neighborDist, MinHeap orderedSeed) {
	float newReachDist;
	float coreDist = curr.coreDist;
	for (int i = 0; i < neighbor.size(); i++) {
	    Point p = neighbor.get(i);
	    float dist = neighborDist.get(i);
	    newReachDist = Math.max(coreDist, dist);
	    if (p.reachDist == UNDEF) {
		p.reachDist = newReachDist;
		orderedSeed.addPoint(p);
	    } else if (newReachDist < p.reachDist) {
		p.reachDist = newReachDist;
	    }
	}

    }

    boolean hasProcessed(Point p) {
	return p.doc.getSupport() == 0;
    }

    void setProcessed(Point p) {
	p.doc.clearSupport();
    }

    /*
     * read ordered points from tmp file, assign it to a cluster based on
     * epsilon threshold
     * 
     * @param epsilon threshold for OPTICS clustering
     * 
     * @return topics sorted by its popularity in descending order
     */
    public Vector<Topic> determineTopics(float epsilon, String path)
	    throws IOException {

	BufferedReader in = new BufferedReader(new FileReader(path
		+ ".orderedPoint"));

	String line;
	Topic tp = null;
	Vector<Topic> topics = new Vector<Topic>();

	while ((line = in.readLine()) != null) {
	    Point p = new Point(line);
	    if (p.reachDist > epsilon) {
		if (p.coreDist <= epsilon) {
		    tp = new Topic();
		    tp.addDoc(p.doc);
		    topics.add(tp);
		}
	    } else {
		tp.addDoc(p.doc);
	    }
	}

	// sort topics according to support
	Collections.sort(topics);

	in.close();
	return topics;
    }

    /*
     * save topics into disk
     * 
     * @param path tweet file path(not path for storing topics, which is
     * path+".topics")
     */
    public void saveTopics(Vector<Topic> topics, String path)
	    throws IOException {

	BufferedWriter out = new BufferedWriter(new FileWriter(path + ".topic"));
	for (Topic tp : topics) {
	    // generate popular words of the topic
	    tp.aggregateDocs();
	    out.write(tp.toString());
	    out.newLine();
	}
	out.flush();
	out.close();

    }

    /*
     * encode detected topics into html, raw text of the top k documents for
     * each topic are attached
     * 
     * @param topics detected topics
     * 
     * @path original tweet file path
     * 
     * @return a json string for all topics
     */

    public String generateHtmlTopics(Vector<Topic> topics, Vector<String> paths)
	    throws NumberFormatException, IOException {

	HashMap<Integer, DocumentRandomAccess> docAccess = new HashMap<Integer, DocumentRandomAccess>(
		paths.size() * 2);

	// init random accessors
	for (String path : paths) {
	    DocumentRandomAccess docAcs = new DocumentRandomAccess(path);
	    docAcs.startRead(path);

	    File f = new File(path);
	    int slot = Integer.parseInt(f.getName());
	    docAccess.put(slot, docAcs);
	}
	

	String html = "";
	for (int i = 0; i < Math.min(topics.size(), TOPK); i++) {
	    Topic tp = topics.get(i);

	    // generate popular words of the topic
	    if (tp.getDocuments() != null)
		tp.aggregateDocs();

	    html += "<div class='topic' id='" + i + "'>";
	    html += "<span class='support' >Support: " + tp.getSupport()
		    + "</span><br/>";
	    html += "<a href='#' onclick='show(" + i + ")'>show all</a>";
	    html += "<div id='" + i + "-first'>";
	    
	    int end = tp.getPopularDocs().size() > 3 ? 3 : tp.getPopularDocs()
		    .size();
	    for (int k = 0; k < end; k++) {
		Document doc = tp.getPopularDocs().poll();
		int slot = doc.getSlot();
		String raw = docAccess.get(slot).readDocument(doc.getId());

		raw = docAccess.get(slot).readDocument(doc.getId());
		html += html4topic(raw);
		// $html.='<li>'.$jsondoc.'</li>';
	    }
	    html += "</div><div class ='second' id='" + i + "-second'>";

	    for (int k = end; k < tp.getPopularDocs().size(); k++) {
		Document doc = tp.getPopularDocs().poll();
		int slot = doc.getSlot();
		String raw = docAccess.get(slot).readDocument(doc.getId());

		raw = docAccess.get(slot).readDocument(doc.getId());
		html += html4topic(raw);
	    }

	    html += "</div></div>";

	}

	for (DocumentRandomAccess docAcs : docAccess.values())
	    docAcs.endRead();
	return html;
    }

    String html4topic(String raw) {	
	JSONObject obj = (JSONObject) JSONValue.parse(raw);
	long id = (Long) obj.get("id");
	String text = (String) obj.get("text");
	long time = (Long) obj.get("time");
	java.util.Date date = new java.util.Date();
	date.setTime(time);

	return "<li id='" + id + "'>" + text + " [" + date.toString()
		+ "]</li>";
    }

    /*
     * encode the topics into json string
     */
    @SuppressWarnings("unchecked")
    public String generateJsonTopics(Vector<Topic> topics, Vector<String> paths)
	    throws NumberFormatException, IOException {

	HashMap<Integer, DocumentRandomAccess> docAccess = new HashMap<Integer, DocumentRandomAccess>(
		paths.size() * 2);

	// init random accessors
	for (String path : paths) {
	    DocumentRandomAccess docAcs = new DocumentRandomAccess(path);
	    docAcs.startRead(path);

	    File f = new File(path);
	    int slot = Integer.parseInt(f.getName());
	    docAccess.put(slot, docAcs);
	}

	
	JSONArray jsonary = new JSONArray();

	for (int i = 0; i < Math.min(topics.size(), TOPK); i++) {
	    Topic tp = topics.get(i);

	    // generate popular words of the topic
	    if (tp.getDocuments() != null)
		tp.aggregateDocs();

	    JSONObject obj = new JSONObject();
	    obj.put("pop", tp.getSupport());

	    // System.out.println("popularity: "+tp.getSupport());

	    JSONArray popDocs = new JSONArray();
	    for (Document doc : tp.getPopularDocs()) {
		String raw = docAccess.get(tp.getSlot()).readDocument(
			doc.getId());
		popDocs.add(raw);

		// System.out.println(raw);
	    }

	    obj.put("docs", popDocs);
	    jsonary.add(obj);
	}
	for (DocumentRandomAccess docAcs : docAccess.values())
	    docAcs.endRead();
	return jsonary.toJSONString();
    }

    /*
     * maintain the ordredSeed in increasing order based on reachability
     * distance
     */
    class MinHeap {
	Vector<Point> points;

	// keep the position of previous min point, later inserted point can be
	// placed here
	int vacantPos;

	public MinHeap(int size) {
	    points = new Vector<Point>(size);
	    vacantPos = -1;
	}

	public void addPoint(Point point) {
	    if (vacantPos >= 0) {
		points.set(vacantPos, point);
		vacantPos = -1;
	    } else
		points.add(point);
	}

	public Point getMinPoint() {

	    // remove previous min point by place the last element at its
	    // position
	    if (vacantPos >= 0) {
		points.set(vacantPos, points.lastElement());
		points.remove(points.lastElement());
	    }

	    float minReachDist = points.get(0).reachDist;
	    vacantPos = 0;
	    for (int i = 1; i < points.size(); i++) {
		if (minReachDist > points.get(i).reachDist) {
		    minReachDist = points.get(i).reachDist;
		    vacantPos = i;
		}
	    }

	    return points.get(vacantPos);
	}

	public boolean isEmpty() {
	    // must check both two conditions
	    return points.isEmpty() || (points.size() == 1 && vacantPos >= 0);
	}
    }

    /*
     * for topic support based comparison. class TopicComp implements
     * Comparator<Topic> { public int compare(Topic t1, Topic t2) { return
     * t2.getSupport() - t1.getSupport(); } }
     */

    /*
     * for sort points based on distance
     */
    class DistSizePair implements Comparable<DistSizePair> {
	int size;
	float dist;

	public DistSizePair(int s, float d) {
	    size = s;
	    dist = d;
	}

	public int compareTo(DistSizePair other) {
	    if (this.dist < other.dist)
		return -1;
	    else
		return 1;
	}
    }

    class Point implements Comparable<Point> {
	int id;
	float coreDist, reachDist;
	Document doc;

	public Point(Document doc, int id) {
	    this.id = id;
	    this.doc = doc;
	    reachDist = UNDEF;
	    coreDist = UNDEF;
	}

	public Point(String str) {
	    int idx1 = str.indexOf(",");
	    coreDist = Float.parseFloat(str.substring(0, idx1));

	    int idx2 = str.indexOf(",", idx1 + 1);
	    reachDist = Float.parseFloat(str.substring(idx1 + 1, idx2));

	    String rest = str.substring(idx2 + 1);
	    if (rest.charAt(0) == '{') {
		doc = new Document();
		doc.decode(rest);
	    } else {
		doc = Topic.parse(rest);
	    }

	}

	float distanceTo(Point other) {
	    return 1f - doc.sim(other.doc);
	}

	int getSize() {
	    return doc.getSupport();
	}

	public int compareTo(Point other) {
	    if (this.reachDist > other.reachDist)
		return -1;
	    else
		return 1;
	}

	void save(BufferedWriter out) throws IOException {
	    String ret = coreDist + "," + reachDist + "," + doc.toString();
	    out.write(ret);
	    out.newLine();
	}
    }

    /*
     * append each document to a set of list, them we can reduce the cluster
     * boundary. i.e., only apply clustering algorithm on each list
     */
    class InvertedList {// implements Comparable<InvertedList> {
	int id;// id of the gram, from the dictionary
	       // float idf;
	int size;

	Vector<Document> docs;

	public InvertedList(int id) {
	    this.id = id;
	    this.size = 0;
	    this.docs = new Vector<Document>();
	}

	public void addDoc(Document doc) {
	    docs.add(doc);
	    size += doc.getSupport();
	}

	/*
	 * public float getTfIdf() { return size;// * idf; }
	 */

	public int compareTo(InvertedList other) {
	    if (this.size < other.size) {
		return 1;
	    } else
		return -1;
	}

    }

    // for test
    public static void main(String[] args) throws IOException,
	    ParserConfigurationException, SAXException {
	Configure conf = new Configure("config/config.xml");
	Parser parser = new TweetParser(conf);

	Vector<String> paths = new Vector<String>();

	OPTICSCluster optics = new OPTICSCluster(conf, parser);
	Vector<Topic> topics = optics.detectTopics("data/oneday/3");
	paths.add("data/oneday/3");
	// optics.encodeTopics(topics, paths);
	optics.saveTopics(topics, "data/oneday/3");
    }
}
