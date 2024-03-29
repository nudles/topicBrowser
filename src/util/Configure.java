package util;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Configure {
    private enum ParamName {
	platform, stopWord, dictionary, wordOccrT, tokenModel, startT, timeW, rawDocsD, docsD, meps, eps, minPts, logConf, port, topk,docN, unknown
    };

    public static long startTime = 0;
    public static long timeWindow = 0;

    private int wordOccrThreshold = 1;

    private String platformName = "Twitter";

    private File stopWordFile = null;
    private File dictionaryFile = null;
    private File dictionaryStat = null;
    private File tokenModelFile = null;
    private File rawDocsDir = null;
    private File docsDir = null;
    

    private float totalDocN=10000000f;
    private float mepsValue = 1f;
    private float epsValue = 1f;
    private int minPtsValue = 100;
    private int portid=19899;
    private int topK=10;

    String configFile = "data/config.xml";

    public Configure(String file) throws ParserConfigurationException, SAXException, IOException {
	configFile = file;
	config();

    }

    public Configure() throws ParserConfigurationException, SAXException, IOException {
	config();
    }

    public void config() throws ParserConfigurationException, SAXException,
	    IOException {
	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	DocumentBuilder db = dbf.newDocumentBuilder();
	Document doc = db.parse(configFile);
	doc.getDocumentElement().normalize();

	parseXML(doc.getDocumentElement());

	if (startTime == 0 || timeWindow == 0) {
	    Logger.getLogger(Configure.class).error(
		    "no startTime or timeWindow is available\n");
	    System.exit(0);
	}
    }

    public static int getSlot(long time){	    
	return (int) ((time - Configure.startTime) / Configure.timeWindow);	    
    }

    public int getPort(){
	return portid;
    }
    
    public int getTopK(){
	return topK;
    }
    
    public String getPlatform() {
	return platformName;
    }

    public File getStopWordFile() {
	return stopWordFile;
    }

    public File getDictionaryFile() {
	return dictionaryFile;
    }

    public File getDictionaryStatFile() {
	return dictionaryStat;
    }

    public int getWordOccrThreshold() {
	return wordOccrThreshold;
    }

    public File getTokenModelFile() {
	return tokenModelFile;
    }

    public File getDocsDir() {
	return docsDir;
    }

    public File getRawDocsDir() {
	return rawDocsDir;
    }

    /*
     * @return the max epsilon value for OPTICS clustering
     */
    public float getEps() {
	return epsValue;
    }

    /*
     * @return the epsilon value for OPTICS clustering
     */
    public float getMeps() {
	return mepsValue;
    }

    public float getTotalDocN(){
	return totalDocN;
    }
    
    /*
     * @return the minimum points of a cluster for OPTICS
     */
    public int getMinPts() {
	return minPtsValue;
    }

    private ParamName resolveParamName(String name) {
	try {
	    return ParamName.valueOf(name.trim());
	} catch (Exception e) {
	    return ParamName.unknown;
	}
    }

    private String getParamValue(Element xmlParam) {

	Node nodeContent = xmlParam.getChildNodes().item(0);

	if (nodeContent == null)
	    return null;

	if (nodeContent.getNodeType() != Node.TEXT_NODE)
	    return null;

	String content = nodeContent.getTextContent().trim();

	if (content.length() == 0)
	    return null;

	return content;
    }

    private void parseXML(Element xml) {
	NodeList children = xml.getChildNodes();

	for (int i = 0; i < children.getLength(); i++) {

	    Node xmlChild = children.item(i);

	    if (xmlChild.getNodeType() == Node.ELEMENT_NODE) {

		Element xmlParam = (Element) xmlChild;

		String paramName = xmlParam.getNodeName();
		String paramValue = getParamValue(xmlParam);

		if (paramValue == null)
		    continue;

		switch (resolveParamName(xmlParam.getNodeName())) {

		case platform:
		    this.platformName = paramValue;
		    break;
		case stopWord:
		    this.stopWordFile = new File(paramValue);
		    break;
		case dictionary:
		    this.dictionaryFile = new File(paramValue);
		    break;
		case wordOccrT:
		    this.wordOccrThreshold = Integer.valueOf(paramValue);
		    break;
		case tokenModel:
		    this.tokenModelFile = new File(paramValue);
		    break;
		case startT:
		    try {
			SimpleDateFormat dateFormat = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			Date date = dateFormat.parse(paramValue);
			Configure.startTime = date.getTime();
		    } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		    }
		    break;
		case timeW:
		    Configure.timeWindow = 3600 * Integer.parseInt(paramValue) * 1000;
		    break;
		case rawDocsD:
		    rawDocsDir = new File(paramValue);
		    break;
		case docsD:
		    docsDir = new File(paramValue);
		    break;		
		case eps:
		    epsValue = Float.parseFloat(paramValue);
		    break;
		case meps:
		    mepsValue = Float.parseFloat(paramValue);
		    break;
		case minPts:
		    minPtsValue = Integer.parseInt(paramValue);
		    break;
		case logConf:
		    File logProperty = new File(paramValue);
		    if (logProperty.exists())
			PropertyConfigurator.configure(paramValue);
		    else BasicConfigurator.configure();
		    break;
		case port:
		    portid=Integer.parseInt(paramValue);
		    break;
		case topk:
		    topK=Integer.parseInt(paramValue);
		    break;
		case docN:
		    totalDocN=Float.parseFloat(paramValue);
		    break;

		default:
		    Logger.getLogger(Configure.class).warn(
			    "ignoring unkown parameter" + paramName);
		}
	    }
	}
    }
}
