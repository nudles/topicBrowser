package twitter;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import gnu.trove.TLongObjectHashMap;

import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.util.InvalidFormatException;

import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.xml.sax.SAXException;

import util.Configure;

import util.Document;
import util.Parser;
import util.Stemmer;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;

/*
 * Parser for tweet document, inherit from {@link Parser}
 */

public class TweetParser extends Parser {
    Stemmer stemmer = null;
    Tokenizer tokenizer = null;

    Pattern stopWordPattern = null;
    Pattern englishChecker = null;
    
    private TLongObjectHashMap<Tweet> tSet;

    public TweetParser(Configure conf) throws IOException {
	
	super(conf);
	
	tSet = new TLongObjectHashMap<Tweet>();
	
	stemmer = new Stemmer();
	initTokenizer(conf.getTokenModelFile());
	initStopWordPattern(conf.getStopWordFile());
	initEnglishChecker();
    }

    
    public void clearTweetSet() {
	tSet.clear();
    }


    void initTokenizer(File file) throws InvalidFormatException, IOException {
	InputStream modelIn = new FileInputStream(file);
	TokenizerModel model = new TokenizerModel(modelIn);
	tokenizer = new TokenizerME(model);
    }

    
    void initStopWordPattern(File file) throws IOException {
	BufferedReader reader = new BufferedReader(new FileReader(file));
	String line;
	String stopWord = "";
	while ((line = reader.readLine()) != null) {
	    stopWord += line.trim() + "|";
	}

	stopWordPattern = Pattern.compile(stopWord.substring(0,
		stopWord.length() - 1));
    }

    /*
     *English check is based on characters 
     */
    void initEnglishChecker() {
	englishChecker = Pattern.compile("[\\x20-\\x7f]+");
    }

    
    /*
     * check whether this word is a stop word or not
     * 
     *  @param word input word
     *  @return True if word is a stop word, false otherwise
     */
    public boolean isStopWord(String word) {
	if (stopWordPattern != null)
	    return stopWordPattern.matcher(word).matches();
	return false;
    }

    
    /*
     * check whether a word is English or not, by matching 
     * its characters upon a regular expression [\\x20-\\x7f]+
     * 
     * @param text content of the tweet
     * @return  True if english, otherwise fasle;
     */
    public boolean isEnglish(String text) {
	return englishChecker.matcher(text).matches();
    }

    
    /*
     * (non-Javadoc)
     * @see util.Parser#stemWord(java.lang.String)
     */
    public String stemWord(String word) {
	stemmer.add(word.toCharArray(), word.length());
	stemmer.stem();
	return stemmer.toString();
    }

    /*
     * (non-Javadoc)
     * @see util.Parser#tokenize(java.lang.String)
     */
    public String[] tokenize(String text) {
	return tokenizer.tokenize(text);
    }
    
    /*
     * (non-Javadoc)
     * @see util.Parser#parseDocument(java.lang.String)
     * 
     */
    public Document parseRawDocument(String line) {
	Tweet tweet;
	Long id = 0l, retid = 0l;
	String text = "", date = "";
	try {
	    JSONObject obj = (JSONObject) JSONValue.parse(line);
	    id = (Long) obj.get("id");
	    if (id == null) {
		return null;
	    }

	    retid = (Long) obj.get("retid");
	    if (retid == null)
		retid = 0l;

	    text = (String) obj.get("text");
	    if (!isEnglish(text))
		return null;
	    JSONObject timeobj = (JSONObject) obj.get("crtdt");
	    date = (String) timeobj.get("$date");
	} catch (Exception e) {
	    System.out.println(line);
	    return null;
	}

	SimpleDateFormat dateFormat = new SimpleDateFormat(
		"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	Date cdate;
	try {
	    cdate = dateFormat.parse(date);
	    long ctime = cdate.getTime();
	    tweet = new Tweet(id, retid, 1, ctime, text,0);
	} catch (ParseException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    Logger.getLogger(TweetParser.class).warn(
		    "can't parse the time for tweet:" + line);
	    return null;
	}
	
	HashMap<String, Integer> words = new HashMap<String, Integer>();
	parseText(text,words);
	
	
	for (Map.Entry<String, Integer> entry : words.entrySet())
	    dict.add(entry.getKey());
	
	return tweet;
    }

    
    /*
     * (non-Javadoc)
     * @see util.Parser#parseDocument(java.lang.String)
     */
    public Document parseDocument(String doc){	
	Long id = 0l, retid = 0l, time;
	String text = "";
	
	try {
	    JSONObject obj = (JSONObject) JSONValue.parse(doc);
	    id = (Long) obj.get("id");
	    if (id == null) {
		return null;
	    }

	    //for a retweet, if it is in current window, then inc its support
	    //otherwise regard this tweet as the original tweet
	    retid = (Long) obj.get("retid");
	    if(retid!=null){
		Tweet origTweet=tSet.get(retid);
		if(origTweet!=null){
		    origTweet.incSupport();
		    return null;
		}
		else id=retid;
	    }

	    time=(Long) obj.get("time");
	    if(time==null)
		return null;
	    
	    text = (String) obj.get("text");
	    if (!isEnglish(text))
		return null;
	} catch (Exception e) {
	    System.out.println(doc);
	    return null;
	}
	
	
	HashMap<String, Integer> words = new HashMap<String, Integer>();
	int len= parseText(text,words);
	Tweet tweet=new Tweet(id,retid,1,time,null,len);
	
	for (Map.Entry<String, Integer> entry : words.entrySet()) {
	    int wId=dict.retrieve(entry.getKey());
	    if(wId>0)
		tweet.append(wId, entry.getValue());
	}
	
	return tweet;
    }
    
    
    /*
     * parse a text string into a set of words, whose occurrence number is associated
     * 
     * @param text input text string to be parsed
     * @param words a HashMap which associates each token with a occurrence number
     * @return  number of tokens within the text
     */
     int parseText(String text,HashMap<String, Integer> words) {
	
	String[] tokens = tokenize(text);
	

	for (int i = 0; i < tokens.length; i++) {
	    String token = stemWord(tokens[i]).toLowerCase();//tokens[i].toLowerCase();// 
	    if (token.length()==1||isStopWord(token) || token.startsWith("@")
		    || token.startsWith("://t")||token.matches("(\\p{Punct}+[a-zA-Z]?)|([a-zA-Z]?\\p{Punct}+)"))
		continue;

	    Integer occrNum = words.get(token);
	    if (occrNum == null)
		occrNum = 1;
	    else
		occrNum++;
	    words.put(token, occrNum);
	}

	return tokens.length;
    }

    /*
     * save the dictionary which is sorted by entity frequency
     * 
     */
    public void saveDictionary() {
	try {
	    dict.saveDictionarySortedByOccr();
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	    System.out.println("error in saving dictionary to disk");
	}
    }
    
    public void loadDictionary() throws IOException{
	dict.loadDictionary();
    }
    
    //test TweetParser class
    public static void main(String []args) throws ParserConfigurationException, SAXException, IOException{
	String text="You can imagine Princes William and Harry drawing moustaches on each other's royal portaits, can't you? #lovelyblokes";
	Configure conf = new Configure("config/config.xml");
	conf.config();
	
	TweetParser parser=new TweetParser(conf);
	
	//HashMap<String, Integer> words = parser.parseText(text);
	System.out.println(parser.stemWord("decidedly"));
    }

}
