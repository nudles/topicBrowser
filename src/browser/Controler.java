package browser;


import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

import org.apache.log4j.Logger;

import topicDetector.OPTICSCluster;
import topicDetector.Topic;
import twitter.TweetParser;
import util.Configure;
import util.Converter;
import util.Parser;

/*
 * This is the controller of the project.
 * It can pre-process data, cluster topics, serve user querying
 */
public class Controler {

    static final Logger logger = Logger.getLogger(Controler.class);

    public static void main(String[] args) throws Exception {
	if (args.length == 0) {
	    System.out
		    .println("This program consists of three functions\n"
			    + "1. convert raw tweets into cleaned representation\n"
			    + "2. query tok-K topics in given time window\n"
			    + "3. do topic clustering based on tweets in each unit time window\n"
			    + "4. recluster topics by provding a new epsilon\n"
			    + "Usage: -cmd convert|cluster|query|recluster \t[-conf confFilePath}] "
			    + "\t[-date date(e.g.,2012-08-01)]\t[-epsilon epsilon]\n");
	    System.exit(0);
	}

	float epsilon=0f;
	String cmd = "";
	String confPath = "config/config.xml";
	Vector<String> queryDates=new Vector<String>();
	
	
	for (int i = 0; i < args.length; i++) {
	    if ("-cmd".equals(args[i])) {
		cmd = args[i + 1];
		i++;
	    } else if ("-conf".equals(args[i])) {
		confPath = args[i + 1];
		i++;
	    } else if("-date".equals(args[i])){
		queryDates.add(args[i+1]);
		i++;
	    } else if("-epsilon".equals(args[i])){
		epsilon=Float.parseFloat(args[i+1]);
		i++;
	    }
	    else{
		System.out.println("Bad input param:" + args[i]);
		i++;
	    }
	}

	Configure conf = new Configure(confPath);	
	Parser parser = null;
	if (conf.getPlatform().equals("Twitter"))
	    parser = new TweetParser(conf);

	// cmd convert is to split big tweet data file into splits according to
	// time window the tweet resides.
	if (cmd.equals("convert")) {
	    Converter converter = new Converter(conf, parser);
	    converter.convert();
	    
	    
	} else if (cmd.equals("cluster")) {
	    // cmd cluster, cluster documents to detect topics
	    // OPTICS clustering algorithm is applied
	    OPTICSCluster optics = new OPTICSCluster(conf, parser);
	    for (File f : conf.getDocsDir().listFiles()) {
		if (f.getName().endsWith(".txt")) {
		    String path = f.getPath().substring(0,
			    f.getPath().length() - 4);
		    Vector<Topic> tps = optics.detectTopics(path);
		    optics.saveTopics(tps, path);
		}
	    }
	    
	    
	} else if (cmd.equals("query")) {
	    // cmd query start the web listener to receive user queries and
	    // return search results.
	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	    Vector<String> paths=new Vector<String>();	    
	    String dir = conf.getDocsDir().getPath();	   
	    for (String dateStr : queryDates) {
		Date date = dateFormat.parse(dateStr);
		long time = date.getTime() + 1000;
		int slot = Configure.getSlot(time);
		File f = new File(dir + "/" + slot + ".txt");
		if (f.exists())
		    paths.add(dir + "/" + slot);
		slot++;

		f = new File(dir + "/" + slot + ".txt");
		if (f.exists())
		    paths.add(dir + "/" + slot);
	    }
	    
	    OPTICSCluster optics = new OPTICSCluster(conf, parser);
	    optics.mergeTopics(paths);	    
	    
	    
	    
	} else if(cmd.equals("recluster")){
	    OPTICSCluster optics = new OPTICSCluster(conf, parser);
	    for (File f : conf.getDocsDir().listFiles()) {
		if (f.getName().endsWith(".txt")) {
		    String path = f.getPath().substring(0,
			    f.getPath().length() - 4);
		    Vector<Topic> tps = optics.determineTopics(epsilon, path);
		    optics.saveTopics(tps, path);
		}
	    }
	}
    }

   
}
