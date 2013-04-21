package browser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
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
public class Browser {

    static final Logger logger = Logger.getLogger(Browser.class);

    public static void main(String[] args) throws Exception {
	if (args.length == 0) {
	    System.out
		    .println("This program consists of three functions\n"
			    + "1. convert raw tweets into cleaned representation\n"
			    + "2. query tok-K topics in given time window\n"
			    + "3. do topic clustering based on tweets in each unit time window\n"
			    + "Usage: -cmd convert|cluster|query\t[-conf confFilePath}] "
			    + "\t[-query {startTime duration}]\n");
	    System.exit(0);
	}

	String cmd = "";
	String confPath = "config/config.xml";
	int port=23451, backlog=1;

	for (int i = 0; i < args.length; i++) {
	    if ("-cmd".equals(args[i])) {
		cmd = args[i + 1];
		i++;
	    } else if ("-conf".equals(args[i])) {
		confPath = args[i + 1];
		i++;
	    } else if ("-query".equals(args[i])) {
		port = Integer.parseInt(args[i + 1]);
		backlog = Integer.parseInt(args[i + 2]);
		i += 2;
	    } else {
		System.out.println("Bad input param:" + args[i]);
		i++;
	    }
	}

	Configure conf = new Configure(confPath);
	conf.config();
	Parser parser = null;
	if (conf.getPlatform().equals("Twitter"))
	    parser = new TweetParser(conf);
	
	
	// cmd convert is to split big tweet data file into splits according to
	// time window the tweet resides.
	if (cmd.equals("convert")) {
	    Converter converter = new Converter(conf, parser);
	    converter.convert();
	}	
	else if (cmd.equals("cluster")) {
	    // 	cmd cluster, cluster documents to detect topics
	    // OPTICS clustering algorithm is applied
	    OPTICSCluster optics = new OPTICSCluster(conf, parser);
	    for (File f : conf.getDocsDir().listFiles()) {
		if (f.getName().endsWith(".txt")) {
		    String path=f.getPath().substring(0, f.getPath().length()-4);
		    Vector<Topic> tps = optics.detectTopics(path);
		    optics.saveTopics(tps, path);
		}
	    }
	}	
	else if (cmd.equals("query")) {
	    // cmd query start the web listener to receive user queries and return search results.
	    try {
		ServerSocket server = new ServerSocket(port, backlog);
		logger.info("Server socket wating for connection...");

		while (true) {
		    Socket conn = server.accept();
		    System.out.println("new connection started...");
		    new Worker(conn, conf, parser).start();
		}

	    } catch (IOException e) {
		System.out.println("connection error...");
	    }

	}
    }

    public static class Worker extends Thread {
	private Socket conn;
	private Configure conf;
	private Parser parser;

	public Worker(Socket conn, Configure conf, Parser parser) {
	    this.conn = conn;
	    this.conf = conf;
	    this.parser = parser;
	}

	private String parseArgs() throws IOException {
	    BufferedReader in = new BufferedReader(new InputStreamReader(
		    conn.getInputStream()));
	    String line = in.readLine();
	    return line;
	}

	public void run() {

	    try {
		long queryTime[] = null;// parseArgs();
		if (queryTime.length == 0) {
		    logger.warn("empty args...");
		    return;
		}

		String paths[] = new String[queryTime.length];
		for (int i = 0; i < queryTime.length; i++) {
		    paths[i] = conf.getDocsDir() + "/"
			    + Configure.getSlot(queryTime[i]) + ".txt";
		}
		OPTICSCluster optics = new OPTICSCluster(conf, parser);
		Vector<Topic> topics=optics.mergeTopics(paths);
		
		String ret=optics.encodeTopics(topics, paths);

		PrintWriter out = new PrintWriter(conn.getOutputStream());
		// TODO encode topics into string and output it
		out.println(ret);
		out.flush();
		conn.close();
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
    }

}
