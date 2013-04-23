package browser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.xml.sax.SAXException;

import topicDetector.OPTICSCluster;
import topicDetector.Topic;
import util.Configure;

/*
 * serves for request from php script initiated by users
 * 
 */
public class WebServer {
    static final Logger logger = Logger.getLogger(Controler.class);

    Configure conf;

    public WebServer(Configure conf) {
	this.conf = conf;
    }

    /*
     * start a socket server, which would keep listen request from php script.
     */
    public void start() throws ParserConfigurationException, SAXException,
	    ParseException {

	try {

	    ServerSocket server = new ServerSocket(conf.getPort());
	    logger.info("Server socket wating for connection...");

	    while (true) {
		Socket conn = server.accept();
		logger.info("new connection started...");
		new Worker(conn, conf).start();
	    }

	} catch (IOException e) {
	    logger.warn("connection error...");
	}
    }

    
    /*
     * start a new thread for each request.
     * it parses query dates, merge topics of these dates
     * encode merged topics as string, print it to the socket receiver.
     */
    public static class Worker extends Thread {
	private Socket conn;
	private Configure conf;

	public Worker(Socket conn, Configure conf) {
	    this.conn = conn;
	    this.conf = conf;
	}

	/*
	 * parse query dates from the request stream dates are in the format:
	 * yyyy-MM-dd, e.g.,2012-08-01
	 * 
	 * @return a vector strings represent the paths(without suffix) of files
	 * corresponding to query dates
	 */
	private Vector<String> parseArgs() throws IOException, ParseException {
	    BufferedReader in = new BufferedReader(new InputStreamReader(
		    conn.getInputStream()));

	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

	    String line = in.readLine();
	    String[] queryDates = line.split(",");

	    String dir = conf.getDocsDir().getPath();

	    Vector<String> paths = new Vector<String>();
	    for (String dateStr : queryDates) {
		Date date = dateFormat.parse(dateStr);
		long time = date.getTime() + 1000;
		int slot = Configure.getSlot(time);
		File f = new File(dir + "/" + slot + ".txt");
		if (f.exists())
		    paths.add(dir + "/" + slot);
	    }
	    return paths;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#run()
	 */
	public void run() {

	    
	    try {
		Vector<String> paths = parseArgs();

		String ret;

		// return empty if the dates are invalid
		if (paths.isEmpty())
		    ret = "empty";
		else {
		    OPTICSCluster optics = new OPTICSCluster(conf);
		    Vector<Topic> topics = optics.mergeTopics(paths);
		    ret = optics.generateHtmlTopics(topics, paths);
		    // System.out.println(ret.substring(0,1000));
		}
		
		
		PrintWriter out = new PrintWriter(conn.getOutputStream());
		out.println(ret);
		out.flush();
		
		conn.close();
		
		logger.info("finished");
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    }
	}
    }

}
