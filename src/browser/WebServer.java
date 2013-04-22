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

public class WebServer {
    static final Logger logger = Logger.getLogger(Controler.class);

    public static void main(String[] args) throws ParserConfigurationException,
	    SAXException {
	try {
	    String confPath = "config/config.xml";
	    Configure conf = new Configure(confPath);

	   
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

    public static class Worker extends Thread {
	private Socket conn;
	private Configure conf;

	public Worker(Socket conn, Configure conf) {
	    this.conn = conn;
	    this.conf = conf;
	}

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
		slot++;

		f = new File(dir + "/" + slot + ".txt");
		if (f.exists())
		    paths.add(dir + "/" + slot);
	    }
	    return paths;
	}

	public void run() {

	    try {
		Vector<String> paths = parseArgs();
		String ret;
		if (paths.isEmpty())
		    ret = "empty";
		else {
		    OPTICSCluster optics = new OPTICSCluster(conf);
		    Vector<Topic> topics = optics.mergeTopics(paths);
		    ret = optics.encodeTopics(topics, paths);
		}
		PrintWriter out = new PrintWriter(conn.getOutputStream());
		// TODO encode topics into string and output it
		out.println(ret);
		out.flush();
		conn.close();
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	    } catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		logger.info("no valid dates");
	    }
	}
    }

}
