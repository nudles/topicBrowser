package browser;

import java.io.IOException;



import twitter.TweetParser;
import util.Configure;
import util.Converter;
import util.Parser;

/*
 * This is the controller of the project.
 * It pre-processes data, clusters topics, serves user querying
 */
public class Browser {
    
    public static void main(String []args) throws Exception {
	if(args.length==0){
	    System.out.println("This program consists of three functions\n" +
	    		"1. convert raw tweets into cleaned representation\n" +
	    		"2. query tok-K topics in given time window\n"+
	    		"3. do topic clustering based on tweets in each unit time window\n"+
	    		"Usage: -cmd convert|cluster|query\t[-conf confFilePath}] " +
	    			"\t[-query {startTime duration}]\n");
	    System.exit(0);
	}
	
	String cmd="";
	String confPath="config/config.xml";
	String startTime, endTime;
	
	for(int i=0;i<args.length;i++){
	    if("-cmd".equals(args[i])){
		cmd=args[i+1];
		i++;
	    }
	    else if("-conf".equals(args[i])){
		confPath=args[i+1];
		i++;
	    }
	    else if("-query".equals(args[i])){
		startTime=args[i+1];
		endTime=args[i+2];
		i+=2;
	    }
	    else{
		System.out.println("Bad input param:"+args[i]);
		i++;
	    }
	}
	
	Configure conf=new Configure(confPath);
	conf.config();
	
	if(cmd.equals("convert")){
	    Parser parser=null;
	    if(conf.getPlatform().equals("Twitter"))
	    	parser=new TweetParser(conf);
	    Converter converter=new Converter(conf,parser);
	    try {
		converter.convert();
		
	    } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
		throw new IOException("error in RawTweetConvert\n");
	    }
	}
    }

}
