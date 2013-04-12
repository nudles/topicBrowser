package twitter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.json.simple.JSONObject;

import util.Configure;
import util.Document;

public class Tweet extends Document{
    
    
    //if this tweet is a retweet, retid is the original tweet id
    public long retid=0;
    protected String text;
    

    public Tweet(long id, long retid, int support, long time){
	super(id,support,time);
	this.retid=retid;
    }
    
    public Tweet(long id, long retid, int support, long time, String text, int len){
	super(id,support,time);
	this.retid=retid;	
	this.text=text;
	this.vec=new Vector<Word>(15);
    }
    
    /*
     * set tweet length in terms of words(include meaningless word, e.g., kkkkk)
     * @param l length of the tweet
     */
    public  void setLength(int length){
	len=length;
    }
    
    
    
    /*
     * convert the tweet into a json string
     * 
     * @return  json string represent this tweet
     */
    @SuppressWarnings("unchecked")
    public String encode(){
	
	JSONObject jsonTweet=new JSONObject();
	jsonTweet.put("id", new Long(id));
	jsonTweet.put("retid", new Long(retid));
	//jsonTweet.put("support", new Integer(support));
	jsonTweet.put("time", new Long(time));
	jsonTweet.put("text", text);
	//jsonTweet.put("len", len);
	
	JSONObject jsonVec=new JSONObject();
	for(int i=0;i<vec.size();i++)
	    jsonVec.put(vec.get(i).getId(),vec.get(i).getWeight());
	if(vec.size()>0)
	    jsonTweet.put("vec", jsonVec);
	
	StringWriter out=new StringWriter();
	try {
	    jsonTweet.writeJSONString(out);
	} catch (IOException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();	    
	}
	
	return out.toString();	
    }

    
}
