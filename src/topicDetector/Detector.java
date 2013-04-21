package topicDetector;

import java.io.IOException;
import java.util.Vector;

import util.Document;

public interface Detector {

   
    
    
    /*
     * detect topics from a set of documents. Clustering algorithms(OPTICS,DBSCAN) can be used to do 
     * the detection work. 
     * 
     * @param docs a set of documents from which the topics are detected
     * @return a set of detected topics
     */
    Vector<Topic> detectTopics(Vector<Document> docs) throws IOException;
}
