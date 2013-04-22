package topicDetector;

import java.io.IOException;
import java.util.Vector;


public interface Detector {
    
    /*
     * detect topics from a set of documents. Clustering algorithms(OPTICS,DBSCAN) can be used to do 
     * the detection work. 
     * 
     * @param path a file path storing a set of documents
     * @return a set of detected topics
     */
    Vector<Topic> detectTopics(String path) throws IOException;
}
