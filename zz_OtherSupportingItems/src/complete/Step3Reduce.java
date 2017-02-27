package complete;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.CustomProperties;



/**
 * Input: fromNode		fromNode_[ToNodes]_PageRank
 * Do nothing, just flow though to next job.
 * Output: fromNode		fromNode_[ToNodes]_PageRank 
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step3Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	
	String massLost=new String("0");
	
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
    	//Receive all the outgoing  edges to neighbours of Node i.
    	//nodeid, Node -> Just one
    	//nodeid, pMass -> Many many

    	
	    	Iterator<Text> iteratorNodes = values.iterator();
	    	
	    	Node thisNode=null;
	    	while(iteratorNodes.hasNext())
	    	{
	    		Text iteratorNode = iteratorNodes.next();
	    		if(iteratorNode.toString().split("_").length>1)
	    		{
	    			//This is a node, definitely not a dangler. if this is reached.
	    		    thisNode=new Node(iteratorNode.toString());
	    		}
	    	}    	
	        	context.write(key, thisNode.printToString());
	            CustomProperties.printDebug("Step3Reduce: "+ key.toString() + "\t" + thisNode.printToString());
    	
    }
}