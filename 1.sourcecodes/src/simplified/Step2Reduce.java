package simplified;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.CustomProperties;


/**
 * Input1: fromNode	fromNode_[ToNodes]_PageRank
 * Input2: toNode		distributeMass
 * Sum up all the mass that is emitted by other nodes and consolidated here.
 * PageRank computeed for this iteration.
 * Emit the entire structure with pagerank info.
 * output: fromNode		fromNode_[ToNodes]_PageRabk
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step2Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
	
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
    	//Receive all the outgoing  edges to neighbours of Node i.
    	//nodeid, Node -> Just one
    	//nodeid, pMass -> Many many
    	
    	Iterator<Text> iteratorNodesAndPmasses = values.iterator();
    	
    	Node thisNode=null;
    	float sumPagerank=0;    
    	while(iteratorNodesAndPmasses.hasNext())
    	{
    		Text iteratorNodesAndPmass = iteratorNodesAndPmasses.next();
    		if(iteratorNodesAndPmass.toString().split("_").length>1)	//Check if its a node
    		{
    		    thisNode=new Node(iteratorNodesAndPmass.toString());
    		}
    		else
    		{
    			//Not a node, means its the pagerank mass
    			//Sum up all the mass that is emitted by other nodes and consolidated here.
    			sumPagerank=sumPagerank+Float.parseFloat(iteratorNodesAndPmass.toString());
    		}
    	}    	
    	
    	if (thisNode!=null)
    	{
    		//PageRank computeed for this iteration.
    		//Emit the entire structure with pagerank info.
        	thisNode.setPagerank(sumPagerank);
        	context.write(key, thisNode.printToString());
            CustomProperties.printDebug("Step2Reduce: "+ key.toString() + "\t" + thisNode.printToString());
    	}
    	
    }
}