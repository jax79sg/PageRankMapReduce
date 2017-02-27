package complete;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

import util.CustomProperties;



/**
 * Input fromNode#TotalNodes, ToNotes....
 * Compute initial page rank for all nodes. 1/totalnodes
 * Output //fromNode_[ToNodes]_PageRabk		//Using Text, so use string to manipulate

 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step1Reduce extends Reducer<Text, Text, LongWritable, Text> {
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	//Receive all the outgoing  edges to neighbours of Node i.
    	
    	CustomProperties.printDebug("Step1Reduce: Starting");
    	String nodeId = key.toString().split("#")[0];
    	float totalNodesFlt = Float.parseFloat(key.toString().split("#")[1]);
    	float initialPageRank = 1/totalNodesFlt;
    	CustomProperties.printDebug("Step1Reduce: TotalNodes:"+ totalNodesFlt + "\t" +"Initial PageRank: " +initialPageRank);
    	
    	Iterator<Text> iteratorNeighbours = values.iterator();
    	String neighbours = new String();
    	while(iteratorNeighbours.hasNext())
    	{
    		String neighbour = iteratorNeighbours.next().toString();
	    		if (neighbours.compareTo("")==0)
	    		{
	    			//Current list is empty
	    			neighbours = neighbours+neighbour;
	    		}
	    		else
	    		{
	    			//List has at least one item
	    			neighbours = neighbours+","+neighbour;
	    		}
    		
    	}    	
    	
    	
    	Node node = new Node(nodeId,neighbours,initialPageRank);
    	context.write(new LongWritable(node.getNodeIdInt()), node.printToString());
        CustomProperties.printDebug("Step1Reduce: "+ node.getNodeIdInt() + "\t" + node.printToString());
    }
}