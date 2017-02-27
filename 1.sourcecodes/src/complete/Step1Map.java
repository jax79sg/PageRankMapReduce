package complete;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import util.CustomProperties;
import util.CustomProperties.PAGERANK_COUNTER;


/**
 * Read the file.
 * Extract totalnodes and total edges from the comments
 * Read every line and emit fromnode#totalnodes,tonode //Unable to transfer value to reducer via counter, so use this method here.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step1Map extends Mapper<LongWritable, Text, Text, Text>{
	String totalNodes="0";
	String totalEdges="0";
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	Logger logger = Logger.getLogger(Step1Map.class);
    	String line = value.toString();
    	if (line.contains("#"))
    	{
    		//This is a commented line in the text file. It means i can retrieve the total nodes/edges.
    		
    		if(line.contains("Nodes"))
    		{
    			//This line contains information about total number of nodes
    			//Save it into Hadoop counter for use later
    			totalNodes=line.split(" ")[2];
    			totalEdges=line.split(" ")[4];
    			//Using Hadoop counter to hold the values. The counter is INT, so i amplify the value. In the driver code, this should be scaled back to original before using
    			if (context.getCounter(PAGERANK_COUNTER.TOTALNODES).getValue()==0)
    			{
    				context.getCounter(PAGERANK_COUNTER.TOTALNODES).increment(Long.parseLong(totalNodes));
    			}
    			
    			if (context.getCounter(PAGERANK_COUNTER.TOTALEDGES).getValue()==0)
    			{
    				context.getCounter(PAGERANK_COUNTER.TOTALEDGES).increment(Long.parseLong(totalEdges));
    			}
    			
    		}
    	}
    	else
    	{
    		//This line contains a directed link
    		String fromNode = line.split("\\t")[0]+"#"+totalNodes; //Unable to transfer value to reducer via counter, so use this method here.
    		String toNode=line.split("\\t")[1];
    		context.write(new Text(fromNode), new Text(toNode));
    		CustomProperties.printDebug("Step1Map: NodeId: " +fromNode + ", ToNode: " + toNode);
    	}
    	
    }


    
    
}