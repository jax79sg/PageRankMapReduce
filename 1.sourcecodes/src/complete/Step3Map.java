package complete;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import util.CustomProperties;
import util.CustomProperties.PAGERANK_COUNTER;


/**
 * Input: fromNode		fromNode_[ToNodes]_PageRank
 * Get masslost
 * Compute complete pagerank
 * Output: fromNode		fromNode_[ToNodes]_PageRank
 * 
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step3Map extends Mapper<LongWritable, Text, LongWritable, Text>{
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	Logger logger = Logger.getLogger(Step3Map.class);
    	
    	Node thisNode = new Node(value.toString().split("\t")[1]);
    	
    	// updatedPmass = alpha*(1/totalNodes) + dampingFactor * ((massLost/totalNodes)+currentPmass)
    	//Get totalNodes from Hadoop counter
    	Configuration conf  = context.getConfiguration();
    	float totalNodes = conf.getFloat("totalNodes",0);
    	float dampingFactor=CustomProperties.dampingFactor;
    	// dampingFactor = 1 - alpha;
    	float alpha = 1 - dampingFactor;
    	//Get total mass lost from counter for this iteration
    	float massLost = ( conf.getFloat("trueMassLost",-1));	
    	
    	float currentPmass = thisNode.getPagerankFlt();
    	//Compute complete pagerank algo
    	float updatedPmass = alpha*(1/totalNodes) + dampingFactor * ((massLost/totalNodes)+currentPmass);
    	thisNode.setPagerank(updatedPmass);
    	
    	context.write(new LongWritable(thisNode.getNodeIdInt()), thisNode.printToString()); //LongWritable, Node
    	CustomProperties.printDebug("Step3Map MainNode: \t" + thisNode.getNodeIdInt() + "\t" + thisNode.printToString());

    }


    
    
}