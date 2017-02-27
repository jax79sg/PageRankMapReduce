package complete;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import util.CustomProperties;
import util.CustomProperties.PAGERANK_COUNTER;


/**
 * Input:fromNode	fromNode_[ToNodes]_PageRank
 * Calculate mass to distribute, then emit fromNode	fromNode_[ToNodes]_PageRank (Pass along structure)
 * Output1: fromNode	fromNode_[ToNodes]_PageRank
 * Emit the pmass of neighbour that will receive from this node.
 * Output2: toNode		distributeMass
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step2Map extends Mapper<LongWritable, Text, LongWritable, Text>{
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	Logger logger = Logger.getLogger(Step2Map.class);
    	
    	Node thisNode = new Node(value.toString().split("\t")[1]);
    	//Calculate mass to distribute, then emit fromNode	fromNode_[ToNodes]_PageRank (Pass along structure)
    	FloatWritable pMass = new FloatWritable(thisNode.getPagerankFlt()/thisNode.getSizeOfAdjacencyList());
    	context.write(new LongWritable(thisNode.getNodeIdInt()), thisNode.printToString()); //LongWritable, Node

		long amplifiedMassGiven = Math.round(thisNode.getPagerankFlt()*CustomProperties.multiplier);
		context.getCounter(PAGERANK_COUNTER.MASSGIVENOUT).increment(amplifiedMassGiven);
    	
    	CustomProperties.printDebug("Step2Mapy MainNode: \t" + thisNode.getNodeIdInt() + "\t" + thisNode.printToString());
    	
    	List<String> outlinkList = thisNode.getOutlinkListList();
    	for (int i=0; i<outlinkList.size();i++)
    	{
    		//Emit the pmass that this neighbour will receive
    		LongWritable neighbourId = new LongWritable(Integer.parseInt(outlinkList.get(i)));
    		context.write(neighbourId, new Text(pMass.toString()));	//LongWritable, Text
    		CustomProperties.printDebug("Step2Mapy Neighbours: \t" + neighbourId.toString() + "\t" + pMass.toString());
    	}    	
    }


    
    
}