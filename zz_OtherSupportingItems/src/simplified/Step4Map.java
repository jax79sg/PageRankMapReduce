package simplified;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import util.CustomProperties;


/**
 * Input: fromNode		fromNode_[ToNodes]_PageRank
 * Just emit whatever comes in and let hadoop framework do the sorting. See NodeComposite.java and the Driver codes.
 * Output: fromNode_[ToNodes]_PageRank	-1 //-1 is just a placeholder for my own identification
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step4Map extends Mapper<LongWritable, Text, NodeComposite, Text>{
	
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	NodeComposite thisNode = new NodeComposite(value.toString().split("\t")[1]);
    	context.write(thisNode, new Text("-1"));
    	CustomProperties.printDebug("Step4Mapy MainNode: \t"  + thisNode.printToString() + "\t-1");
    	//System.out.println("Step4Mapy MainNode: \t"  + thisNode.printToString() + "\t-1");
    	
    	    	
    }


    
    
}