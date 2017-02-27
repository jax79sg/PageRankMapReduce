package simplified;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.CustomProperties;


/**
 * Input: fromNode_[ToNodes]_PageRank	-1
 * Output: Node		PageRank
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Step4Reduce extends Reducer<NodeComposite, Text, Text, Text> {
	int printCounter=0;
    public void reduce(NodeComposite key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	if(printCounter<10)	//Limit to top 10. This is guaranteed because secondary sorting come in sorted desc. 
    	{    	
	        context.write(new Text(key.getNodeId().toString()),new Text(key.getPagerank().toString()));
	        CustomProperties.printDebug("Step4Reduce: "+ key.getNodeId().toString()+"\t"+key.getPagerank().toString());
	        printCounter++;
    	}
    }
}