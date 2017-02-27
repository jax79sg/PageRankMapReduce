package complete;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Composite Key is used for transfering in Hadoop. Mainly used for secondary sorting
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class NodeComposite implements  WritableComparable<NodeComposite> {
	Text nodeId;
	Text pagerank;
	
	public NodeComposite()
	{
		this.nodeId= new Text("0");
		this.pagerank=new Text("0.0");
	}
	
	public NodeComposite(String string) {
		// TODO Auto-generated constructor stub
		String[] nodeContents = string.split("_");
		this.nodeId=new Text(nodeContents[0]);
		this.pagerank=new Text(nodeContents[2]);

	}

	
	
	public Text getNodeId() {
		return nodeId;
	}

	public void setNodeId(Text nodeId) {
		this.nodeId = nodeId;
	}

	public Text getPagerank() {
		return pagerank;
	}

	public void setPagerank(Text pagerank) {
		this.pagerank = pagerank;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		nodeId.readFields(arg0);
		pagerank.readFields(arg0);
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		nodeId.write(arg0);
		pagerank.write(arg0);
	}
	
	/**
	 * The hadoop sorting will use this class (See driver code). When sorting the framework will pagerank
	 */
	@Override
	public int compareTo(NodeComposite o) {
		// TODO Auto-generated method stub
		int result=0;
		float argPageRank = Float.parseFloat(o.pagerank.toString());
		float thisPageRank = Float.parseFloat(this.pagerank.toString());
		if(thisPageRank>argPageRank)
		{
			result=1;
		}
		else
		if(thisPageRank<argPageRank)			
		{
			result=-1;
		}
		else
		if(thisPageRank<argPageRank)			
		{
			result=0;
		}
			
		return -1*result;
	}
	
	public Text printToString()
	{
		return new Text(this.nodeId.toString()+"\t"+this.pagerank);
	}

}
