package complete;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


/**
 * Used to help organise the structure
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Node implements WritableComparable<Node>{
	
	IntWritable nodeId;
	Text outlinkList = new Text();
	FloatWritable pagerank= new FloatWritable(0);
	int totalNodes;
	int totalEdges;
	float massLost;
	
	
	
	public Node()
	{
		
	}
	
	public Node(String string)
	{
		String[] nodeContents = string.split("_");
		this.nodeId=new IntWritable(Integer.parseInt(nodeContents[0]));
		this.outlinkList=new Text(nodeContents[1]);
		this.pagerank=new FloatWritable(Float.parseFloat(nodeContents[2]));
		
	
	}

	public Node(String fromNode, String toNode, float initialPageRank) {
		// TODO Auto-generated constructor stub
		this.nodeId=new IntWritable(Integer.parseInt(fromNode));
		this.pagerank=new FloatWritable(initialPageRank);
		this.addToAdjacencyList(toNode);
	}	

	public Node(String fromNode, String toNode, float initialPageRank, String totalNodes, String totalEdges, String massLost) {
		// TODO Auto-generated constructor stub
		this.nodeId=new IntWritable(Integer.parseInt(fromNode));
		this.pagerank=new FloatWritable(initialPageRank);
		this.addToAdjacencyList(toNode);
	}	
	
	
	public int getSizeOfAdjacencyList()
	{		
		//CustomProperties.printDebug("Size of nodeId:" + this.nodeId + " is " + outlinkList.toString().split(",").length);
		return outlinkList.toString().split(",").length;
	}

	public void addToAdjacencyList(String node)
	{
		if (outlinkList.toString().compareTo("")==0)
		{
			//Current list is empty
			this.outlinkList = new Text(node);
		}
		else
		{
			//List has at least one item
			String currentListStr = outlinkList.toString();
			currentListStr=currentListStr+","+node;
			this.outlinkList = new Text(currentListStr);
		}
	}
	
	public Text printToString()
	{
		//Text result=new Text(this.nodeId +"_"+outlinkList.toString() +"_"+this.pagerank+"_"+this.totalNodes+"_"+this.totalEdges+"_"+this.massLost);
		Text result=new Text(this.nodeId +"_"+outlinkList.toString() +"_"+this.pagerank);
		return result;
	}
	
	public Text printFinal()
	{
		Text result=new Text(this.nodeId + "\t"+this.pagerank);
		return result;
	}	
	
	public int getNodeIdInt() {
		return nodeId.get();
	}

	public void setNodeId(int nodeId) {
		this.nodeId = new IntWritable(nodeId);
	}


	public List<String> getOutlinkListList() {
		String[] outlinkListArray = outlinkList.toString().split(",");
		List<String> newList = new ArrayList<String>();
		for (int i=0;i<outlinkListArray.length;i++)
		{
			newList.add(outlinkListArray[i]);
		}
		return newList;
	}
	
	
	public Text getOutlinkListTxt() {
		return outlinkList;
	}

	public void setOutlinkListTxt(Text outlinkList) {
		this.outlinkList = outlinkList;
	}

	public float getPagerankFlt() {
		return pagerank.get();
	}

	public void setPagerank(float pagerank) {
		this.pagerank = new FloatWritable(pagerank);
	}
	
	public IntWritable getNodeId() {
		return nodeId;
	}

	public void setNodeId(IntWritable nodeId) {
		this.nodeId = nodeId;
	}

	public FloatWritable getPagerank() {
		return pagerank;
	}

	public void setPagerank(FloatWritable pagerank) {
		this.pagerank = pagerank;
	}	

	@Override
	public void readFields(DataInput readin) throws IOException {
		// TODO Auto-generated method stub
		
		nodeId.readFields(readin);
		
		outlinkList.readFields(readin);
		
		pagerank.readFields(readin);
		
		
	}

	@Override
	public void write(DataOutput writeout) throws IOException {
		// TODO Auto-generated method stub
		
		nodeId.write(writeout);
		outlinkList.write(writeout);
		pagerank.write(writeout);
		
	}

	@Override
	public int compareTo(Node o) {
		// TODO Auto-generated method stub
		float thisPageRank = this.getPagerankFlt();
		float pageRank = o.getPagerankFlt();
		int result;
		if(pageRank==thisPageRank)
		{
			result=0;
		}
		else
		if(thisPageRank<pageRank)
		{
			result=-1;
		}
		else
		{
			result=1;
		}
		
		return -1*result;
	}
	
}
