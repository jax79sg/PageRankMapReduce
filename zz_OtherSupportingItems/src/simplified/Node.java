package simplified;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;




/**
 * Used to help organise the structure
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public class Node implements Writable{
	
	int nodeId;
	Text outlinkList = new Text();
	float pagerank=0;
	
	public Node()
	{
		
	}
	
	public Node(String string)
	{
		String[] nodeContents = string.split("_");
		this.nodeId=Integer.parseInt(nodeContents[0]);
		this.outlinkList=new Text(nodeContents[1]);
		this.pagerank=Float.parseFloat(nodeContents[2]);
		
	}

	public Node(String fromNode, String toNode, float initialPageRank) {
		// TODO Auto-generated constructor stub
		this.nodeId=Integer.parseInt(fromNode);
		this.pagerank=initialPageRank;
		this.addToAdjacencyList(toNode);
	}	
	
	public int getSizeOfAdjacencyList()
	{		
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
	{	//fromNode_[ToNodes]_PageRabk
		Text result=new Text(this.nodeId +"_"+outlinkList.toString() +"_"+this.pagerank);
		return result;
	}
	
	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
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

	public float getPagerank() {
		return pagerank;
	}

	public void setPagerank(float pagerank) {
		this.pagerank = pagerank;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
