package simplified;

import java.io.IOException;
import org.apache.log4j.Logger;

import util.CustomProperties;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
PageRank Simplified version
Job 1
Mapper
 * Read the file.
 * Extract totalnodes and total edges from the comments
 * Read every line and emit fromnode,tonode

Reducer
 * Input fromNode#TotalNodes, ToNotes....
 * Compute initial page rank for all nodes. 1/totalnodes
 * Pack into adjacency list
 * Output //fromNode_[ToNodes]_PageRabk		//Using Text, so use string to manipulate

Start Iterate(Job2)
Job 2
Mapper
 * Input:fromNode	fromNode_[ToNodes]_PageRank
 * Calculate mass to distribute, then emit fromNode	fromNode_[ToNodes]_PageRank (Pass along structure)
 * Output1: fromNode	fromNode_[ToNodes]_PageRank
 * Emit the pmass of neighbour that will receive from this node.
 * Output2: toNode		distributeMass

Reducer
 * Input1: fromNode	fromNode_[ToNodes]_PageRank
 * Input2: toNode		distributeMass
 * Sum up all the mass that is emitted by other nodes and consolidated here.
 * PageRank computeed for this iteration.
 * Emit the entire structure with pagerank info.
 * output: fromNode		fromNode_[ToNodes]_PageRabk
 * 
End Iterate(Job2) - No convergence, but used complete as benchmark for timing.

Job 3
Mapper
 * Input: fromNode		fromNode_[ToNodes]_PageRank
 * Just emit whatever comes in and let hadoop framework do the secondary sorting. See NodeComposite.java and the Driver codes.
 * Output: fromNode_[ToNodes]_PageRank	-1 //-1 is just a placeholder for my own identification
 * 
 Reducer
 * Input: fromNode_[ToNodes]_PageRank	-1
 * Limit to top 10
 * Output: Node		PageRank

 */
public class MapReduce extends Configured implements Tool {

	Logger logger = Logger.getLogger(MapReduce.class);
    public static void main(String[] args) throws Exception {
    	
    	long start = System.currentTimeMillis();
        int res = ToolRunner.run(new Configuration(), new MapReduce(), args);
        long duration = System.currentTimeMillis() - start;
        String durationStr=DurationFormatUtils.formatDuration(duration, "mm:ss:SS");
        CustomProperties.printDebug("Duration:"+durationStr);
        System.exit(res);
	}

    public int run(String args[]) {
    	
    	
    	
        try {
        	boolean jobStatus=runJob1(args[0],args[1]);

            if (jobStatus)
            {
            	int i=0;
        		String input=null;
        		String output=null;

            	while(i<CustomProperties.noOfPageRankInteration)
            	{       
            		if (i==0)
            		{
            			input=args[1];
            			output=args[2]+"iteration_"+i;
            		}
            		else
            		{
            			input=output;
            			output=args[2]+"iteration_"+i;
            		}
            		CustomProperties.printDebug("Running Job Two Iteration: " + i + "\n"+"Input: "+input+"\nOutput: "+output);
            		jobStatus=runJob2(input,output);	
            		i++;
            	}
            	input=output;
            	jobStatus=runJob4(input,args[2]+"COMPLETED");

            }
            else
            {
            	return 1;
            }
            
            if(jobStatus)
            {
            	return 0;
            }else
            {
            	return 1;
            }
            
        } catch (InterruptedException|ClassNotFoundException|IOException e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }

	private boolean runJob4(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
    	//Step 1        	
		
        Configuration conf = new Configuration();              
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapReduce.class);
        // specify a mapper
        job.setMapperClass(Step4Map.class);
        // specify a reducer
        job.setReducerClass(Step4Reduce.class);
        // specify a partitioner
        //job.setPartitionerClass(CustomPartitioner.class);
        // specify output types
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NodeComposite.class);	//For secondary sorting
        job.setOutputValueClass(Text.class);
        // specify input and output DIRECTORIES
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job4status=job.waitForCompletion(true);

        
        CustomProperties.printDebug("Job four sucess:"+job4status);
        
       return job4status;
	}
        
    
    
	private boolean runJob2(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
    	//Step 1        	
		
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapReduce.class);
        // specify a mapper
        job.setMapperClass(Step2Map.class);
        // specify a reducer
        job.setReducerClass(Step2Reduce.class);
        // specify a partitioner
        // job.setPartitionerClass(pairs.CustomCompositePartitioner.class);
        // specify output types
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        // specify input and output DIRECTORIES
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job2status=job.waitForCompletion(true);
        CustomProperties.printDebug("Job two sucess:"+job2status);
        return job2status;
	}

	private boolean runJob1(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
    	//Step 1        	
		CustomProperties.printDebug("Running job one: Sorting and packing");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapReduce.class);
        // specify a mapper
        job.setMapperClass(Step1Map.class);
        // specify a reducer
        job.setReducerClass(Step1Reduce.class);
        // specify a partitioner
        //job.setPartitionerClass(pairs.CustomPartitioner.class);
        // specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(5);
        // specify input and output DIRECTORIES      
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job1status=job.waitForCompletion(true);
        CustomProperties.printDebug("Job one sucess:"+job1status);
        return job1status;
	}
}