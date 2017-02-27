package complete;

import java.io.IOException;
import org.apache.log4j.Logger;

import util.CustomProperties;
import util.CustomProperties.PAGERANK_COUNTER;

import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
PageRank Complete version
Job 1
Mapper
 * Read the file.
 * Extract totalnodes and total edges from the comments
 * Read every line and emit fromnode#totalnodes,tonode //Unable to transfer value to reducer via counter, so use this method here.

Reduce
 * Input fromNode#TotalNodes, ToNotes....
 * Compute initial page rank for all nodes. 1/totalnodes
 * Output //fromNode_[ToNodes]_PageRabk		//Using Text, so use string to manipulate

Start Iterate(Job2, Job3)
Job 2
 * Track mass lost
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

Job 3
Mapper
 * Input: fromNode		fromNode_[ToNodes]_PageRank
 * Get masslost
 * Compute complete pagerank
 * Output: fromNode		fromNode_[ToNodes]_PageRank

Reducer
 * Input: fromNode		fromNode_[ToNodes]_PageRank
 * Do nothing, just flow though to next job.
 * Output: fromNode		fromNode_[ToNodes]_PageRank 

End Iterate(Job2, Job3) - Fixed Iteration based on local testing for convergence

Job 4
Mapper
 * Input: fromNode		fromNode_[ToNodes]_PageRank
 * Just emit whatever comes in and let hadoop framework do the sorting. See NodeComposite.java and the Driver codes.
 * Output: fromNode_[ToNodes]_PageRank	-1 //-1 is just a placeholder for my own identification

Reducer
 * Input: fromNode_[ToNodes]_PageRank	-1
 * Output: Node		PageRank

 */
public class MapReduce extends Configured implements Tool {

	Logger logger = Logger.getLogger(MapReduce.class);
	long totalNodes=0;
	long totalEdges=0;
	float trueMassLost=0.0f;
	
    public static void main(String[] args) throws Exception {
    	
    	long start = System.currentTimeMillis();
        int res = ToolRunner.run(new Configuration(), new MapReduce(), args);
        long duration = System.currentTimeMillis() - start;
        String durationStr=DurationFormatUtils.formatDuration(duration, "mm:ss:SS");
        CustomProperties.printDebug("Duration:"+durationStr);
        System.exit(res);
	}

    /**
     * Coordinate the jobs
     * Hadoop counters explicitly used for sharing certain information between jobs.
     */
    public int run(String args[]) {
    	
    	
    	
        try {
        	boolean jobStatus=runJob1(args[0],args[1]);
        	
        	
            if (jobStatus)
            {
            	
            	int i=0;
        		String input=null;
        		String output=null;

        		//Feeding the output of job 2 into job 3, then output of job 3 to job 2 and iterate
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
            		System.out.println("Running Job Two Iteration: " + i + "\n"+"Input: "+input+"\nOutput: "+output);
            		jobStatus=runJob2(input,output);	
            		System.out.println("Mass loss in this iteration = " + trueMassLost);
            		input = output;
            		output = input+"a";
            		CustomProperties.printDebug("Running Job Three Iteration: " + i + "\n"+"Input: "+input+"\nOutput: "+output);
            		System.out.println("Running Job Three Iteration: " + i + "\n"+"Input: "+input+"\nOutput: "+output);
            		jobStatus=runJob3(input,output);	
            		trueMassLost=0;
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

    /**
     * Driver for job 4
     * @param input
     * @param output
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
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
        job.setNumReduceTasks(1);	//Limit to 1 reducer
        job.setOutputKeyClass(NodeComposite.class);
        job.setOutputValueClass(Text.class);
        // specify input and output DIRECTORIES
        logger.info("Arg[0]:"+ input);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        logger.info("Arg[1]:"+ output);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job4status=job.waitForCompletion(true);

        
        //counters.findCounter(PAGERANK_COUNTER.MASSLOST).setValue(0);
        //System.out.println("Reset counter to "+ counters.findCounter(PAGERANK_COUNTER.MASSLOST).getValue());
        CustomProperties.printDebug("Job four sucess:"+job4status);
        
       return job4status;
	}
    
    
    /**
     * Driver for Job 3
     * @param input
     * @param output
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
	private boolean runJob3(String input, String output) throws IOException, ClassNotFoundException, InterruptedException {
    	//Step 1        	
		
        Configuration conf = new Configuration();
        
        conf.setLong("totalNodes",totalNodes);
        conf.setFloat("trueMassLost", trueMassLost);
        
        Job job = Job.getInstance(conf);
        job.setJarByClass(MapReduce.class);
        // specify a mapper
        job.setMapperClass(Step3Map.class);
        // specify a reducer
        job.setReducerClass(Step3Reduce.class);
        // specify a partitioner
        // job.setPartitionerClass(pairs.CustomCompositePartitioner.class);
        // specify output types
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        // specify input and output DIRECTORIES
        logger.info("Arg[0]:"+ input);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        logger.info("Arg[1]:"+ output);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job3status=job.waitForCompletion(true);

        
        //counters.findCounter(PAGERANK_COUNTER.MASSLOST).setValue(0);
        //System.out.println("Reset counter to "+ counters.findCounter(PAGERANK_COUNTER.MASSLOST).getValue());
        CustomProperties.printDebug("Job three sucess:"+job3status);
        
        return job3status;
	}
    
    
	/**
	 * Driver for Job 2
	 * Keep track of masslost
	 * input: Adjacency list
	 * output: 
	 * @param input	
	 * @param output 
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
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
        // specify input and output DIRECTORIESset count
        logger.info("Arg[0]:"+ input);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        logger.info("Arg[1]:"+ output);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job2status=job.waitForCompletion(true);
        Counters counters = job.getCounters();
        
        
        long massLostMultiplied = counters.findCounter(PAGERANK_COUNTER.MASSLOST).getValue();
        trueMassLost = massLostMultiplied/CustomProperties.multiplier;
        
        long massGivenMultiplied = counters.findCounter(PAGERANK_COUNTER.MASSGIVENOUT).getValue();
        float trueMassGiven= massGivenMultiplied/CustomProperties.multiplier;
        trueMassLost=1-trueMassGiven; //I can use this since i started with initialpagerank = 1/totalNodes
        CustomProperties.printDebug("Job two sucess:"+job2status);
        
        return job2status;
	}

	/**
	 * Driver for Job 1 
	 * Also used counters for Driver to keep track of the total number of nodes for use later.
	 * Input: File of epinion
	 * Output: Adjacency list
	 * @param input "s3://emr-cc/emr-input "
	 * @param output "s3://emr-cc/emr-output/complete/results"
	 * @return
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
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
        // specify output types
        job.setOutputKeyClass(Text.class);	//To save into S3
        job.setOutputValueClass(Text.class); //To save into S3
        // specify input and output DIRECTORIES
        logger.info("Arg[0]:"+ input);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);
        logger.info("Arg[1]:"+ output);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        
        boolean job1status=job.waitForCompletion(true);
        Counters counters = job.getCounters();
        totalNodes = counters.findCounter(PAGERANK_COUNTER.TOTALNODES).getValue();
        totalEdges = counters.findCounter(PAGERANK_COUNTER.TOTALEDGES).getValue();
        
        
        
        CustomProperties.printDebug("Job one sucess:"+job1status +"\nTotal Nodes: "+totalNodes+"\nTotal Edges: "+totalEdges);        
        return job1status;
	}
}