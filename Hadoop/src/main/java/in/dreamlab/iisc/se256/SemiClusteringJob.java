package in.dreamlab.iisc.se256;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.*;
/**
 * Based on by WARCTagCounter by Stephen Merity (Smerity)
 * 
 * @author simmhan extended by Abhilash
 *
 */
public class SemiClusteringJob extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(SemiClusteringJob.class);
 	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new SemiClusteringJob(), args);
        System.exit(res);
	}
		
	public int run(String[] args) throws Exception {
	    /////////////////////////////////////////////////////////
	    // Parse args from commandline
	    // NOTE: Input has to be under /user/$username (or) /SE256 
	    // NOTE: Output has to be under /user/$username ONLY. Not having a leading '/' will ensure that.
		String inputPath = args.length >= 1 ? args[0] : "/SE256/CC/*19*.warc.gz";
		String outputPath = args.length >= 2 ? args[1] : "alpha/cc/output";
		long numMaps = Long.parseLong(args.length >= 3 ? args[2] : "40"); // not used now
		long numReduces = Long.parseLong(args.length >= 4 ? args[3] : "10");
		long memMaps = Long.parseLong(args.length >= 5 ? args[4] : "4000");
		long memReduces = Long.parseLong(args.length >= 6 ? args[5] : "4000");
        
		
		/////////////////////////////////////////////////////////
	    // Init configuration. Do this BEFORE creating Job instance. 
	    // Set Mapper and Reducer resource requirements, overriding default.
		Configuration conf = new Configuration();
		
//		conf.setBoolean("yarn.nodemanager.vmem-check-enabled", true);
//		conf.setBoolean("yarn.nodemanager.pmem-check-enabled", true);
//		conf.setDouble("yarn.nodemanager.vmem-pmem-ratio", 1.5);
		
		conf.setLong("mapreduce.map.memory.mb", memMaps); // we have large input compressed files, so mapper needs more memory 
		conf.setLong("mapreduce.map.java.opts.max.heap", (long)0.9*memMaps); // ensure we don't go to virtual memory
		conf.setLong("mapreduce.map.cpu.vcores", 1); // one core will suffice for each mapper
		//conf.setLong("mapreduce.job.maps", numMaps); // number of mappers decided by splits from WARC input format  
		
		conf.setLong("mapreduce.reduce.memory.mb", memReduces); // Reducer output is more modest
		conf.setLong("mapreduce.reduce.java.opts.max.heap", (long)0.9*memReduces);
		conf.setLong("mapreduce.reduce.cpu.vcores", 1);
		conf.setLong("mapreduce.job.reduces", numReduces);
 
//		ArrayList<String> list = new ArrayList<String>();
//		
//		
//		String temp;
//		try{
//		BufferedReader br = new BufferedReader(new FileReader("/home/abhilash/countries.txt"));//change accordingly
//		while ((temp = br.readLine()) != null) {
//			list.add(temp);
//		}
//			br.close();
//		}
//		catch(Exception ex)
//		{
//			System.out.println("Error while reading file with list of countries");
//		}
//		String[] arr = list.toArray(new String[list.size()]);
//		conf.setStrings("cntr_arr", arr);
		//conf.set("test","123");
		
		
		conf.set("iteration", "0");
	    /////////////////////////////////////////////////////////
	    // Init job with configuration. Update configuration BEFORE creating job.
		// Give a human-friendly job name. 
	    Job job = Job.getInstance(conf, "SemiClustering");
	    
	    // Log configuration used to run job
	    LOG.info("====================== CONF START ======================");
	    for(Entry<String, String> entry : job.getConfiguration()){
	    	LOG.info(entry.getKey() + "=" + entry.getValue());
	    }
	    LOG.info("====================== CONF END ======================");
	    
	    
	    /////////////////////////////////////////////////////////
	    // Set classes for Job, Mapper, Combiner and Reducer. 
	    job.setJarByClass(SemiClusteringJob.class);	    
	    job.setMapperClass(SemiClusteringMapper.class);
//	    job.setCombinerClass(LongSumReducer.class);
	    job.setReducerClass(SemiClusteringReducer.class);

	    
	    /////////////////////////////////////////////////////////
	    // Set input and output file locations. 
	    // NOTE: Input has to be under /user/$username (or) /SE256 
		FileInputFormat.addInputPath(job, new Path(inputPath));

	    // NOTE: Output has to be under /user/$username ONLY. Not having a leading '/' will ensure that.
		FileSystem fs = FileSystem.newInstance(conf);
		if (fs.exists(new Path(outputPath))) { // delete output folder if already present
			fs.delete(new Path(outputPath), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		LOG.info("Input path: " + inputPath); 
		LOG.info("Output path: " + outputPath);

		
	    /////////////////////////////////////////////////////////
	    // Set input and output file formats. 	    
//		job.setInputFormatClass(WARCFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		
	    /////////////////////////////////////////////////////////
	    // Set output Key-Value types. 
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    
	    // Submit the job, then poll for progress until the job is complete
        return job.waitForCompletion(true) ? 0 : 1;        
	  }

}
