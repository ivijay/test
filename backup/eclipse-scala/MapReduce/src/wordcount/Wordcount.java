package wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.*;




public class Wordcount {

	/**
	 * @param args
	 */


	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		 @SuppressWarnings("deprecation")
		Job job = new Job(conf,"wordcount");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setJarByClass(Wordcount.class);
		job.setMapperClass(WordcountMapper.class);
	//	job.setCombinerClass(Reduce.class);
		job.setReducerClass(WordcountReducer.class);
		
//		job.setInputFormatClass(TextInputFormat.class);
		job.setInputFormatClass(ParagraphInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}