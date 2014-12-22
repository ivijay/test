package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
//		System.out.println("key: "+key);
//		LongWritable l = new LongWritable(0);
//		System.out.println("key:"+key);
//		System.out.println("l:"+l);
//		if(key.get() == new LongWritable(0).get()) {
//			System.out.println("1st line:" + value);
//			
//		}
		if(value.getLength()==2){
			
			System.out.println("0"+(int)value.charAt(0));
			System.out.println("1"+(int)value.charAt(1));
		}
		String line = value.toString();
		System.out.println("in mapper:"+line);
		StringTokenizer tokenizer = new StringTokenizer(line," ");
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			context.write(word,one);							
		}
	}
}