import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;




public class DocumentsPerWord {
	public static Hashtable<String, ArrayList<String>> wordAndDocumentsListTable = new Hashtable<String, ArrayList<String>>();

	public static class Map extends MapReduceBase implements
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			FileSplit fileSplit = (FileSplit) reporter.getInputSplit();

			StringTokenizer itr = new StringTokenizer(value.toString());
			String cranNo = itr.nextToken();

			while (itr.hasMoreTokens()) {
				String currentWord = itr.nextToken();
				
				if(wordAndDocumentsListTable.containsKey(currentWord))
				{
					if(wordAndDocumentsListTable.get(currentWord).contains(cranNo))
					{
						//already counted in this file
					}
					else
					{
						wordAndDocumentsListTable.get(currentWord).add(cranNo);
						
						word.set(currentWord);
						output.collect(word, one);
					}
				}
				
				//first time appear
				else
				{
					wordAndDocumentsListTable.put(currentWord, new ArrayList<String>());
					wordAndDocumentsListTable.get(currentWord).add(cranNo);
					
					word.set(currentWord);
					output.collect(word, one);
				}
				
				
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;

			while (values.hasNext()) {
				sum = sum + values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf job = new JobConf(DocumentsPerWord.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		JobClient.runJob(job);
	}
}
