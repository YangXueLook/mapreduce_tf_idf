import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class OneWordLookUp {

	

	public static Hashtable<String, ArrayList<String>> keywordAndFileFreTable = new Hashtable<String, ArrayList<String>>();

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		Text keyWord = new Text();
		Text tfText = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString());
			
			// format: keyword + " in " + cranfieldxxxx + f
			String keyWordString = itr.nextToken();
			if(itr.hasMoreElements())
			{
				itr.nextToken();
				String cranNo = itr.nextToken();
								
				String f = (itr.nextToken());
				
				//existed keyword
				if(keywordAndFileFreTable.containsKey(keyWordString))
				{
					 ArrayList<String> content = keywordAndFileFreTable.get(keyWordString);
					 keywordAndFileFreTable.remove(keyWordString);
					 
					 content.add(cranNo+ " " + f);
					 
					
					keywordAndFileFreTable.put(keyWordString,content);
				}
				//first time
				else
				{
					ArrayList<String> content = new ArrayList<String>();
					content.add(cranNo+ " " + f);
					keywordAndFileFreTable.put(keyWordString,content);
				}
				
				
			}
			else
			//lookup element
			{
				
				
				Stemmer stemmer = new Stemmer();
				keyWordString = keyWordString.toLowerCase();
				
				if (!keyWordString.equals("")) {

					for (int j = 0; j < keyWordString.length(); j++) {
						stemmer.add(keyWordString.charAt(j));
					}

					stemmer.stem();
					keyWordString = stemmer.toString();
					
				}
				
				
				keyWord.set(keyWordString);
				
				tfText.set(keyWordString);

				
				output.collect(keyWord, tfText);
			}
			
			

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		Text keyWord = new Text();
		Text filenameAndFreText = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(key.toString());
			
			String keyWord = itr.nextToken();

			ArrayList<String> content = keywordAndFileFreTable.get(keyWord);
			
			for(int i = 0; i < content.size(); i++)
			{
				filenameAndFreText.set(content.get(i));

				output.collect(key, filenameAndFreText);
			}
			
			
			
			

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(OneWordLookUp.class);
		conf.setJobName("OneWordLookUp");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[1]), true);

		JobClient.runJob(conf);

	}
}
