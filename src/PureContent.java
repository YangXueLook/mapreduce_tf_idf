import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;
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

public class PureContent {
	public static ArrayList<String>  stopWordList = new ArrayList<String>();
	public static void initStopWords(){
		stopWordList.add("a");
		stopWordList.add("all");
		stopWordList.add("an");
		stopWordList.add("and");
		stopWordList.add("any");
		stopWordList.add("are");
		stopWordList.add("as");
		stopWordList.add("be");
		stopWordList.add("been");
		stopWordList.add("but");
		stopWordList.add("by ");
		stopWordList.add("few");
		stopWordList.add("for");
		stopWordList.add("have");
		stopWordList.add("he");
		stopWordList.add("her");
		stopWordList.add("here");
		stopWordList.add("him");
		stopWordList.add("his");
		stopWordList.add("how");
		stopWordList.add("i");
		stopWordList.add("in");
		stopWordList.add("is");
		stopWordList.add("it");
		stopWordList.add("its");
		stopWordList.add("many");
		stopWordList.add("me");
		stopWordList.add("my");
		stopWordList.add("none");
		stopWordList.add("of");
		stopWordList.add("on ");
		stopWordList.add("or");
		stopWordList.add("our");
		stopWordList.add("she");
		stopWordList.add("some");
		stopWordList.add("the");
		stopWordList.add("their");
		stopWordList.add("them");
		stopWordList.add("there");
		stopWordList.add("they");
		stopWordList.add("that ");
		stopWordList.add("this");
		stopWordList.add("us");
		stopWordList.add("was");
		stopWordList.add("what");
		stopWordList.add("when");
		stopWordList.add("where");
		stopWordList.add("which");
		stopWordList.add("who");
		stopWordList.add("why");
		stopWordList.add("will");
		stopWordList.add("with");
		stopWordList.add("you");
		stopWordList.add("your");

		
	}
	

	public static ArrayList<String> getPureContentList(String sourceText) {
		
		Stemmer stemmer = new Stemmer();
		
		

		ArrayList<String> result = new ArrayList<String>();

		String[] devidedStringArray = sourceText.split("</DOC>\n");
		
		System.out.println("devidedStringArray lenght === "+ devidedStringArray.length);

		for (int i = 0; i < devidedStringArray.length; i++) {
			String cranNo = "";

			String pureContentString = "";

			for (int j = 0; j < 13; j++) {

				cranNo = cranNo + devidedStringArray[i].charAt(j);
			}
			System.out.println("cranNo === "+ cranNo);
			String noHTMLString = devidedStringArray[i].replaceAll(cranNo, "");
			noHTMLString = noHTMLString.replaceAll("\\<.*?\\>", "");
			noHTMLString = noHTMLString.replaceAll("\\d", "");

			StringTokenizer tokenizer = new StringTokenizer(noHTMLString,
					"~!@#$%^&*()`[]{}|;':,./<>?-=_+\t\n ");

			String lowercase = "";
			while (tokenizer.hasMoreTokens()) {
				lowercase = tokenizer.nextToken().toLowerCase();

				if (stopWordList.contains(lowercase))
					lowercase = "";

				if (!lowercase.equals("")) {

					for (int j = 0; j < lowercase.length(); j++) {
						stemmer.add(lowercase.charAt(j));
					}

					stemmer.stem();
					lowercase = stemmer.toString();
					pureContentString = pureContentString + " " + lowercase;
				}

			}
			String element = cranNo + ":" + pureContentString;

			result.add(element);
		}

		return result;

	}

	public static ArrayList<String> buildStopWordList(String sourceFile) {

		File f = new File(sourceFile);

		BufferedReader bf;

		ArrayList<String> result = new ArrayList<String>();

		try {
			bf = new BufferedReader(new FileReader(f));
			String str;
			try {
				while ((str = bf.readLine()) != null) {
					result.add(str);
				}
				bf.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return result;
	}

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		Text cranfieldNo = new Text();
		Text content = new Text();

		String lowercase;

		Stemmer stemmer = new Stemmer();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			
			ArrayList<String> pureContentList = getPureContentList(value
					.toString());
			for (int i = 0; i < pureContentList.size(); i++) {
				// cranfieldNo:content
				// devidedNameAndContent.size == 2
				String[] devidedNameAndContent = pureContentList.get(i).split(
						":");
				cranfieldNo.set(devidedNameAndContent[0]);
				content.set(devidedNameAndContent[1]);
				output.collect(cranfieldNo, content);
				
				
			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				output.collect(key, values.next());
			}

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(PureContent.class);
		conf.setJobName("PureContent");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		FileSystem.get(conf).delete(new Path(args[1]), true);
		initStopWords();
		
		JobClient.runJob(conf);

	}

}