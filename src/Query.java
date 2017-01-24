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

public class Query {

	public static Hashtable<String, Double> file_keywordAndWeightTable = new Hashtable<String, Double>();

	public static Hashtable<String, Double> keywordAndIdfTable = new Hashtable<String, Double>();

	public static Hashtable<String, Double> fileAndDistanceSquareTable = new Hashtable<String, Double>();

	public static ArrayList<String> fileNameList = new ArrayList<String>();

	public static ArrayList<String> stopWordList = new ArrayList<String>();

	public static void initStopWords() {
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

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		Text keyWord = new Text();
		Text queryContentText = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString());

			String firstWord = itr.nextToken();
			if (!firstWord.contains("Query")) {
				if (firstWord.contains("cranfield")
						&& (firstWord.length() == 13)) {
					// weight record; format: cranfieldxxxx + keyword +
					// weightValue

					String cranNo = firstWord;

					String keywordString = itr.nextToken();

					String weightValue = itr.nextToken();

					file_keywordAndWeightTable.put(
							cranNo + " " + keywordString,
							Double.parseDouble(weightValue));

					fileNameList.add(cranNo);

				}

				else
				// idf record; format: keyword + idf
				{
					String keywordString = firstWord;
					String idfValue = itr.nextToken();

					keywordAndIdfTable.put(keywordString,
							Double.parseDouble(idfValue));
				}

			}

			else
			// format: Query1 aaa bbb ccc ddd
			{

				String queryX = firstWord;

				String queryWords = "";

				Stemmer stemmer = new Stemmer();

				while (itr.hasMoreTokens()) {
					String currentWord = itr.nextToken().toLowerCase();

					if (!stopWordList.contains(currentWord)) {
						for (int j = 0; j < currentWord.length(); j++) {
							stemmer.add(currentWord.charAt(j));
						}

						stemmer.stem();
						currentWord = stemmer.toString();

						queryWords = queryWords + " " + currentWord;
					}

				}

				keyWord.set(queryX);

				queryContentText.set(queryWords);

				output.collect(keyWord, queryContentText);
			}

		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		Text keyWord = new Text();
		Text similarityText = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// first calculate |D| for each file store in fileAndDistanceTable

			

			double DFileSquare = 0;

			for (Iterator it2 = file_keywordAndWeightTable.keySet().iterator(); it2
					.hasNext();) {

				String file_keyword = (String) it2.next();

				StringTokenizer itr3 = new StringTokenizer(file_keyword);

				String fileName = itr3.nextToken();

				

				double weightOnWord = file_keywordAndWeightTable
						.get(file_keyword);

			

				DFileSquare = weightOnWord * weightOnWord;
				if (fileAndDistanceSquareTable.containsKey(fileName)) {
					double value = fileAndDistanceSquareTable.get(fileName);
					fileAndDistanceSquareTable.remove(fileName);

					value = value + DFileSquare;

					fileAndDistanceSquareTable.put(fileName, value);
					

				}

				else {
					fileAndDistanceSquareTable.put(fileName, DFileSquare);

				}

			}

			StringTokenizer itr = new StringTokenizer(key.toString());

			String queryX = itr.nextToken();
			
			while (values.hasNext()) {
				
				
				StringTokenizer itr2 = new StringTokenizer(values.next()
						.toString());
				// keywords list in query
				ArrayList<String> wordsInQuery = new ArrayList<String>();

				while (itr2.hasMoreElements()) {
					String currentWordInQuery = itr2.nextToken();
					wordsInQuery.add(currentWordInQuery);
				}
				

				Hashtable<String, Integer> keywordAndTf = new Hashtable<String, Integer>();

				for (int i = 0; i < wordsInQuery.size(); i++) {
					if (keywordAndTf.containsKey(wordsInQuery.get(i)))
					// existed
					{
						int tf = keywordAndTf.get(wordsInQuery.get(i));
						keywordAndTf.remove(wordsInQuery.get(i));
						tf++;
						keywordAndTf.put(wordsInQuery.get(i), tf);

					} else
					// fist time
					{
						keywordAndTf.put(wordsInQuery.get(i), 1);
					}
				}

				// System.out.println(keywordAndTf.size()
				// + "keywordAndTf SSSSSSSSSSSS");

				// max f in query
				int fmax = 0;

				for (Iterator it2 = keywordAndTf.keySet().iterator(); it2
						.hasNext();) {
					String term = (String) it2.next();
					int f = keywordAndTf.get(term);
					if (fmax < f)
						fmax = f;
				}

				// System.out.println("fmax ????????????? " + fmax);

				Hashtable<String, Double> query_KeywordAndTf = new Hashtable<String, Double>();
				// calculate tf
				// key: QueryX + " " + term; value: tf
				for (Iterator it2 = keywordAndTf.keySet().iterator(); it2
						.hasNext();) {
					String term = (String) it2.next();
					int f = keywordAndTf.get(term);
					double tf = (double) (f) / (double) (fmax);
					query_KeywordAndTf.put(queryX + " " + term, tf);
				}

				// System.out.println(query_KeywordAndTf.size()
				// + "query_KeywordAndTf SSSSSSSSSSSS");

				// calculate weight
				// key: QueryX + " " + term; value: weight
				Hashtable<String, Double> query_KeywordAndWeight = new Hashtable<String, Double>();

				for (Iterator it2 = query_KeywordAndTf.keySet().iterator(); it2
						.hasNext();) {

					String queryX_term = (String) it2.next();

					StringTokenizer itr3 = new StringTokenizer(queryX_term);
					itr3.nextToken();
					String currentTerm = itr3.nextToken();

					

					double tf = query_KeywordAndTf.get(queryX_term);

					// System.out.println("tf ????????????? " + tf);

					double idf = 0;
					if (keywordAndIdfTable.containsKey(currentTerm)) {
						idf = keywordAndIdfTable.get(currentTerm);
					}

					// System.out.println("idf ????????????? " + idf);

					double weight = tf * idf;

					query_KeywordAndWeight.put(queryX_term, weight);

				}

				// query distance
				double DQuerySquare = 0;
				for (Iterator it2 = query_KeywordAndWeight.keySet().iterator(); it2
						.hasNext();) {
					String queryX_term = (String) it2.next();

					double queryTermWeight = query_KeywordAndWeight
							.get(queryX_term);

					DQuerySquare = DQuerySquare + queryTermWeight
							* queryTermWeight;
				}
				double DQuery = Math.sqrt(DQuerySquare);

				// calculate sigma Wij*Wiq
				// key: QueryX + " " + cranNO; value: sigmaW

				

				//
				Hashtable<String, Double> query_filenameAndSigmaWW = new Hashtable<String, Double>();

				for (int i = 0; i < fileNameList.size(); i++) {
					double sigmaWInOneFile = 0;
					String currentFile = fileNameList.get(i);
					for (Iterator it2 = query_KeywordAndWeight.keySet()
							.iterator(); it2.hasNext();) {

						// keyword weight in file?

						String queryX_term = (String) it2.next();

						StringTokenizer itr3 = new StringTokenizer(queryX_term);
						itr3.nextToken();
						String keyword = itr3.nextToken();

						String keywordInWeightTable = currentFile + " "
								+ keyword;

						double termWeightInOneFile = 0;

						double queryTermWeight = query_KeywordAndWeight
								.get(queryX_term);

						if (file_keywordAndWeightTable
								.containsKey(keywordInWeightTable)) {
							termWeightInOneFile = file_keywordAndWeightTable
									.get(keywordInWeightTable);
						}

						double sigmaWWOneFileOneWord = queryTermWeight
								* termWeightInOneFile;

						sigmaWInOneFile = sigmaWInOneFile
								+ sigmaWWOneFileOneWord;
					}

					// sigma/dd

					Double similarity = sigmaWInOneFile
							/ (DQuery * Math.sqrt(fileAndDistanceSquareTable
									.get(currentFile)));

					keyWord.set(queryX + " " + currentFile);
					similarityText.set(similarity.toString());

					

					output.collect(keyWord, similarityText);
				}

				

			}

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Query.class);
		conf.setJobName("Query");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);

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
