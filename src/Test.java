import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class Test {

	public static void test() {
		ArrayList<String> stopWordList = PureContent
				.buildStopWordList("D:\\stopwords.txt");
		Stemmer stemmer = new Stemmer();

		ArrayList<String> result = new ArrayList<String>();

		File f = new File("D:\\cranfield.txt");
		BufferedReader bf;

		try {
			bf = new BufferedReader(new FileReader(f));
			String str;
			try {
				while ((str = bf.readLine()) != null) {
					String cranNo = "";

					String pureContentString = "";

					for (int j = 0; j < 13; j++) {

						cranNo = cranNo + str.charAt(j);
					}

					String noHTMLString = str.replaceAll(cranNo, "");
					noHTMLString = noHTMLString.replaceAll("\\<.*?\\>", "");
					noHTMLString = noHTMLString.replaceAll("\\d", "");

					StringTokenizer tokenizer = new StringTokenizer(
							noHTMLString, "~!@#$%^&*()`[]{}|;':,./<>?-=_+\t\n ");

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
							pureContentString = pureContentString + " "
									+ lowercase;
						}

					}
					String element = cranNo + ":" + pureContentString;

					result.add(element);

					System.out.println(element);

				}
				System.out.println(result.size() + " == result");

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	public static void main(String[] args) {
		test();
	}

}
