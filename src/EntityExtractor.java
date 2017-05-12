/*
Extract entities from a given text and count their occurences
*/
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.tokensregex.TokenSequenceMatcher;
import edu.stanford.nlp.ling.tokensregex.TokenSequencePattern;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class EntityExtractor {
	private static final String outputFile = "result.txt";
	private static final String source = "DB"; //where to get texts from: FILE or DB
	private static final boolean print_to_stdout = false;
	//number of iterations to trim hash table after
	// hash table can grow too large and exceed memory
	private static final int trimThreshold = 30000; 
	private static final int maxNumTexts = 210000;
	private static final int trimmedSize = 1000000; // size of hash table after trimming
	
	static BufferedWriter bw_dd = null;
	static FileWriter fw_dd = null;
	
	static final String firstWord = "([{tag:/NN.*|JJ.*|CD/}]&/[A-Z0-9].*/)";
	static final String middleWords = "([({tag:/CC|IN/})|(!{tag:/VB.*|MD|W.*/}&!{word:/(,|;|[a-z]*)/})]){0,3}";
	static final String lastWord = firstWord;
	/*
	 * Differences between case and no case:
	 * i/FW am/VBP currently/RB living/VBG in/IN the/DT united/VBN states/NNS of/IN america/NN
	 * Must account for VBN and VBD
	 */
	static final String firstWordNoCase = "([{tag:/NN.*|JJ.*|CD|VBN|VBD/}&!{word:/,|;|\\**/}])";
	static final String middleWordsNoCase = "([{tag:/CC|IN/}|(!{tag:/VB|VBG|VBP|VBZ|MD|W.*/}&!{word:/,|;|\\**/})]){0,3}";
	static final String lastWordNoCase = firstWordNoCase;
	
	static final String exclude = "&!{word:/[A-Za-z][\\.]|Mr\\.|Dr\\.|Mrs\\.|Ms\\.|[Jj]r[\\.]*|[Dd]e|[Hh]on[\\.]*|[Rr]ev[\\.]*|[Ss]t[\\.]*/}";
	static final String singleProperNoun = "[{tag:/PRP.*/}]*[{tag:/NNP.*/}]+[{tag:/CD/}]?";
	static final String regex = firstWord + middleWords + lastWord+"|"+singleProperNoun;
	//static final String regex = firstWordNoCase + middleWordsNoCase + lastWordNoCase;
	static Set<String> months = new TreeSet<String>();
	
	
	
	private static int hashTableSize = 0;
	
	public static void main(String[] args) throws IOException, SQLException {
		
		fw_dd = new FileWriter("data_dump.txt");
		bw_dd = new BufferedWriter(fw_dd);
		
		months.add("January");
		months.add("February");
		months.add("March");
		months.add("April");
		months.add("May");
		months.add("June");
		months.add("July");
		months.add("August");
		months.add("September");
		months.add("October");
		months.add("November");
		months.add("December");
		months.add("Jan");
		months.add("Feb");
		months.add("Mar");
		months.add("Apr");
		months.add("May");
		months.add("Jun");
		months.add("Jul");
		months.add("Aug");
		months.add("Sep");
		months.add("Sept");
		months.add("Oct");
		months.add("Nov");
		months.add("Dec");
		long startTime = System.nanoTime();
		String text;
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, pos");
		//StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
		//store entities and counts in hash map
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
		System.out.println("here");
		DataManager dman = new DataManager(source, maxNumTexts);
		int count = 0;
		text = dman.getNextText();
		while(text != null && count<maxNumTexts){
			//System.out.println("here");
			print(text.toLowerCase());
			count++;
			//getEntities(text, pipeline, hashMap);
			text = dman.getNextText();
			if(count%1000 == 0)
				System.out.println(count); //print status
			if(count%trimThreshold == 0 && hashTableSize >= trimmedSize)
			{
				hashMap = trim(hashMap);
				System.out.println("Trimming hash map");
			}
		}
		// sort phrases by count
		Map<String, Integer> sortedMap = sortByValue(hashMap);
		//print the hashmap to file and stdout
		print(sortedMap);
		System.out.println("\n***********\nProcessed "+String.valueOf(count)+" texts");
		long endTime = System.nanoTime();
		System.out.println("Took "+(endTime - startTime)/1000000000 + "s"); 
	}	
	
	public static void getEntities(String text, StanfordCoreNLP pipeline, HashMap<String, Integer> hashMap)
	{
		Annotation annotation = new Annotation(text);
		pipeline.annotate(annotation);
		List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
		if (sentences != null && !sentences.isEmpty()) {
			for (CoreMap s : sentences) {
				List<CoreLabel> tokens = s.get(CoreAnnotations.TokensAnnotation.class);
				matchRegex(regex, tokens, hashMap);
				//matchRegex(singleProperNoun, tokens, hashMap);
			}
		}
	}
	
	public static void matchRegex(String regex, List<CoreLabel> tokens, HashMap<String, Integer> hashMap)
	{
		TokenSequencePattern pattern = TokenSequencePattern.compile(regex);				
		TokenSequenceMatcher matcher = pattern.getMatcher(tokens);
		while (matcher.find()) {
		     String matchedString = matcher.group();
		     //update hashmap count corresponding to matched string
		     if(hashMap.containsKey(matchedString))
		    	 hashMap.put(matchedString, hashMap.get(matchedString)+1);
		     else{
		    	 hashMap.put(matchedString, new Integer(1));
		    	 hashTableSize ++;
		     }
		}
	}
	
	public static HashMap<String, Integer> trim(HashMap<String, Integer> map)
	{
		Map<String, Integer> sorted = sortByValue(map);
		HashMap<String, Integer> trimmed = new HashMap<String, Integer>();
		int count = 0;
		for (String entity: sorted.keySet())
		{
			trimmed.put(entity, map.get(entity));
			count ++;
			if(count>trimmedSize)
				break;
		}
		return trimmed;
	}
	
	// custom comparator to sort the hashmap
	public static Map<String, Integer> sortByValue( Map<String, Integer> map ){
		List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<String, Integer>>()
		{
			public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
			{
				return (o2.getValue()).compareTo( o1.getValue() );
			}
		} );

		Map<String, Integer> result = new LinkedHashMap<String, Integer>();
		for (Map.Entry<String, Integer> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}
	
	public static boolean isInformative(String s)
	{
		if(months.contains(s.split(" ")[0]))
			return false;
		if(s.length() == 2 && s.charAt(1)=='.')
			return false;
		if(s.length() == 3 && s.charAt(2)=='.')
			return false;
		return true;
	}
	
	public static void print(Map<String, Integer> map) {
		//write the key value pairs of map to file and stdout
		BufferedWriter bw = null;
		FileWriter fw = null;
		String line;
		try {
			fw = new FileWriter(outputFile);
			bw = new BufferedWriter(fw);
			for (Object entity: map.keySet()){
				if(isInformative((String)entity))
				{	
					line = entity + " = " + String.valueOf(map.get(entity));
					bw.write(line);
					bw.write("\n");
					if(print_to_stdout)
						System.out.println(line);
				}
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	
	public static void print(String t) {
		
		try {
			
			bw_dd.write(t);
			bw_dd.write("\n");
			if(print_to_stdout)
				System.out.println(t);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
