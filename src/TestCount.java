import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class TestCount {
	private static final int maxNumTexts = 2000000;
	private static final int topEntities = 1000;
	static BufferedReader br = null;
	static FileReader fr = null;
	public static void main(String args[]) throws SQLException, FileNotFoundException{
		fr = new FileReader("data_dump.txt");
		br = new BufferedReader(fr);
		
		
		matchString("un dos hi my un dos. ha name is kartikeya un dos.","un dos");
		long startTime = System.nanoTime();
		String text;
		//store entities and counts in hash map
		HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
		HashSet<String> dict = getTopEntities();
		
		//DataManager dman = new DataManager("DB", maxNumTexts);
		int count = 0;
		text =  getNext(); //dman.getNextText();
		while(text != null && count<maxNumTexts){
			//text = text.toLowerCase();
			//System.out.println(text);
			count++;
			countEntities(text, dict, hashMap);
			text = getNext();//dman.getNextText();
			if(count%100 == 0)
				System.out.println(count); //print status
		}
		// sort phrases by count
		Map<String, Integer> sortedMap = sortByValue(hashMap);
		//print the hashmap to file and stdout
		print(sortedMap);
		System.out.println("\n***********\nProcessed "+String.valueOf(count)+" texts");
		long endTime = System.nanoTime();
		System.out.println("Took "+(endTime - startTime)/1000000000 + "s");
	}
	
	static String getNext()
	{
		String next = null;
		try {
			next = br.readLine();

		} catch (IOException e) {

			e.printStackTrace();

		} 
		return next;
	}
	
	static int matchString(String text, String s)
	{
		//return StringUtils.countMatches(text,s);
		s = s.trim();
		String s1 = s+".?";
		String s2 = s+".$";
		s = "^"+s1+" | "+s1+" | "+s2;
		final Pattern p = Pattern.compile(s);
        Matcher  matcher = p.matcher(text);

        int count = 0;
        while (matcher.find())
            count++;
        return count;
	}
	
	static void countEntities(String text, HashSet<String> dict, HashMap<String,Integer> hashMap){
		for(String entity:dict)
		{
			int count = matchString(text,entity);
			if(count>0)
			{
				if(hashMap.containsKey(entity))
					hashMap.put(entity, hashMap.get(entity)+count);
				else
					hashMap.put(entity, new Integer(1));
			}
		}
		
	}
	
	static void test(String text, String s){
		int count = StringUtils.countMatches(text,s);
//			int lastIndex = 0;
//			int count = 0;
//			int l = s.length();
//			while(lastIndex != -1){
//			    lastIndex = text.indexOf(s,lastIndex);
//
//			    if(lastIndex != -1){
//			        count ++;
//			        lastIndex += l;
//			    }
//			}
			System.out.println(count);
		
	}
	
	static HashSet<String> getTopEntities() throws FileNotFoundException
	{
		HashSet<String> hs = new HashSet<String>();
		final Scanner s = new Scanner(new File("dict.txt"));
		for(int i=0;i<topEntities; i++) {
		    String line = s.nextLine();
		    String[] parts = line.split(" = ");
		    //System.out.println(parts[0].trim().toLowerCase());
		    hs.add(parts[0].trim().toLowerCase());
		}
		s.close();
		return hs;
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
		public static void print(Map<String, Integer> map) {
			boolean print_to_stdout = false;
			//write the key value pairs of map to file and stdout
			BufferedWriter bw = null;
			FileWriter fw = null;
			String line;
			try {
				fw = new FileWriter("test_result.txt");
				bw = new BufferedWriter(fw);
				for (Object entity: map.keySet()){	
						line = entity + " = " + String.valueOf(map.get(entity));
						bw.write(line);
						bw.write("\n");
						if(print_to_stdout)
							System.out.println(line);					
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
}

class CountTask extends Thread{
	HashSet<String> dict;
	public CountTask(HashSet<String> dict)
	{
		this.dict = dict;
	}
	  public void run(){  
		    System.out.println("My thread is in running state.");  
		  }
}
