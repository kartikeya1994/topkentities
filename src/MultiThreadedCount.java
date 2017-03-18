import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MultiThreadedCount 
{
	private static final int topEntities = 1000;
	private static final int batchSize = 100;
	static int maxLines = 1000000;
	static BufferedReader br = null;
	static FileReader fr = null;
      public static void main(String[] args) throws FileNotFoundException 
      {
    	  HashSet<String> dict = getTopEntities();
    	  fr = new FileReader("data_dump.txt");
  		  br = new BufferedReader(fr);
          ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(8);
          List<Future<List<Pair>>> resultList = new ArrayList<>();
          HashMap<String, Integer> finalResult = new HashMap<String, Integer>();
          boolean done = false;
           
          int count = 0;
          
          while(count<maxLines && !done){
        	  String text = "";
          for (int i=0; i<batchSize; i++)
          {
        	  count ++;
        	  String t = getNext();
        	  if(t == null)
        	  {
        		  done = true;
        		  break;
        	  }
        	  text = text + "\n" + t;
          }
          
          RegexWorker regexWorker  = new RegexWorker(dict, text,count);
          Future<List<Pair>> result = executor.submit(regexWorker);
          resultList.add(result);
          }
           
          for(Future<List<Pair>> future : resultList)
          {
                try
                {
                	merge(future,finalResult);
                    //System.out.println("Future result is - " + " - " + future.get() + "; And Task done is " + future.isDone());
                } 
                catch (InterruptedException | ExecutionException e) 
                {
                    e.printStackTrace();
                }
            }
            //shut down the executor service now
            executor.shutdown();
         // sort phrases by count
    		Map<String, Integer> sortedMap = sortByValue(finalResult);
    		//print the hashmap to file and stdout
    		print(sortedMap);
    		System.out.println("\n***********\nProcessed "+String.valueOf(count)+" texts");
      }
   static void merge(Future<List<Pair>> future, HashMap<String,Integer> finalResult) throws InterruptedException, ExecutionException
   {
	   List<Pair> f = future.get();
	   for (Pair p: f){	
		   if(finalResult.containsKey(p.e))
				finalResult.put(p.e, finalResult.get(p.e)+p.c);
			else
				finalResult.put(p.e, p.c);				
	}
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

class Pair
{
	public String e;
	public int c;
	
	public Pair(String e, int c){
		this.c = c;
		this.e = e;
	}
	
}

class RegexWorker implements Callable<List<Pair>>
{
	List<Pair> r ;
	HashMap<String,Integer> hashMap;
    private HashSet<String> dict;
    private String text;
    int id;
    public RegexWorker(HashSet<String> dict, String text, int id) {
        this.dict = dict;
        this.text = text;
        this.r = new LinkedList<Pair>();
        this.hashMap = new HashMap<String, Integer>();
        this.id = id;
    }
 
    @Override
    public List<Pair> call() throws Exception {
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
    	for (String entity: hashMap.keySet())	
			r.add(new Pair(entity,hashMap.get(entity)))	;				
		
    	if(id%1000==0)
  		  System.out.println(id);
        return r;
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
}