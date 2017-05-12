/*
* Manage data input for the entity extractor
* When creating an object, pass the string 'DB' or 'FILE'
* as the source. 
*/

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import edu.stanford.nlp.io.IOUtils;

public class DataManager {
	private Connection connect = null;
	private Statement statement = null;
	private ResultSet resultSet = null;
	String source;

	// if source is FILE, will pick all text files in this folder
	private String folderName = "input";

	// if source is DB, will read from the following database and table
	final String colName = "body";
	final String dbName = "declassification_cables";


	List<String> fileNames = null;
	ListIterator<String> listIter = null;
	public final String FILE = "FILE";
	public final String DB = "DB";
	
	int offset;
	int batchSize;
	int init;
	int maxRows;
	boolean done;
	
	
	public DataManager(String source, int maxNumTexts)
	{
		this.source = source;
		offset = 0;
		batchSize = 25000;
		init = 0;
		maxRows = maxNumTexts;
		if(maxRows < batchSize)
			batchSize = maxRows;
		done = false;
	}
	
	public String getNextText() throws SQLException{
		/*
		* acts like a generator, pools batchSize number of results and returns 1
		* returns null when all texts have been read.
		*/
		//returns the next piece of text or null if reached end of source
//		if(done)
//			return null;
		if(init == 0 && !done){
			// initialize data source
			if(source.equals(FILE))
				initFiles();
			else
				connectDB();
			init = batchSize;
		}
		
		if(source.equals(FILE) && listIter.hasNext())
			return IOUtils.slurpFileNoExceptions(folderName+"/"+listIter.next());
			
		else if(source.equals(DB)){
			if(resultSet.next()){
				init--; // count down from batch size to 0
				return resultSet.getString(colName);
			}
			else{
				close(); //close DB stuff cleanly
			}
		}
		return null; // if source has no more data to offer
	}
	
	public void initFiles()
	{
		final File folder = new File(folderName);
		fileNames = getFileNames(folder);
		listIter = fileNames.listIterator();
	}
	
	public List<String> getFileNames(final File folder) {
		List<String> result = new LinkedList<String>();
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            result.addAll(getFileNames(fileEntry));
	        } else {
	            result.add(fileEntry.getName());
	        }
	    }
	    return result;
	}

	public void connectDB() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://history-lab.org/"+dbName+"?user=de_reader&password=XreadF403";
			connect = DriverManager.getConnection(url);
			
			statement = connect.createStatement();
			if(offset > maxRows)
				throw new Exception("Max rows exceeded");
			String q = "select body from declassification_cables.docs where type = \'TE\' and body not like \'%TELEGRAM TEXT FOR THIS MRN IS UNAVAILABLE%\' and body != \'\' order by id limit "+ String.valueOf(offset)+", "+String.valueOf(batchSize);
			//resultSet = statement.executeQuery("select "+colName+" from "+dbName+".docs order by id limit "+ String.valueOf(offset)+", "+String.valueOf(batchSize));
			resultSet = statement.executeQuery(q);
			offset += batchSize;
			if(offset >= maxRows)
				done = true;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void close() {

		try {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (connect != null) {
				connect.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}