package project.ee6889;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.rds.AmazonRDSClient;
import com.amazonaws.services.rds.model.DBInstance;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
public class test {
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		
			// defining MySQL connection strings, and constants
			final String USERNAME="";
			final String PASSWORD="";
			final String DB_NAME="stream";
			final String HOSTNAME = "";
			final Integer PORT = 3306;
			final String HOME = System.getProperty("user.home");
			final String PATH_SEP = System.getProperty("file.separator");
			final String PROJECT_DIR = HOME + PATH_SEP + "workspace" + PATH_SEP + "YoutubeStreams" + PATH_SEP + "data" + PATH_SEP;
			final String DB_VIDEO_DATA =  PROJECT_DIR + "db_video_data.txt";
			final String DB_TREND_DATA =  PROJECT_DIR + "db_trend_data.txt";
			final String DB_GET_DONE = PROJECT_DIR + "db_get_done.txt";
			final String PROCESSED_DONE = PROJECT_DIR + "processed_done.txt";
			final String PROCESSED_RESULT = PROJECT_DIR + "processed_result.txt";
			
			// declare a filewriter variable
			OutputStreamWriter fileWriter = null;
			FileReader fileReader = null;
			String jdbcUrl = null;

			// creating the sql connection string
			jdbcUrl = "jdbc:mysql://" + HOSTNAME + ":" + PORT + "/" + DB_NAME + "?user=" + USERNAME + "&password=" + PASSWORD;
			Connection connection = null;
			Statement sqlStatement = null;
			ResultSet resultSet = null;			
			
			// begin the database connection strings
			try {
				connection = getConnection(jdbcUrl);
				sqlStatement = connection.createStatement();
				
				resultSet = sqlStatement.executeQuery("SELECT * FROM processing_tags");
				
				// keep looping until bit is set	
				//int i = 0;
				resultSet.next();
				int bit = resultSet.getInt("ready_to_process");
				System.out.println("Ready to process is initially " + bit);
				resultSet.close();				
				while(bit != 0) {					
					resultSet = sqlStatement.executeQuery("SELECT * FROM processing_tags");
					resultSet.next();
					bit = resultSet.getInt("ready_to_process");
					resultSet.close();
				}			
				sqlStatement.close();
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			try {
				//Call getConnection to get a connection to the database. Change the URL parameters
				connection = getConnection(jdbcUrl);
	
				//Creating a statement object
				sqlStatement = connection.createStatement();
	
				// make sql statement to read from video trend table
				resultSet = sqlStatement.executeQuery("SELECT * FROM video_trend");
				
				// create file to store video trend meta-data to be passed to streams
				FileOutputStream file = new FileOutputStream(DB_TREND_DATA);
				try {
					fileWriter = new OutputStreamWriter(file, "UTF-8");
				} catch (IOException ex) {
					ex.printStackTrace();
					System.out.println("Do you have permission to write to the db_data file? ");
				}
	
				//Iterating the resultset and printing the 3rd column
				while (resultSet.next()) {
					fileWriter.write(resultSet.getString("video_id")+ ";");
					fileWriter.write(resultSet.getString("acquire_time") + ";");
					fileWriter.write(resultSet.getString("title") + ";");
					fileWriter.write(resultSet.getString("publishAt")+ ";");	
					fileWriter.write(resultSet.getInt("viewCount") + ";");
					fileWriter.write(resultSet.getInt("likeCount") + ";");
					fileWriter.write(resultSet.getInt("dislikeCount"));				
					fileWriter.write("\n");
	
				}				

				fileWriter.flush();
				fileWriter.close();
				file.close();
				
				// read data from the video table
				resultSet = sqlStatement.executeQuery("SELECT * FROM video");				
				file = new FileOutputStream(DB_VIDEO_DATA);
				try {
					fileWriter = new OutputStreamWriter(file, "UTF-8");
				} catch (IOException ex) {
					ex.printStackTrace();
					System.out.println("Do you have permission to write to the db_data file? ");
				}
	
				//Iterating the resultset and printing the 3rd column
				while (resultSet.next()) {
					fileWriter.write(resultSet.getString("video_id")+ ";" +
					resultSet.getString("title")+ ";" +
					resultSet.getString("publishedAt") + ";" +
					resultSet.getString("publishedAtInseconds") + ";" +
					resultSet.getString("picture_url") + ";" +
					resultSet.getInt("viewCount") + ";" +
					resultSet.getInt("likeCount") + ";" +
					resultSet.getInt("dislikeCount") + ";" +
					resultSet.getString("duration") + ";" +
					resultSet.getString("topicIds")						
					+"\n");
	
				}
				//close the resultset, statement, connection and file writer.
				try {
					fileWriter.flush();
					fileWriter.close();
					file.close();
				} catch(IOException e) {
					System.out.println("There must be a problem flushing and closing the file");
				}
				resultSet.close();
				sqlStatement.close();
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			} 
			System.out.println("The Database results for video and video_trend have been written to file");
			
			// write to file db_get_done.txt
			try {
				fileWriter = new FileWriter(DB_GET_DONE);
				fileWriter.write("1", 0, 1);
				fileWriter.flush();
				fileWriter.close();
			} catch(IOException ex) {
				System.out.println(ex.toString());
			}
			
			// create file handle to check if processing is done in streams
			fileReader = new FileReader(PROCESSED_DONE);
			
			// loop test variable i, Check if the bit is set (49 unicode for 1)
			// and break out of loop if set
			int i = fileReader.read();
			fileReader.close();
			while(i != 49) {
				fileReader = new FileReader(PROCESSED_DONE);
				i = fileReader.read();
				fileReader.close();
			}
			
			// push processed result to AWS database
			//FileOutputStream file = new FileOutputStream(PROCESSED_RESULT);
				connection = getConnection(jdbcUrl);
				try {
					
					PreparedStatement sqlstatement = null;
					Statement sql = null;
					sql = connection.createStatement();
					//connection.execute("TRUNCATE video");
					BufferedReader br = new BufferedReader(new FileReader(PROCESSED_RESULT));
					String line;
					sql.executeUpdate("truncate video_trend_results");
					sql.close();
					
					// remove bad characters that cause error in sql
					while((line=br.readLine()) != null) {
						String[] value = line.split(";");
                        value[1]=value[1].replace("'","");
                        value[1]=value[1].replace(")","");
                        value[0]=value[1].replace("'","");
                        value[0]=value[1].replace(")",""); 
                        value[2]=value[1].replace("'","");
                        value[2]=value[1].replace(")","");                       
						sqlstatement = connection.prepareStatement("INSERT INTO video_trend_results VALUES ('"+value[0]+"','"+value[1]+"','"+value[2]+"')");
						sqlstatement.executeUpdate();
					}
					
					// state test for transition
					System.out.println("Printing the content of ready_to_display before");
					resultSet = sqlStatement.executeQuery("SELECT * FROM processing_tags");
					while(resultSet.next()) {
						System.out.println(resultSet.getString("ready_to_display"));
					}
					
					sqlStatement.executeUpdate("UPDATE processing_tags SET ready_to_display=1");
					System.out.println("\n");
					System.out.println("Printing the content of ready_to_display after");
					resultSet = sqlStatement.executeQuery("SELECT * FROM processing_tags");
					while(resultSet.next()) {
						System.out.println(resultSet.getString("ready_to_display"));
					}
					br.close();
					resultSet.close();
					sqlStatement.close();
					connection.close();
				} catch(SQLException ex) {
					ex.printStackTrace();
				}
			
			// initialize check out files
			try {
				fileWriter = new FileWriter(PROCESSED_DONE);
				fileWriter.write("0", 0, 1);
				fileWriter.flush();
				fileWriter.close();
				fileWriter = new FileWriter(DB_GET_DONE);
				fileWriter.write("0", 0, 1);
				fileWriter.flush();
				fileWriter.close();
			} catch(IOException exc) {
				exc.printStackTrace();
			}
	
	} // End of the main class for test class
	
	
	// method to create connection
	public static Connection getConnection(String jdbcurl) {
		Connection connect = null;
		try {
		// loading the jdbc driver
		Class.forName("com.mysql.jdbc.Driver");

		//Getting a connection to the database. Change the URL parameters
		
		connect = DriverManager.getConnection(jdbcurl);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return connect;
	}

} // End of the Test Class
