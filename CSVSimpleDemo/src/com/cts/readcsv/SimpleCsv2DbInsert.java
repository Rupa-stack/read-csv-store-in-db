package com.cts.readcsv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

public class SimpleCsv2DbInsert {
	
	public static void main(String[] args) throws ClassNotFoundException {
		String jdbcURL = "jdbc:mysql://localhost:3306/sales";
		String username= "root";
		String password = "password-1";
		
		String csvFilePath = "C:\\Users\\t-rupa2\\Desktop\\data.csv";
		
		int batchSize=20;
		
		Connection connection = null;
		Class.forName("com.mysql.jdbc.Driver");
		try
		{
			connection = DriverManager.getConnection(jdbcURL,username,password);
			connection.setAutoCommit(false);
			
			String sql= "INSERT INTO review (course_name,student_name,tstamp,rating,comments) VALUES (?,?,?,?,?)";
			PreparedStatement statement = connection.prepareStatement(sql);
			
			BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
			String lineText = null;
			
			int count = 0;
			
			lineReader.readLine(); //skip header line
			
			while((lineText = lineReader.readLine()) != null) {
				String[] data = new String[100];
				data = lineText.split(",");
				//System.out.println(data[0]);
				String coursename = data[0];
				//System.out.println(data[1]);
				String studentName = data[1];
				String timestamp = data[2];
				String rating = data[3];
				String comment = data.length == 5? data[4]:"";
				
				statement.setString(1, coursename);
				statement.setString(2, studentName);
				
				Timestamp sqlTimestamp = Timestamp.valueOf(timestamp);
				statement.setTimestamp(3, sqlTimestamp);
				
				Float fRating = Float.parseFloat(rating);
				statement.setFloat(4, fRating);
				
				statement.setString(5, comment);
				
				statement.addBatch();
				
				if(count%batchSize ==0)
				{
					statement.executeBatch();
				}
			}
			lineReader.close();
			
			//execute remaining queries
			statement.executeBatch();
			connection.commit();
			connection.close();
		}
		catch(IOException | SQLException ex){
			ex.printStackTrace();
		}
	}

}
