package com.employee_payrollJDBC;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.mysql.jdbc.Statement;

public class EmpPayroll {
	
    public static void main( String[] args ) {
    	
       Connection con = getSqlConnection();
       try {
    	   if(con != null) {
    		   //Insert a data
    		   String insertQuery = "INSERT INTO employee_payroll(name, gender, salary, startDate) VALUES (?,?,?,?)";
    		   PreparedStatement insertStatement = con.prepareStatement(insertQuery);
    		   insertStatement.setString(2, "Terisa");
    		   insertStatement.setString(3, "F");
    		   insertStatement.setInt(4, 20000);
    		   insertStatement.setDate(5, new Date(2021-07-13));
    		   
    		   int rowInserted = insertStatement.executeUpdate();
    		   if(rowInserted > 0){
    			   System.out.println("Data Inserted");
    		   }
    		   
    		   //Retrieve all the data
    		   String retrieveQuery = "SELECT * FROM employee_payroll";
    		   Statement statement = (Statement) con.createStatement();
    		   ResultSet resultSet = statement.executeQuery(retrieveQuery);
    		   
    		   while(resultSet.next()) {
    			   
    			   int id = resultSet.getInt("id");
    			   String name = resultSet.getString("name");
    			   String gender = resultSet.getString("gender");
    			   int salary = resultSet.getInt("salary");
    			   String startDate = resultSet.getDate("startDate").toString();
    			   
    			   String rowData = String.format("Id : %d \nName : %s \nGender : %s \nSalary : %d \nStartDate : %s", id, name, gender, salary,startDate);
    			   System.out.println(rowData);
    		   }
    		   
    		   //Update data with salary by person's name
    		   String updateQuery = "UPDATE employee_payroll SET salary = ? WHERE name = ?";
    		   PreparedStatement updateStatement = con.prepareStatement(updateQuery);
    		   updateStatement.setInt(1, 30000);
    		   updateStatement.setString(2, "Terisa");
    		   
    		   int rowUpdated = updateStatement.executeUpdate();
    		   if(rowUpdated > 0){
    			   System.out.println("Data Updated");
    		   }
    		   
    		   //Retrieve data using person's name 
    		   String selectQuery = "SELECT * FROM employee_payroll WHERE name = ?";
    		   PreparedStatement selectStatement = con.prepareStatement(selectQuery);
    		   selectStatement.setString(1, "Terisa");
    		   ResultSet resultSet1 = selectStatement.executeQuery();
    		   
    		   while(resultSet1.next()) {
    			   
    			   int id = resultSet1.getInt("id");
    			   String name = resultSet1.getString("name");
    			   String gender = resultSet1.getString("gender");
    			   int salary = resultSet1.getInt("salary");
    			   String startDate = resultSet1.getDate("startDate").toString();
    			   
    			   String rowData = String.format("Id : %d \nName : %s \nGender : %s \nSalary : %d \nStartDate : %s", id, name, gender, salary,startDate);
    			   System.out.println(rowData);
    		   }
		   
    		   //Retrieve employees who have joined in particular date range
    		   String selQuery = "SELECT * FROM employee_payroll WHERE startDate > ? AND startDate < ?";
    		   PreparedStatement selStatement = con.prepareStatement(selQuery);
    		   selStatement.setString(1, "2021-07-01");
    		   selStatement.setString(2, "2021-07-15");
    		   ResultSet resultData = selStatement.executeQuery();
    		   
    		   while(resultData.next()) {
    			   
    			   int id = resultData.getInt("id");
    			   String name = resultData.getString("name");
    			   String gender = resultData.getString("gender");
    			   int salary = resultData.getInt("salary");
    			   String startDate = resultData.getDate("startDate").toString();
    			   
    			   String rowData = String.format("Id : %d \nName : %s \nGender : %s \nSalary : %d \nStartDate : %s", id, name, gender, salary,startDate);
    			   System.out.println(rowData);
    		   }
    		   
    		   //use databaseFunction
    		   String selOpQuery = "SELECT SUM(salary) AS 'Sum',AVG(salary) AS 'Average',MIN(salary) AS 'Minimum',MAX(salary) AS 'Maximum',COUNT(id) AS 'Count' FROM employee_payroll WHERE gender = ? GROUP BY gender";
    		   PreparedStatement selOpStatement = con.prepareStatement(selOpQuery);
    		   selOpStatement.setString(1, "F");
    		   ResultSet resultOpreate = selOpStatement.executeQuery();
    		   
    		   while(resultOpreate.next()) {
    			   
    			   int Sum = resultOpreate.getInt("Sum");
    			   Float Average = resultOpreate.getFloat("Average");
    			   int Minimum = resultOpreate.getInt("Minimum");
    			   int Maximum = resultOpreate.getInt("Maximum");
    			   int Count = resultOpreate.getInt("Count");
    			   
    			   String rowData = String.format("Sum : %d \nAverage : %f \nMinimum : %d \nMaximum : %d \nCount : %s", Sum, Average, Minimum, Maximum,Count);
    			   System.out.println(rowData);
    		   }
    		   
    		   selOpQuery = "SELECT SUM(salary) AS 'Sum',AVG(salary) AS 'Average',MIN(salary) AS 'Minimum',MAX(salary) AS 'Maximum',COUNT(id) AS 'Count' FROM employee_payroll WHERE gender = ? GROUP BY gender";
    		   selOpStatement = con.prepareStatement(selOpQuery);
    		   selOpStatement.setString(1, "M");
    		   resultOpreate = selOpStatement.executeQuery();
    		   
    		   while(resultOpreate.next()) {
    			   
    			   int Sum = resultOpreate.getInt("Sum");
    			   Float Average = resultOpreate.getFloat("Average");
    			   int Minimum = resultOpreate.getInt("Minimum");
    			   int Maximum = resultOpreate.getInt("Maximum");
    			   int Count = resultOpreate.getInt("Count");
    			   
    			   String rowData = String.format("Sum : %d \nAverage : %f \nMinimum : %d \nMaximum : %d \nCount : %s", Sum, Average, Minimum, Maximum,Count);
    			   System.out.println(rowData);
    		   }
    	   }
    	   
       }catch(SQLException sqlException) {
    	   System.out.println(sqlException.getMessage());
       }finally {
    	   
    	   if(con != null) {
    		   try {
    			   con.close();
    		   }catch(SQLException sqlException) {
    	    	   System.out.println(sqlException.getMessage());
    		   }
    	   }
       }
    }

	private static Connection getSqlConnection() {
		Connection con = null;
	       try {
	    	   
	    	   String dbHostUrl = "jdbc:mysql://localhost:3306/payroll_service?useSSL=false";
	    	   String userName = "root";
	    	   String passWord = "vismaya";
	    	   
	    	   con = DriverManager.getConnection(dbHostUrl, userName, passWord);
	    	   
	    	   if(con != null) {
	    		   System.out.println("Connection is Established");
	    	   }
	    	   
	       }catch(SQLException sqlException) {
	    	   System.out.println(sqlException.getMessage());
	       }
		return con;
	}
}
