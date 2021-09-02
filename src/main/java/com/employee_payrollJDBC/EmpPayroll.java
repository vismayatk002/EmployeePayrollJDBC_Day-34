package com.employee_payrollJDBC;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Scanner;

import com.mysql.jdbc.Statement;

public class EmpPayroll {
	
	public void insertData(Connection con) throws SQLException {
		
		//Insert a data
		if(con != null) {
	
			String insertQuery = "INSERT INTO emp_payroll(name, gender, salary, startDate) VALUES (?,?,?,?)";
			PreparedStatement insertStatement = con.prepareStatement(insertQuery);
			insertStatement.setString(1, "Terisa");
			insertStatement.setString(2, "F");
			insertStatement.setInt(3, 20000);
			insertStatement.setDate(4, new Date(2021-07-13));
			   
			int rowInserted = insertStatement.executeUpdate();
			if(rowInserted > 0){
				System.out.println("Data Inserted");
			}
		}
	}
	
	public void readAllData(Connection con) throws SQLException {
	
		//Retrieve all the data
		if(con != null) {
		   String retrieveQuery = "SELECT * FROM emp_payroll";
		   Statement statement = (Statement) con.createStatement();
		   ResultSet resultSet = statement.executeQuery(retrieveQuery);
		   
		   while(resultSet.next()) {
			   
			   int id = resultSet.getInt("id");
		   String name = resultSet.getString("name");
		   String gender = resultSet.getString("gender");
		   int salary = resultSet.getInt("salary");
		   String startDate = resultSet.getDate("startDate").toString();
		   
		   String rowData = String.format("\nId : %d \nName : %s \nGender : %s \nSalary : %d \nStartDate : %s", id, name, gender, salary,startDate);
		   System.out.println("Data retrieved " + rowData);
		   }
		}
	}
	
	public void updateDataByName(Connection con) throws SQLException {
		
		//Update data with salary by person's name
		if(con != null) {
			
			String updateQuery = "UPDATE emp_payroll SET salary = ? WHERE name = ?";
			PreparedStatement updateStatement = con.prepareStatement(updateQuery);
			updateStatement.setInt(1, 30000);
			updateStatement.setString(2, "Terisa");
		   
			int rowUpdated = updateStatement.executeUpdate();
			if(rowUpdated > 0){
			   System.out.println("Data Updated");
		   }
		}
	}
	
	public void readDataByName(Connection con) throws SQLException {
		
		//Retrieve data using person's name
		if(con != null) {
			
			String selectQuery = "SELECT * FROM emp_payroll WHERE name = ?";
 		   	PreparedStatement selectStatement = con.prepareStatement(selectQuery);
 		   	selectStatement.setString(1, "Terisa");
 		   	ResultSet resultSet1 = selectStatement.executeQuery();
 		   
 		   	while(resultSet1.next()) {
 			   
 			   int id = resultSet1.getInt("id");
 			   String name = resultSet1.getString("name");
 			   String gender = resultSet1.getString("gender");
 			   int salary = resultSet1.getInt("salary");
 			   String startDate = resultSet1.getDate("startDate").toString();
 			   
 			   String rowData = String.format("\nId : %d \nName : %s \nGender : %s \nSalary : %d \nStartDate : %s", id, name, gender, salary,startDate);
 			   System.out.println("Data read by name" + rowData);
 		   	}
		}
	}
	
	public void readDataByDateRange(Connection con) throws SQLException {
		
		//Retrieve employees who have joined in particular date range
		if(con != null) {
			
			String selQuery = "SELECT * FROM emp_payroll WHERE startDate > ? AND startDate < ?";
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
 			   
 			   String rowData = String.format("\nId : %d \nName : %s \nGender : %s \nSalary : %d \nStartDate : %s", id, name, gender, salary,startDate);
 			   System.out.println(rowData);
 		   	}
		}
	}
	
	public void databaseFunction(Connection con) throws SQLException {
		
		if(con != null) {
			Scanner sc = new Scanner(System.in);
			System.out.print("Enter gender : ");
			String gen = sc.nextLine();
			String selOpQuery = "SELECT SUM(salary) AS 'Sum',AVG(salary) AS 'Average',MIN(salary) AS 'Minimum',MAX(salary) AS 'Maximum',COUNT(id) AS 'Count' FROM emp_payroll WHERE gender = ? GROUP BY gender";
 		   	PreparedStatement selOpStatement = con.prepareStatement(selOpQuery);
 		   	selOpStatement.setString(1, gen);
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
		}
	}
	public static void main( String[] args ) {
    	
		EmpPayroll emp = new EmpPayroll();
		Connection con = getSqlConnection();
		try {
    	   
			emp.insertData(con);
			emp.readAllData(con);
			emp.updateDataByName(con);
			emp.readDataByName(con);
			emp.readDataByDateRange(con);
			emp.databaseFunction(con);
			
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
