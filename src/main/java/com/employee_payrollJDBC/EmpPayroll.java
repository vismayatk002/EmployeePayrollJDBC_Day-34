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
    		   
    		   String readQuery = "SELECT * FROM employee_payroll";
    		   Statement statement = (Statement) con.createStatement();
    		   ResultSet resultSet = statement.executeQuery(readQuery);
    		   
    		   while(resultSet.next()) {
    			   
    			   int id = resultSet.getInt("id");
    			   String name = resultSet.getString("name");
    			   String gender = resultSet.getString("gender");
    			   int salary = resultSet.getInt("salary");
    			   String startDate = resultSet.getDate("startDate").toString();
    			   
    			   String rowData = String.format("Id : %d \nName : %s \nGender : %s \nSalary : %d \nStartDate : %s", id, name, gender, salary,startDate);
    			   System.out.println(rowData);
    		   }
   		   
    		   String updateQuery = "UPDATE employee_payroll SET salary = ? WHERE name = ?";
    		   PreparedStatement updateStatement = con.prepareStatement(updateQuery);
    		   updateStatement.setInt(1, 30000);
    		   updateStatement.setString(2, "Terisa");
    		   
    		   int rowUpdated = updateStatement.executeUpdate();
    		   if(rowUpdated > 0){
    			   System.out.println("Data Updated");
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
