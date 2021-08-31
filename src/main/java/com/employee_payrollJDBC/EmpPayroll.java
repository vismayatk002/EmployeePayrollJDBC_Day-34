package com.employee_payrollJDBC;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class EmpPayroll {
	
    public static void main( String[] args ) {
    	
       Connection con = getSqlConnection();
       try {
    	   if(con != null) {
    		   String insertQuery = "INSERT INTO employee_payroll(id, name, gender, salary, startDate) VALUES (?,?,?,?,?)";
    		   PreparedStatement preparedStatement = con.prepareStatement(insertQuery);
    		   preparedStatement.setInt(1, 4);
    		   preparedStatement.setString(2, "Terisa");
    		   preparedStatement.setString(3, "F");
    		   preparedStatement.setInt(4, 20000);
    		   preparedStatement.setString(5, "2021-07-13");
    		   int rowInserted = preparedStatement.executeUpdate();
    		   if(rowInserted > 0){
    			   System.out.println("Data Inserted");
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
