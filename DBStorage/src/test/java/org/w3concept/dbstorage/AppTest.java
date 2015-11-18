package org.w3concept.dbstorage;

import java.io.IOException;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.beans.factory.xml.XmlBeanFactory;
import org.springframework.core.io.FileSystemResource;
import org.w3concept.dbstorage.common.DBConnection;
import org.w3concept.dbstorage.common.DBException;
import org.w3concept.dbstorage.common.DBMetaDataSet;
import org.w3concept.dbstorage.common.DBMySQLConnection;
import org.w3concept.dbstorage.common.DBOracleConnection;
import org.w3concept.dbstorage.common.DBResultSet;
import org.w3concept.dbstorage.common.DBRow;
import org.w3concept.dbstorage.common.DBTable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
@SuppressWarnings("deprecation")
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     * @throws SQLException 
     * @throws IOException 
     */
	public AppTest( String testName ) throws DBException, SQLException, IOException
    {
		super( testName );

		FileSystemResource resource = new FileSystemResource("applicationContext.xml");
		XmlBeanFactory factory = new XmlBeanFactory (resource);
		DataSource ds = (DataSource) factory.getBean("mysql");
		
		DBConnection conn = new DBMySQLConnection(ds.getConnection(),false);
		DBTable devise = new DBTable(conn, "DEVISE");
		
		DBRow row = devise.newRow();
		row.getColumn("DEV_CODE").setValue("TT");
		row.getColumn("DEV_SYMBOLE").setValue("ALEX");
		
		conn.insert(devise.getName(), row);
		row.getColumn("DEV_SYMBOLE").setValue("ALEX2");
		
		conn.update(devise.getName(), row);
		System.out.println("Row="+row);
		
		row = devise.newRow();
		row.getColumn("DEV_CODE").setValue("T0");
		row.getColumn("DEV_SYMBOLE").setValue("ALEX0");

		conn.insert(devise.getName(), row);

		DBResultSet set = new DBResultSet(conn, "select * from devise");
		devise = new DBTable(conn, "DEVISE");
		devise.fill(set);
		
		
		ObjectMapper mapper = new ObjectMapper();
		String serialized = mapper.writeValueAsString(devise);
		System.out.println("json Row="+serialized);
		
		mapper.readValue(serialized, DBTable.class);
		
		//conn.delete(devise.getName(), row);
		conn.rollback();
		conn.close();
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
}
