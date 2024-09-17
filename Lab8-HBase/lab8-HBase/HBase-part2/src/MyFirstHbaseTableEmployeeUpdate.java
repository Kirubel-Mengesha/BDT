
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTableEmployeeUpdate extends MyFirstHbaseTable{

	   public static void main(String[] args) throws IOException {

	      // Instantiating Configuration class
	      Configuration config = HBaseConfiguration.create();

	      // Instantiating HTable class
	      HTable hTable = new HTable(config, "employees");

	      	      
	      int idOfBob = 3;
          Get g = new Get(Bytes.toBytes(Integer.toString(idOfBob)));
          Result result = hTable.get(g);

          Double prevSalary = Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes(PROFESSIONAL_COLUMN_GROUP_NAME), Bytes.toBytes(SALARY_COLUMN))));

          Put p = new Put(Bytes.toBytes(Integer.toString(idOfBob)));
          String updatedval=Double.toString(prevSalary * 1.03);
          p.addColumn(Bytes.toBytes(PROFESSIONAL_COLUMN_GROUP_NAME), Bytes.toBytes(SALARY_COLUMN), Bytes.toBytes(updatedval));
          p.addColumn(Bytes.toBytes(PROFESSIONAL_COLUMN_GROUP_NAME), Bytes.toBytes(DESIGNATION_COLUMN), Bytes.toBytes("Sr. Engineer"));

          hTable.put(p);
          
          hTable.close();
          System.out.println(" Done!");
	   }
	
}