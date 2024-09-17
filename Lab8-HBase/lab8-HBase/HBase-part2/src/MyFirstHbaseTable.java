
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

public class MyFirstHbaseTable
{

	public static final String EMPLOYEE_TABLE_NAME = "employees";
	
	public static final String PERSONAL_COLUMN_GROUP_NAME = "pnl_dt";
	public static final String PROFESSIONAL_COLUMN_GROUP_NAME = "prf_dt";
	
	public static final String NAME_COLUMN = "nm";
	public static final String CITY_COLUMN = "cty";
	public static final String DESIGNATION_COLUMN = "dsgntn";
	public static final String SALARY_COLUMN = "slry";

	public static void main(String... args) throws IOException
	{

		Configuration config = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin())
		{
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(EMPLOYEE_TABLE_NAME));

			HColumnDescriptor personalDataGroup = new HColumnDescriptor(PERSONAL_COLUMN_GROUP_NAME);
            personalDataGroup.setCompressionType(Algorithm.NONE);

            HColumnDescriptor professionalDataGroup = new HColumnDescriptor(PROFESSIONAL_COLUMN_GROUP_NAME);


            tableDescriptor.addFamily(personalDataGroup);
			tableDescriptor.addFamily(professionalDataGroup);


            if (admin.tableExists(tableDescriptor.getTableName()))
			{
                admin.disableTable(tableDescriptor.getTableName());
                admin.deleteTable(tableDescriptor.getTableName());
            }

            admin.createTable(tableDescriptor);

            HTable hTable = new HTable(config, EMPLOYEE_TABLE_NAME);

            List<Employee> employees = Arrays.asList(
                    new Employee(1, "Peter", "Fairfield", "Manager", 110000),
                    new Employee(2, "Nancy", "Burlington", "Sr. Engineer", 120000),
                    new Employee(3, "Bob", "Weymoutht", "Jr. Engineer", 90000)
            );

            hTable.put(toPuts(employees));


            
		}
	}

	private static List<Put> toPuts(List<Employee> employees){

	    List<Put> result = new ArrayList<>();

	    for (Employee employee : employees){

	        Put p = new Put(Bytes.toBytes(Integer.toString(employee.id)));

            p.addColumn(Bytes.toBytes(PERSONAL_COLUMN_GROUP_NAME), Bytes.toBytes(NAME_COLUMN), Bytes.toBytes(employee.name));
            p.addColumn(Bytes.toBytes(PERSONAL_COLUMN_GROUP_NAME), Bytes.toBytes(CITY_COLUMN), Bytes.toBytes(employee.city));

            p.addColumn(Bytes.toBytes(PROFESSIONAL_COLUMN_GROUP_NAME), Bytes.toBytes(DESIGNATION_COLUMN), Bytes.toBytes(employee.designation));
            p.addColumn(Bytes.toBytes(PROFESSIONAL_COLUMN_GROUP_NAME), Bytes.toBytes(SALARY_COLUMN), Bytes.toBytes(Double.toString(employee.salary)));

            result.add(p);
        }
	    System.out.println("table creation is successfully done!!!");
        return result;
        
    }

	public static class Employee {

	    private int id;

	    private String name;

	    private String city;

	    private String designation;

	    private double salary;

        public Employee(int id, String name, String city, String designation, double salary) {
            this.id = id;
            this.name = name;
            this.city = city;
            this.designation = designation;
            this.salary = salary;
        }
        
    }
}

