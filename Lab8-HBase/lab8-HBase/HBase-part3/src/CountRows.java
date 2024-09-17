import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;


public class CountRows {
	
	@SuppressWarnings({"resource", "deprecation"})
	  public static void main(String[] args) throws IOException {

	      org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
	      HTable table = new HTable(config, "employees");
	      	      
	      int count = 0;
	      Scan scan = new Scan();
	      
	      FilterList flist = new FilterList();
	      flist.addFilter(new KeyOnlyFilter());
	      scan.setFilter(flist);
	      
	      ResultScanner rs = table.getScanner(scan);
	      while((rs.next()) != null){
	    	  count++;
	      }
	      System.out.println("Number Of Rows = " + count);
	}
}
