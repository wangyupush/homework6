import java.io.IOException;  
//import java.util.Map;  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;   
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.util.Bytes;  
@SuppressWarnings("deprecation")
public class HbaseTest{//声明静态HBaseConfiguration  
    static Configuration cfg=HBaseConfiguration.create();  
 @SuppressWarnings("resource")
public static void create(String tablename,String cf1,String cf2,String cf3,String cf4)throws Exception{  
    //HBaseAdmin admin=new HBaseAdmin(cfg); 
	 HBaseAdmin admin=new HBaseAdmin(cfg);
     if(admin.tableExists(tablename)){  
         System.out.println("table Exists!");  
         System.exit(0);  
     }  
     else{  
         HTableDescriptor tableDesc =new HTableDescriptor(tablename);  
         tableDesc.addFamily(new HColumnDescriptor(cf1));  
         tableDesc.addFamily(new HColumnDescriptor(cf2)); 
         tableDesc.addFamily(new HColumnDescriptor(cf3)); 
         tableDesc.addFamily(new HColumnDescriptor(cf4));
         admin.createTable(tableDesc);  
         System.out.println("create table success!");  
     }  
 }  
 public static void put(String tablename,String row,String cf,String column,String data)  
         throws IOException{  
     @SuppressWarnings("resource")
	HTable table=new HTable(cfg,tablename);  
    Put p1=new Put(Bytes.toBytes(row));  
    p1.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(data));  
    table.put(p1);  
    System.out.println("put'"+row+"','"+cf+":"+column+"','"+data+"'");  
 }  
 public static void get(String tablename,String row)throws IOException{  
     
	@SuppressWarnings("resource")
	HTable table=new HTable(cfg,tablename);  
     Get g=new Get(Bytes.toBytes(row));  
     Result result=table.get(g);  
     System.out.println("Get:"+result);  
 }  
 public static void scan(String tablename) throws Exception{  
     @SuppressWarnings("resource")
	HTable table=new HTable(cfg,tablename);  
     Scan s=new Scan();  
     ResultScanner rs=table.getScanner(s);  
     for(Result r:rs){  
         System.out.println("Scan:"+r);  
     }  
 }  
 public static boolean delete(String tablename) throws IOException{  
     @SuppressWarnings("resource")
	HBaseAdmin admin=new HBaseAdmin(cfg);  
     if(admin.tableExists(tablename)){  
         try{  
             admin.disableTable(tablename);  
             admin.deleteTable(tablename);  
         }catch(Exception ex){  
             ex.printStackTrace();  
             return false;  
         }  
     }  
     return true;  
 }  
 public static void main(String [] args){  
     String tablename="students";  
     String cf1="ID";
     String cf2="Description";
     String cf3="Courses";
     String cf4="Home";
     try{  
         HbaseTest.create(tablename,cf1,cf2,cf3,cf4);  
         HbaseTest.put(tablename, "001", cf2, "Name", "Li Lei");
         HbaseTest.put(tablename, "001", cf2, "Height", "176");
         HbaseTest.put(tablename, "001", cf3, "Chinese", "80");
         HbaseTest.put(tablename, "001", cf3, "Math", "90");
         HbaseTest.put(tablename, "001", cf3, "Physics", "95");
         HbaseTest.put(tablename, "001", cf4, "Province", "Zhejiang");
         
         HbaseTest.put(tablename, "002", cf2, "Name", "Han Meimei");
         HbaseTest.put(tablename, "002", cf2, "Height", "183");
         HbaseTest.put(tablename, "002", cf3, "Chinese", "88");
         HbaseTest.put(tablename, "002", cf3, "Math", "77");
         HbaseTest.put(tablename, "002", cf3, "Physics", "66");
         HbaseTest.put(tablename, "002", cf4, "Province", "Beijing");
         
         HbaseTest.put(tablename, "003", cf2, "Name", "Xiao Ming");
         HbaseTest.put(tablename, "003", cf2, "Height", "162");
         HbaseTest.put(tablename, "003", cf3, "Chinese", "90");
         HbaseTest.put(tablename, "003", cf3, "Math", "90");
         HbaseTest.put(tablename, "003", cf3, "Physics", "90");
         HbaseTest.put(tablename, "003", cf4, "Province", "Shanghai"); 
         HbaseTest.scan(tablename);  
        /* if(true==yin.delete(tablename)){ 
             System.out.println("Delete table:"+tablename+"success!"); 
         }*/  
     }catch(Exception e){  
         e.printStackTrace();  
     }  
 }  
}  