package io.bit.ads;
//下面这些和sql有关的api，都是从mysql-connector那个libray里面拿出来的
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//这个Class是关于MySQL的一个封装，相当于所有关于MySql的操作都会在这个class里面完成
public class MySQLAccess {
	 private Connection d_connect = null; 
     private String d_user_name;
     private String d_password;
     private String d_server_name;
     private String d_db_name;
     public void close() throws Exception {
         System.out.println("Close database");
           try {
             if (d_connect != null) {
                 d_connect.close();
             }
           } catch (Exception e) {
               throw e;
           }
     }
     
     //Constructor
     public MySQLAccess(String server,String db,String user,String password) {
    	 //这里就是有很多mysqlDB的相关参数
    	 //db_name就是在mysql里面创造了的DB的名字
    	 //server_name就是创建mysql connections时候的127.0.0.1:3306
    	 //user和password就是自己的database的user和password
    	 //这些都可以在mysql里面看到，而实际上这些内容也都是从web.xml配置文件里面获取的
         d_server_name = server;
         d_db_name = db;
         d_user_name = user;
         d_password = password;
     }
     
     //首先，当对任何一个MySQLDB进行连接的时候，首先要创建一个connection的private function
     //这说明要去连接数据库，只有这样才能进行一系列的后续操作
     private Connection getConnection() throws Exception {
           try {
        	   // This will load the MySQL driver, each DB has its own driver
               Class.forName("com.mysql.jdbc.Driver");
               //connection需要的一些参数: server_name, db_name, user_name和password
               //这些都是在Constructor里面传给对象的，所以调用这个function的时候就可以用了
               String conn = "jdbc:mysql://" + d_server_name + "/" + 
                       d_db_name+"?user="+d_user_name+"&password="+d_password;
               System.out.println("Connecting to database: " + conn);
               //参数指定好了以后，就调用一个DriverManager，得到了一个obj
               d_connect = DriverManager.getConnection(conn);
               System.out.println("Connected to database");
               //这个返回的obj就代表了这个connection
               return d_connect;
           } catch(Exception e) {
               throw e;
           }     
      }
      
     //这个是判断目标数据是否在数据库存在的方法
     //connect参数是与对应数据库进行连接的对象
     //sql_string是个sql语句，用来从数据库里选择出来目标数据（当然select不一定都能select出来，更多是判断数据是否存在）
     private Boolean isRecordExist(Connection connect,String sql_string) throws SQLException {
    	 PreparedStatement existStatement = null;
    	 boolean isExist = false;

    	 try
         {
        	 //这里的sql_string就是sql语句，select ... from ... where ...
    		 //从数据库里面去验证，是否存在目标数据
    		 //这个existStatement对象，就是通过这个connect对象连接到数据库
    		 //然后通过sql_string这个sql语句去数据库里面验证
    		 existStatement = connect.prepareStatement(sql_string);
    		 //现在把这个existStatement对象用executeQuery方法去执行，执行结果储存
             ResultSet result_set = existStatement.executeQuery();
             //如果有数据，就返回true
             if (result_set.next())
             {
            	 isExist = true;
             }  
         }
         catch(SQLException e )
          {
                   System.out.println(e.getMessage());
                   throw e;
          } 
          finally
          {
              //用完了以后，要release这个resource 
        	  if (existStatement != null)
               {
                   existStatement.close();
               };
         }
    	  
         return isExist;
     }
     
     //这个就是把广告数据添加的方法
     public void addAdData(Ad ad) throws Exception {
    	 Connection connect = null;
    	 boolean isExist = false;
    	 //把广告的adId拿出来
    	 //用sql的方法，selct ... from ... where ...，建立一个sql语句
    	 //用这种sql语句，从数据库中查找是否已经存在了拥有这个adId的adData，从而实现验证
    	 String sql_string = "select adId from " + d_db_name + ".ad where adId=" + ad.adId;
    	 PreparedStatement ad_info = null;
    	 try
         {
        	 //创建一个数据库链接
    		 connect = getConnection();
        	 //现在看看这个ad的data是不是已经存在过，就是用上面的isRecordExist方法来判断
        	 isExist = isRecordExist(connect, sql_string);        
         }
         catch(SQLException e )
          {
              System.out.println(e.getMessage());
              throw e;
          } 
          finally
          {
               //这里也是，用完以后release这个resource
        	   if(connect != null && isExist) {
            	   connect.close();
               }
         }
    	 //如果存在的话就返回，不需要再插入了
    	 if(isExist) {
    		 return;
    	 }
    	 //如果不存在的话，准备一个insert的语句，准备进行插入
    	 //下面就要再准备一个insert的sql语句放到sql_string里面
    	 //这些 ? 就是参数，相当于是placeholder的作用
         sql_string = "insert into " + d_db_name +".ad values(?,?,?,?,?,?,?,?,?,?,?)";
          try {
        	  //首先通过connect连接到数据库并准备好sql语句sql_string
        	  ad_info = connect.prepareStatement(sql_string);
        	  //下面的从1到11依次对应着上面sql_string里面的那些问号，每一个都放到对应顺序的问好的位置
        	  ad_info.setLong(1, ad.adId);
        	  ad_info.setLong(2, ad.campaignId);
        	  String keyWords = Utility.strJoin(ad.keyWords, ",");			  
        	  ad_info.setString(3, keyWords);
        	  ad_info.setDouble(4, ad.bidPrice);
        	  ad_info.setDouble(5, ad.price);
        	  ad_info.setString(6, ad.thumbnail);
        	  ad_info.setString(7, ad.description);
        	  ad_info.setString(8, ad.brand);
        	  ad_info.setString(9, ad.detail_url);
        	  ad_info.setString(10, ad.category);
        	  ad_info.setString(11, ad.title);
        	  //然后执行，开始插入
        	  ad_info.executeUpdate();
          }
          catch(SQLException e )
          {
                   System.out.println(e.getMessage());
                   throw e;
          } 
          finally
          {
              //插入完了以后关掉 
        	  if (ad_info != null) {
            	   ad_info.close();
               };
               if (connect != null) {
            	   connect.close();          	   
               }
         }   	 
     }
     
     
     public Ad getAdData(Long adId) throws Exception {
    	 Connection connect = null;
    	 PreparedStatement adStatement = null;
    	 ResultSet result_set = null;
    	 Ad ad = new Ad();
    	 String sql_string = "select * from " + d_db_name + ".ad where adId=" + adId;
    	 try {
        	 connect = getConnection();
        	 adStatement = connect.prepareStatement(sql_string);
        	 result_set = adStatement.executeQuery();
             while (result_set.next()) {      	 
            	 ad.adId = result_set.getLong("adId");
            	 ad.campaignId = result_set.getLong("campaignId");
            	 String keyWords = result_set.getString("keyWords");
            	 String[] keyWordsList = keyWords.split(",");
            	 ad.keyWords = Arrays.asList(keyWordsList);
            	 ad.bidPrice = result_set.getDouble("bidPrice");
            	 ad.price = result_set.getDouble("price");
            	 ad.thumbnail = result_set.getString("thumbnail");
            	 ad.description = result_set.getString("description");
            	 ad.brand = result_set.getString("brand");
            	 ad.detail_url = result_set.getString("detail_url");
            	 ad.category = result_set.getString("category");
            	 ad.title = result_set.getString("title");
             }  
         }
    	 catch(SQLException e )
         {
                  System.out.println(e.getMessage());
                  throw e;
         } 
         finally
         {
              if (adStatement != null) {
            	  adStatement.close();
              };
              if (result_set != null) {
            	  result_set.close();
              }
              if (connect != null) {
           	   connect.close();          	   
              }
        }  
    	return ad;
     }
     
     public void addCampaignData(Campaign campaign) throws Exception {
    	 Connection connect = null;
    	 boolean isExist = false;
    	 String sql_string = "select campaignId from " + d_db_name + ".campaign where campaignId=" + campaign.campaignId;
    	 try
         {
        	 //和上面一样，先判断是否存在
    		 connect = getConnection();
        	 isExist = isRecordExist(connect, sql_string);        
         }
         catch(SQLException e )
          {
              System.out.println(e.getMessage());
              throw e;
          } 
          finally
          {
               if(connect != null && isExist) {
            	   connect.close();
               }
         }
    	 //如果存在，那就不用添加了
  	     if(isExist) {
  	    	 return;
  	     }
  	     //如果不成功，那就和上面一样进行插入
    	 PreparedStatement camp_info = null;   	 
         sql_string = "insert into " + d_db_name +".campaign values(?,?)";
         try {
        	  camp_info = connect.prepareStatement(sql_string);
        	  camp_info.setLong(1, campaign.campaignId);
			  camp_info.setDouble(2, campaign.budget);
			  camp_info.executeUpdate();
         }
         catch(SQLException e )
         {
              System.out.println(e.getMessage());
              throw e;
         } 
         finally
         {
              if (camp_info != null) {
            	  camp_info.close();
              };
              if (connect != null) {
           	   connect.close();          	   
              }
        }   	 
     }
     
     //根据campaignId计算budget的方法
     public Double getBudget(Long campaignId)  throws Exception {
    	 //同样，利用数据库里的数据要先建立一个connect对象
    	 Connection connect = null;
    	 PreparedStatement selectStatement = null;
    	 ResultSet result_set = null;
    	 Double budget = 0.0;
    	 String sql_string= "select budget from " + d_db_name + ".campaign where campaignId=" + campaignId;
         System.out.println("sql: " + sql_string);
    	 try
         {
        	 //连接
    		 connect = getConnection();
        	 //获取sql
    		 selectStatement = connect.prepareStatement(sql_string);
        	 //执行sql
    		 result_set = selectStatement.executeQuery();
             //总之最后返回的是budget
    		 while (result_set.next()) { 
            	 budget = result_set.getDouble("budget");
             }
         }
         catch(SQLException e )
          {
              System.out.println(e.getMessage());
              throw e;
          } 
          finally
          {
        	   if(selectStatement != null) {
        		   selectStatement.close();
        	   }
        	   if(result_set != null) {
        		   result_set.close();
        	   }
               if(connect != null) {
            	   connect.close();
               }
         }
    	 return budget;
     }
     //每次算完钱之后，预算要减去支出，重新计算，重新更新
     public void updateCampaignData(Long campaignId,Double newBudget) throws Exception {
    	 Connection connect = null;
    	 PreparedStatement updateStatement = null;
    	 String sql_string= "update " + d_db_name + ".campaign set budget=" + newBudget +" where campaignId=" + campaignId;
    	 System.out.println("sql: " + sql_string);
    	 try
         {
        	 connect = getConnection();
        	 updateStatement = connect.prepareStatement(sql_string);
        	 updateStatement.executeUpdate();
         }
         catch(SQLException e )
          {
              System.out.println(e.getMessage());
              throw e;
          } 
          finally
          {
        	   if(updateStatement != null) {
        		   updateStatement.close();
        	   }
               if(connect != null) {
            	   connect.close();
               }
         }
    	 
     }
}
