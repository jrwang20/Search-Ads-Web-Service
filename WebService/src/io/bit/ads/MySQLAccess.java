package io.bit.ads;
//������Щ��sql�йص�api�����Ǵ�mysql-connector�Ǹ�libray�����ó�����
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//���Class�ǹ���MySQL��һ����װ���൱�����й���MySql�Ĳ������������class�������
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
    	 //��������кܶ�mysqlDB����ز���
    	 //db_name������mysql���洴���˵�DB������
    	 //server_name���Ǵ���mysql connectionsʱ���127.0.0.1:3306
    	 //user��password�����Լ���database��user��password
    	 //��Щ��������mysql���濴������ʵ������Щ����Ҳ���Ǵ�web.xml�����ļ������ȡ��
         d_server_name = server;
         d_db_name = db;
         d_user_name = user;
         d_password = password;
     }
     
     //���ȣ������κ�һ��MySQLDB�������ӵ�ʱ������Ҫ����һ��connection��private function
     //��˵��Ҫȥ�������ݿ⣬ֻ���������ܽ���һϵ�еĺ�������
     private Connection getConnection() throws Exception {
           try {
        	   // This will load the MySQL driver, each DB has its own driver
               Class.forName("com.mysql.jdbc.Driver");
               //connection��Ҫ��һЩ����: server_name, db_name, user_name��password
               //��Щ������Constructor���洫������ģ����Ե������function��ʱ��Ϳ�������
               String conn = "jdbc:mysql://" + d_server_name + "/" + 
                       d_db_name+"?user="+d_user_name+"&password="+d_password;
               System.out.println("Connecting to database: " + conn);
               //����ָ�������Ժ󣬾͵���һ��DriverManager���õ���һ��obj
               d_connect = DriverManager.getConnection(conn);
               System.out.println("Connected to database");
               //������ص�obj�ʹ��������connection
               return d_connect;
           } catch(Exception e) {
               throw e;
           }     
      }
      
     //������ж�Ŀ�������Ƿ������ݿ���ڵķ���
     //connect���������Ӧ���ݿ�������ӵĶ���
     //sql_string�Ǹ�sql��䣬���������ݿ���ѡ�����Ŀ�����ݣ���Ȼselect��һ������select�������������ж������Ƿ���ڣ�
     private Boolean isRecordExist(Connection connect,String sql_string) throws SQLException {
    	 PreparedStatement existStatement = null;
    	 boolean isExist = false;

    	 try
         {
        	 //�����sql_string����sql��䣬select ... from ... where ...
    		 //�����ݿ�����ȥ��֤���Ƿ����Ŀ������
    		 //���existStatement���󣬾���ͨ�����connect�������ӵ����ݿ�
    		 //Ȼ��ͨ��sql_string���sql���ȥ���ݿ�������֤
    		 existStatement = connect.prepareStatement(sql_string);
    		 //���ڰ����existStatement������executeQuery����ȥִ�У�ִ�н������
             ResultSet result_set = existStatement.executeQuery();
             //��������ݣ��ͷ���true
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
              //�������Ժ�Ҫrelease���resource 
        	  if (existStatement != null)
               {
                   existStatement.close();
               };
         }
    	  
         return isExist;
     }
     
     //������ǰѹ��������ӵķ���
     public void addAdData(Ad ad) throws Exception {
    	 Connection connect = null;
    	 boolean isExist = false;
    	 //�ѹ���adId�ó���
    	 //��sql�ķ�����selct ... from ... where ...������һ��sql���
    	 //������sql��䣬�����ݿ��в����Ƿ��Ѿ�������ӵ�����adId��adData���Ӷ�ʵ����֤
    	 String sql_string = "select adId from " + d_db_name + ".ad where adId=" + ad.adId;
    	 PreparedStatement ad_info = null;
    	 try
         {
        	 //����һ�����ݿ�����
    		 connect = getConnection();
        	 //���ڿ������ad��data�ǲ����Ѿ����ڹ��������������isRecordExist�������ж�
        	 isExist = isRecordExist(connect, sql_string);        
         }
         catch(SQLException e )
          {
              System.out.println(e.getMessage());
              throw e;
          } 
          finally
          {
               //����Ҳ�ǣ������Ժ�release���resource
        	   if(connect != null && isExist) {
            	   connect.close();
               }
         }
    	 //������ڵĻ��ͷ��أ�����Ҫ�ٲ�����
    	 if(isExist) {
    		 return;
    	 }
    	 //��������ڵĻ���׼��һ��insert����䣬׼�����в���
    	 //�����Ҫ��׼��һ��insert��sql���ŵ�sql_string����
    	 //��Щ ? ���ǲ������൱����placeholder������
         sql_string = "insert into " + d_db_name +".ad values(?,?,?,?,?,?,?,?,?,?,?)";
          try {
        	  //����ͨ��connect���ӵ����ݿⲢ׼����sql���sql_string
        	  ad_info = connect.prepareStatement(sql_string);
        	  //����Ĵ�1��11���ζ�Ӧ������sql_string�������Щ�ʺţ�ÿһ�����ŵ���Ӧ˳����ʺõ�λ��
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
        	  //Ȼ��ִ�У���ʼ����
        	  ad_info.executeUpdate();
          }
          catch(SQLException e )
          {
                   System.out.println(e.getMessage());
                   throw e;
          } 
          finally
          {
              //���������Ժ�ص� 
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
        	 //������һ�������ж��Ƿ����
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
    	 //������ڣ��ǾͲ��������
  	     if(isExist) {
  	    	 return;
  	     }
  	     //������ɹ����Ǿͺ�����һ�����в���
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
     
     //����campaignId����budget�ķ���
     public Double getBudget(Long campaignId)  throws Exception {
    	 //ͬ�����������ݿ��������Ҫ�Ƚ���һ��connect����
    	 Connection connect = null;
    	 PreparedStatement selectStatement = null;
    	 ResultSet result_set = null;
    	 Double budget = 0.0;
    	 String sql_string= "select budget from " + d_db_name + ".campaign where campaignId=" + campaignId;
         System.out.println("sql: " + sql_string);
    	 try
         {
        	 //����
    		 connect = getConnection();
        	 //��ȡsql
    		 selectStatement = connect.prepareStatement(sql_string);
        	 //ִ��sql
    		 result_set = selectStatement.executeQuery();
             //��֮��󷵻ص���budget
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
     //ÿ������Ǯ֮��Ԥ��Ҫ��ȥ֧�������¼��㣬���¸���
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
