package io.bit.ads;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import net.spy.memcached.MemcachedClient;

//��������Ϊ��Ҫȥʵ��build index���ܵ�
//��Ҫ��������������inverted index��forward index
public class IndexBuilder {
	private int EXP = 72000; //0: never expire
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	
	public IndexBuilder(String memcachedServer,int memcachedPortal,String mysqlHost,String mysqlDb,String user,String pass)
	{
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
	}
	
	//������build inverted index�ĺ���
	//���build��
	//�����ǰ�term�ó�����Ȼ��term�������һ��posting list
	//term����keyword��posting list����adId
	//���ڴ�������һ��ad����ad���������иոս��õ�װ��keyword��Arraylist���Ϳ��Եõ�term��
	public Boolean buildInvertIndex(Ad ad)
	{
		try 
		{
			//��keywords���Arraylist������΢������һ�£����浽һ��String����
			String keyWords = Utility.strJoin(ad.keyWords, ",");
			//����һ��Memcached����Ϊ��������mamcached��Ϊһ���洢key-value pair������
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(mMemcachedServer, mMemcachedPortal));
			//�Ѵ洢keywords��Stringת����list
			List<String> tokens = Utility.cleanedTokenize(keyWords);
			//����Ҫ��position���������
			//key: AdID, value: list<position>
			//Map<Long, List<Long>> positionIndex = new HashMap<>();
			//�������keywords��tokenize
			for(int i = 0; i < tokens.size();i++)
			{	
				//���Ǹ�list����һ��һ���������keyword���ó���
				String key = tokens.get(i);
				if(cache.get(key) instanceof Set) //�����ǰ��cache���������term
				{
//					//������Щ�������¼ӵ�
//					PositionIndexItem positionIndexItem = (PositionIndexItem)cache.get(key);
//					positionIndexItem.term_count = positionIndexItem.term_count + 1;
//					if(positionIndexItem.positionIndex.containsKey(ad.adId)) {
//						positionIndexItem.positionIndex.get(ad.adId).add((long) i);
//					}else {
//						List<Long> position_list = new ArrayList<>();
//						position_list.add((long) i);						
//						positionIndexItem.positionIndex.put(ad.adId, position_list);
//					}
//					cache.set(key, EXP, positionIndexItem);
					@SuppressWarnings("unchecked")
					//���Ѿ����ڵ�key-value pair����term-posting listȡ����
					//��cache.get(key)�Ϳ��Ի��value��Ҳ����posting list
					Set<Long>  adIdList = (Set<Long>)cache.get(key);
					//Ȼ��ѵ�ǰ������ݵ�adId��ӵ��Ѿ����ڵ�posting list����
					adIdList.add(ad.adId);
					//���洢��cache����
				    cache.set(key, EXP, adIdList);
				    
				    //��adIDListҪ���뵽�����positionIndex����
				    
				}
				else //�����ǰ��cache����û�����term
				{
//					//�����ⲿ�ִ������¼ӵģ�Ϊ�˴洢���ֵ�position
//					PositionIndexItem positionIndexItem = new PositionIndexItem();
//					positionIndexItem.term_count = 1;
//					positionIndexItem.positionIndex = new HashMap<>();
//					List<Long> position_list = new ArrayList<>();
//					position_list.add((long) i);
//					positionIndexItem.positionIndex.put(ad.adId, position_list);
//					cache.set(key, EXP, positionIndexItem);
					
					//��ô�ʹ���һ���������term��posting list
					//������������ʾposting list�����ݽṹ��һ��HashSet
					//������Ϊaccess��ʱ��ҪO(1)�����Ҳ�ϣ��posting list�������ظ���adId
					Set<Long>  adIdList = new HashSet<Long>();
					adIdList.add(ad.adId);
					//Ȼ��洢��cache���棬term����Ϊkey��posting list����Ϊvalue
					cache.set(key, EXP, adIdList);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	//build��inverted index�Ժ󣬾�Ҫ����build forward index��
	//Ҳ���ǰѹ�����������Ϣ���ŵ�index����
	//��Ȼ�����е���Ϣ����ֱ�Ӵ����ݿ�����ȡ����Ȼ��Ž�index��
	//�������������mysql������е�
	//����ȥ��mysql�������ط���
	public Boolean buildForwardIndex(Ad ad)
	{
		try 
		{
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			mysql.addAdData(ad);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
		return true;
	}
	
	//������update Bugdet�ĺ���
	//������build forward indexһ����Ҫ�õ�MySQL����ķ���
	//����addCampaignData������ȥmysql����ȥ����ط���
	public Boolean updateBudget(Campaign camp)
	{
		try 
		{
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			mysql.addCampaignData(camp);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
		return true;
	}
}
