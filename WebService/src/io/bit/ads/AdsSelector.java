package io.bit.ads;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import net.spy.memcached.MemcachedClient;
//��������AdsSelector�࣬���������к�ѡ������
public class AdsSelector {
	private static AdsSelector instance = null;
	//private int EXP = 7200;
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	protected AdsSelector(String memcachedServer,int memcachedPortal,String mysqlHost,String mysqlDb,String user,String pass)
	{
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;	
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;
	}
	//������������е�instance�ǿյ�һ������
	public static AdsSelector getInstance(String memcachedServer,int memcachedPortal,String mysqlHost,String mysqlDb,String user,String pass) {
	      if(instance == null) {
	         instance = new AdsSelector(memcachedServer, memcachedPortal,mysqlHost,mysqlDb,user,pass);
	      }
	      return instance;
    }
	//���濪ʼ��ʽ����select���
	public List<Ad> selectAds(List<String> queryTerms)
	{
		List<Ad> adList = new ArrayList<Ad>();
		HashMap<Long,Integer> matchedAds = new HashMap<Long,Integer>();
		try {
			//selectҪ��index����ȥselect����indexbuilder��������mamcached����index�ģ����Ի�Ҫ��һ��memcachedClient
			//��Server�Ķ˿ںź͵�ַ�Ž�ȥ���Ӷ����MamcachedClient����������ܴ洢��ѡ�Ĺ�棬Ҳ����index
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(mMemcachedServer,mMemcachedPortal));

			//���ÿһ��term��Ҫ����һ��index
			//��ס����cache���棬key��term��value��posting list
			for(String queryTerm : queryTerms)
			{
				System.out.println("selectAds queryTerm = " + queryTerm);
				@SuppressWarnings("unchecked")
				//�Ӵ洢key-value pair��cache���棬����key�ҳ�value��Ҳ���Ǹ���term�ҳ�posting list
				//��ס�����Ǻ�ѡ������ÿһ��term����ƥ��̶��ж��ٵĹ�涼Ҫ�ó���
				Set<Long>  adIdList = (Set<Long>)cache.get(queryTerm);
				if(adIdList != null && adIdList.size() > 0)
				{
					//���posting list�ǿգ���ô�Ͱ������adId���ó���
					for(Object adId : adIdList)
					{
						Long key = (Long)adId;
						//�ó�adId�Ժ�Ҫ���棬�����õ���һ��HashMap���б����
						//HashMap��key����adId��value���ǳ��ֵĴ���
						if(matchedAds.containsKey(key))
						{
							//������ֹ�����ôvalue+1
							int count = matchedAds.get(key) + 1;
							matchedAds.put(key, count);
						}
						else
						{
							//���򣬽��г�ʼ�����key-value pair
							matchedAds.put(key, 1);
						}
					}
				}				
			}
			//������������Ѿ��洢��HashMap����ѡ����˵�adId�ͳ��ִ��������б���
			//Ŀ���������ѡ������ad��relevanceScore
			for(Long adId:matchedAds.keySet())
			{			
				//�õ�adId�Ժ�Ҫ�Ѷ�Ӧ��ad�������Ϣ��Ҳ���Ǵ洢��mysql���ݿ�����Ĺ����Ϣ�ó���
				//������Ϊ��Ҫ����relevanceScore�Ļ���Ҫ�õ�keywords����Ҫ֪��keywords�����ж��ٸ�term
				//���ֻ�ܴ����ݿ������ҵ���index����û��
				//��Ϊindex�����¼����term��posting list��û��keywords����
				//term�ǵ����ĵ��ʣ�keywords�Ǵ���
				System.out.println("selectAds adId = " + adId);
				MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
				Ad  ad = mysql.getAdData(adId);
				//���������relevanceScore��������Ϣ�������ۼ���
				//relevanceScore = number of word match query / total number of words in key words
				//����Ҫ��¼��������ǰ�������Ĺ���ж��ٸ�term�Ǻ��û������query��matched��
				//������HashMap����¼������ÿ��adId���ֵĴ������Ա�������Score
				//keywords.size()�����ж��ٸ�term
				double relevanceScore = (double) (matchedAds.get(adId) * 1.0 / ad.keyWords.size());
				ad.relevanceScore = relevanceScore;
				System.out.println("selectAds pClick = " + ad.pClick);
				System.out.println("selectAds relevanceScore = " + ad.relevanceScore);
				//Ȼ��洢��adList���棬�ٷ��ص�AdsEngine������
				adList.add(ad);
			}
			//calculate pClick
			//�������½���һ��memcache
			//����ֱ�ӿ���Ƶ�ͺ��ˣ�����Ӧ����һ�δ��룬̫�鷳�Ͳ�д��
			//���߿����������մ���
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//���˾�����˹�溣ѡ�Ĺ���
		return adList;
	}
}
