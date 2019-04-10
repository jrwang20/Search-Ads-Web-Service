package io.bit.ads;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import net.spy.memcached.MemcachedClient;
//这个类就是AdsSelector类，是用来进行海选广告的类
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
	//这里就是让所有的instance非空的一个函数
	public static AdsSelector getInstance(String memcachedServer,int memcachedPortal,String mysqlHost,String mysqlDb,String user,String pass) {
	      if(instance == null) {
	         instance = new AdsSelector(memcachedServer, memcachedPortal,mysqlHost,mysqlDb,user,pass);
	      }
	      return instance;
    }
	//下面开始正式进行select广告
	public List<Ad> selectAds(List<String> queryTerms)
	{
		List<Ad> adList = new ArrayList<Ad>();
		HashMap<Long,Integer> matchedAds = new HashMap<Long,Integer>();
		try {
			//select要从index里面去select，在indexbuilder类里面用mamcached建立index的，所以还要有一个memcachedClient
			//把Server的端口号和地址放进去，从而这个MamcachedClient对象里面才能存储候选的广告，也就是index
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(mMemcachedServer,mMemcachedPortal));

			//针对每一个term都要访问一下index
			//记住：在cache里面，key是term，value是posting list
			for(String queryTerm : queryTerms)
			{
				System.out.println("selectAds queryTerm = " + queryTerm);
				@SuppressWarnings("unchecked")
				//从存储key-value pair的cache里面，根据key找出value，也就是根据term找出posting list
				//记住这里是海选，对于每一个term不管匹配程度有多少的广告都要拿出来
				Set<Long>  adIdList = (Set<Long>)cache.get(queryTerm);
				if(adIdList != null && adIdList.size() > 0)
				{
					//如果posting list非空，那么就把里面的adId给拿出来
					for(Object adId : adIdList)
					{
						Long key = (Long)adId;
						//拿出adId以后要保存，这里用的是一个HashMap进行保存的
						//HashMap的key就是adId，value就是出现的次数
						if(matchedAds.containsKey(key))
						{
							//如果出现过，那么value+1
							int count = matchedAds.get(key) + 1;
							matchedAds.put(key, count);
						}
						else
						{
							//否则，进行初始化这个key-value pair
							matchedAds.put(key, 1);
						}
					}
				}				
			}
			//下面对于上面已经存储在HashMap里面选择好了的adId和出现次数，进行遍历
			//目标是算出海选出来的ad的relevanceScore
			for(Long adId:matchedAds.keySet())
			{			
				//拿到adId以后，要把对应的ad的相关信息，也就是存储在mysql数据库里面的广告信息拿出来
				//这是因为，要计算relevanceScore的话需要用到keywords，需要知道keywords里面有多少个term
				//这个只能从数据库里面找到，index里面没有
				//因为index里面记录的是term和posting list，没有keywords本身
				//term是单个的单词，keywords是词组
				System.out.println("selectAds adId = " + adId);
				MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
				Ad  ad = mysql.getAdData(adId);
				//下面就是算relevanceScore，利用信息检索理论计算
				//relevanceScore = number of word match query / total number of words in key words
				//就是要记录下来，当前遍历到的广告有多少个term是和用户输入的query是matched的
				//所以用HashMap来记录下来，每个adId出现的次数，以便算出这个Score
				//keywords.size()就是有多少个term
				double relevanceScore = (double) (matchedAds.get(adId) * 1.0 / ad.keyWords.size());
				ad.relevanceScore = relevanceScore;
				System.out.println("selectAds pClick = " + ad.pClick);
				System.out.println("selectAds relevanceScore = " + ad.relevanceScore);
				//然后存储到adList里面，再返回到AdsEngine类里面
				adList.add(ad);
			}
			//calculate pClick
			//这里是新建了一个memcache
			//这里直接看视频就好了，这里应该有一段代码，太麻烦就不写了
			//或者看它给的最终代码
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//到此就完成了广告海选的过程
		return adList;
	}
}
