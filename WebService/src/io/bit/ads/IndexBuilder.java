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

//这个类就是为了要去实现build index功能的
//主要就是两个函数，inverted index和forward index
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
	
	//下面是build inverted index的函数
	//如何build？
	//首先是把term拿出来，然后term后面跟着一个posting list
	//term就是keyword，posting list就是adId
	//现在传进来了一个ad对象，ad对象里面有刚刚建好的装着keyword的Arraylist，就可以得到term了
	public Boolean buildInvertIndex(Ad ad)
	{
		try 
		{
			//把keywords这个Arraylist进行稍微的整理一下，并存到一个String里面
			String keyWords = Utility.strJoin(ad.keyWords, ",");
			//建立一个Memcached，因为这里是用mamcached作为一个存储key-value pair的容器
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(mMemcachedServer, mMemcachedPortal));
			//把存储keywords的String转换成list
			List<String> tokens = Utility.cleanedTokenize(keyWords);
			//现在要把position给引入进来
			//key: AdID, value: list<position>
			//Map<Long, List<Long>> positionIndex = new HashMap<>();
			//下面进行keywords的tokenize
			for(int i = 0; i < tokens.size();i++)
			{	
				//从那个list里面一个一个把里面的keyword给拿出来
				String key = tokens.get(i);
				if(cache.get(key) instanceof Set) //如果当前的cache里面有这个term
				{
//					//下面这些代码是新加的
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
					//把已经存在的key-value pair，既term-posting list取出来
					//用cache.get(key)就可以获得value，也就是posting list
					Set<Long>  adIdList = (Set<Long>)cache.get(key);
					//然后把当前这个数据的adId添加到已经存在的posting list里面
					adIdList.add(ad.adId);
					//最后存储到cache里面
				    cache.set(key, EXP, adIdList);
				    
				    //把adIDList要插入到上面的positionIndex里面
				    
				}
				else //如果当前的cache里面没有这个term
				{
//					//下面这部分代码是新加的，为了存储出现的position
//					PositionIndexItem positionIndexItem = new PositionIndexItem();
//					positionIndexItem.term_count = 1;
//					positionIndexItem.positionIndex = new HashMap<>();
//					List<Long> position_list = new ArrayList<>();
//					position_list.add((long) i);
//					positionIndexItem.positionIndex.put(ad.adId, position_list);
//					cache.set(key, EXP, positionIndexItem);
					
					//那么就创建一个关于这个term的posting list
					//而这里用来表示posting list的数据结构是一个HashSet
					//这是因为access的时候要O(1)，而且不希望posting list里面有重复的adId
					Set<Long>  adIdList = new HashSet<Long>();
					adIdList.add(ad.adId);
					//然后存储到cache里面，term就作为key，posting list就作为value
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
	
	//build完inverted index以后，就要进行build forward index了
	//也就是把广告里的所有信息都放到index里面
	//既然是所有的信息，就直接从数据库里面取数据然后放进index了
	//所以这里就是在mysql里面进行的
	//现在去看mysql里面的相关方法
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
	
	//这里是update Bugdet的函数
	//和上面build forward index一样，要用到MySQL里面的方法
	//调用addCampaignData方法，去mysql里面去看相关方法
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
