package io.bit.ads;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.*;

public class AdsEngine {
	private String mAdsDataFilePath;
	private String mBudgetFilePath;
	private IndexBuilder indexBuilder;
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	
	//这里就把在SearchAdsServer里面建立obj的时候的那些参数读进来了
	public AdsEngine(String adsDataFilePath, String budgetDataFilePath,String memcachedServer,int memcachedPortal,String mysqlHost,String mysqlDb,String user,String pass)
	{
		mAdsDataFilePath = adsDataFilePath;
		mBudgetFilePath = budgetDataFilePath;
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
		indexBuilder = new IndexBuilder(memcachedServer,memcachedPortal,mysql_host,mysql_db,mysql_user,mysql_pass);
	}
	
	//这就是在SearchAdsServer里面调用的init初始化函数
	//这个函数就是把广告的json数据给读进来然后进行解析
	public Boolean init()
	{
		//load ads data
		try (BufferedReader brAd = new BufferedReader(new FileReader(mAdsDataFilePath))) {
			String line;
			//上面的mAdsDataFilePath，实际上就是之前爬虫爬过来的Ads数据
			//下面就是一个while循环，把Ads的json数据给一行一行的读
			while ((line = brAd.readLine()) != null) {
				//解析json
				JSONObject adJson = new JSONObject(line);
				//建立之前设计好的一个Ad类的ad对象
				Ad ad = new Ad(); 
				//adId和campaignId都是非空的，如果是空的话这个数据就不处理了
				if(adJson.isNull("adId") || adJson.isNull("campaignId")) {
					continue;
				}
				//如果非空，那就读下来
				ad.adId = adJson.getLong("adId");
				ad.campaignId = adJson.getLong("campaignId");
				//下面的这些数据，可能为空，那就给附上一些特殊的值
				ad.brand = adJson.isNull("brand") ? "" : adJson.getString("brand");
				ad.price = adJson.isNull("price") ? 100.0 : adJson.getDouble("price");
				ad.thumbnail = adJson.isNull("thumbnail") ? "" : adJson.getString("thumbnail");
				ad.title = adJson.isNull("title") ? "" : adJson.getString("title");
				ad.detail_url = adJson.isNull("detail_url") ? "" : adJson.getString("detail_url");						
				ad.bidPrice = adJson.isNull("bidPrice") ? 1.0 : adJson.getDouble("bidPrice");
				ad.pClick = adJson.isNull("pClick") ? 0.0 : adJson.getDouble("pClick");
				ad.category =  adJson.isNull("category") ? "" : adJson.getString("category");
				ad.description = adJson.isNull("description") ? "" : adJson.getString("description");
				//下面就是存储keywords，步骤稍微多一点，要用一个array去存
				ad.keyWords = new ArrayList<String>();
				JSONArray keyWords = adJson.isNull("keyWords") ? null :  adJson.getJSONArray("keyWords");
				for(int j = 0; j < keyWords.length();j++)
				{
					ad.keyWords.add(keyWords.getString(j));
				}
				
				//下面是关键，要build inverted index and forward index
				//indexBuilder这个对象是IndexBuilder这个类创建的
				//现在去看看这个IndexBuilder类
				if(!indexBuilder.buildInvertIndex(ad) || !indexBuilder.buildForwardIndex(ad))
				{
					//log				
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		//现在数据已经都放好了，index都建立好了
		//就要开始进行load budget data
		try (BufferedReader brBudget = new BufferedReader(new FileReader(mBudgetFilePath))) {
			String line;
			//同样的，这里仍然是要一行一行地读
			while ((line = brBudget.readLine()) != null) {
				JSONObject campaignJson = new JSONObject(line);
				//读取解析json数据里面的budget和campaignId
				Long campaignId = campaignJson.getLong("campaignId");
				double budget = campaignJson.getDouble("budget");
				//建立一个Campaign对象，把上面抓取的campaignId和budget给存储到campaign对象里面
				Campaign camp = new Campaign();
				camp.campaignId = campaignId;
				camp.budget = budget;
				//然后去update Budget，去IndexBuilder类里去看
				if(!indexBuilder.updateBudget(camp))
				{
					//log
				}			
			}
		}catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
	//到这里adsengine就已经init好了
	
	
	//核心方法，selectAds
	public List<Ad> selectAds(String query)
	{
		//query understanding
		//首先，利用lucene这个library来做NLP；query understanding的结果就是一个list of query term
		//这个在之前的视频里面讲过；去看对应的QueryParser类和里面的相关的方法
		//最后这个方法得到了一个处理好的query
		List<String> queryTerms = QueryParser.getInstance().QueryUnderstand(query);
		//select ads candidates
		//然后，利用上面处理好的query（作为selectAds的参数传进去），找出ads candidates
		//现在就是利用AdsSelector类的getInstance方法获取一个AdsSelector的obj，然后调用selectAds方法
		//去AdsSelector类里面看，如何进行广告的海选
		List<Ad> adsCandidates = AdsSelector.getInstance(mMemcachedServer, mMemcachedPortal, mysql_host, mysql_db,mysql_user, mysql_pass).selectAds(queryTerms);		
		//L0 filter by pClick, relevance score
		//现在，做一个filter，利用pClick排一下顺序，初步过滤一下这些candidates
		//上面已经进行完毕了广告的海选，有了adsCandidates，现在要进行进一步过滤
		//去看AdsFilter类的level0的筛选
		List<Ad> L0unfilteredAds = AdsFilter.getInstance().LevelZeroFilterAds(adsCandidates);
		System.out.println("L0unfilteredAds ads left = " + L0unfilteredAds.size());
		
		//rank
		//这里就是rank Ads
		//去看AdsRanker
		List<Ad> rankedAds = AdsRanker.getInstance().rankAds(L0unfilteredAds);
		System.out.println("rankedAds ads left = " + rankedAds.size());

		//L1 filter by relevance score : select top K ads
		//然后从过滤好的candidates里面，选出top K
		//这里就是进行的是level1的筛选，去看AdsFilter的level1筛选
		int k = 20;
		List<Ad> unfilteredAds = AdsFilter.getInstance().LevelOneFilterAds(L0unfilteredAds,k);
		System.out.println("unfilteredAds ads left = " + unfilteredAds.size());

		//Dedupe ads per campaign
		//把这些结果进行去重，进行dedup
		//去AdsCampaignManager里面找对应的DedupeByCampaignId方法
		//这里其实就是进行一些后期的处理
		List<Ad> dedupedAds = AdsCampaignManager.getInstance(mysql_host, mysql_db,mysql_user, mysql_pass).DedupeByCampaignId(unfilteredAds);
		System.out.println("dedupedAds ads left = " + dedupedAds.size());

		//最后过滤完毕后，就开始算钱算budget了
		//去看AdsCampaignManager里面的ApplyBudget方法
		List<Ad> ads = AdsCampaignManager.getInstance(mysql_host, mysql_db,mysql_user, mysql_pass).ApplyBudget(dedupedAds);
		System.out.println("AdsCampaignManager ads left = " + ads.size());

		//allocation
		//最后开始准备放置广告的位置了
		AdsAllocation.getInstance().AllocateAds(ads);
		return ads;
	}

}
