package io.bit.ads;

import java.util.ArrayList;
import java.util.List;

//这个类是用来进行筛选广告的
//类里面有几个函数，对应的是不同批次不同level的筛选
public class AdsFilter {
	private static AdsFilter instance = null;
	private static double pClickThreshold = 0.0;
	private static double relevanceScoreThreshold = 0.01;
	private static int mimNumOfAds = 4;
	protected AdsFilter()
	{

	}
	public static AdsFilter getInstance() {
	      if(instance == null) {
	         instance = new AdsFilter();
	      }
	      return instance;
	}
	//下面这个方法就是用来把海选出来的adsCandidates进行过滤的方法，level0的筛选
	//方法里面的逻辑，就是各种筛选广告的标准
	public List<Ad> LevelZeroFilterAds(List<Ad> adsCandidates)
	{
		//首先，这次筛选过后的广告不能太少，要设定一个最低广告数量
		//如果传进来的广告数量本身就已经低于这个数量了，那么就没有必要再筛选了，否则就没有广告显示了
		if(adsCandidates.size() <= mimNumOfAds)
			return adsCandidates;
		//下面开始正式筛选
		List<Ad> unfilteredAds = new ArrayList<Ad>();
		for(Ad ad : adsCandidates)
		{
			//这里是利用pClick和relevanceScore进行选择的
			//pClickThreshold和relevanceScoreThreshold都是设置好的下限
			//只有高于这两个下限，广告才能被筛选出来，放到结果的那个ArrayList里面
			if(ad.pClick >= pClickThreshold && ad.relevanceScore > relevanceScoreThreshold)
			{
				unfilteredAds.add(ad);
			}
		}
		return unfilteredAds;
	}
	//下面这个方法就是用来进行level1的筛选
	//和上面的那个筛选不一样的地方就是，上面的level0筛选是只要达到目标的分数就可以通过
	//而这个方法是根据指定的数量，把top k个ads给筛选出来
	public List<Ad> LevelOneFilterAds(List<Ad> adsCandidates,int k)
	{
		if(adsCandidates.size() <= mimNumOfAds)
			return adsCandidates;
		
		List<Ad> unfilteredAds = new ArrayList<Ad>();
		//下面的loop就是依次取前k个广告
		for(int i = 0; i < Math.min(k, adsCandidates.size());i++)
		{
			unfilteredAds.add(adsCandidates.get(i));
		}
		return unfilteredAds;
	}
}
