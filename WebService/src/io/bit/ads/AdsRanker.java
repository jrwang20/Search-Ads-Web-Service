package io.bit.ads;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class AdsRanker {
	private static AdsRanker instance = null;
	
	protected AdsRanker()
	{


	}
	public static AdsRanker getInstance() {
	      if(instance == null) {
	         instance = new AdsRanker();
	      }
	      return instance;
	}
	public List<Ad> rankAds(List<Ad> adsCandidates)
	{
		for(Ad ad : adsCandidates)
		{
			//这里的计算方法和理论课讲的是一模一样的
			//在有了pClick（machinelearning求的）和relevanceScore（这个之前也是用word2vec去求的）以后，代入公式就可以算出rank score了
			//这里的d权重就是0.75
			ad.qualityScore = 0.75 * ad.pClick  +  0.25 * ad.relevanceScore;
			ad.rankScore = ad.qualityScore * ad.bidPrice;			
		}
		//sort by rank score
		//这里就是针对rank score来去sort
		//因为上面的for loop里面每一个ad对象的rank score都有了，现在要做一个comparator类
		//这个类就和heap的那个comparator类一样的原理实际上
		Collections.sort(adsCandidates, new Comparator<Ad>() {
	        @Override
	        public int compare(Ad ad2, Ad ad1)
	        {
	        	if (ad1.rankScore < ad2.rankScore)
	        		return -1;
	        	else if(ad1.rankScore > ad2.rankScore)
	        		return 1;
	        	else
	        		return 0;
	        }
	    });
		for(Ad ad : adsCandidates)
		{
			System.out.println("ranker rankScore = " + ad.rankScore);		
		}
		return adsCandidates;
	}
}
