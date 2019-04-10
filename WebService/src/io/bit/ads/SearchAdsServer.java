package io.bit.ads;

import java.io.IOException;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Servlet implementation class SearchAds
 */
//下面的这个/SearchAds就是request path
@WebServlet("/SearchAds")
public class SearchAdsServer extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private ServletConfig config = null;  
	private AdsEngine adsEngine = null;
	private String uiTemplate = "";
	private String adTemplate = "";

    /**
     * @see HttpServlet#HttpServlet()
     */
    public SearchAdsServer() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	//这里的这个参数，config文件，就是一个配置文件
    //这个配置文件要创建再WEB_INF里面，就是那个web.xml
    //现在去看看这个是干嘛的
    //看完以后，就知道那里面配置的parameter都是现在需要调用的
	public void init(ServletConfig config) throws ServletException {
		// TODO Auto-generated method stub
		//然后，创造一系列的obj，从config文件中先获取context，然后再一个一个去找对应的位置，获取对应的数据
		//比如adsDataFilePath，实际上是从config文件里面获取到了数据的路径文件，然后在这里存储到obj里面，以便这里使用
		//可以看出来，adsDataFilePath是由application创建的，而application是config的context，而config就是配置文件
		this.config =  config;
		super.init(config);
		ServletContext application = config.getServletContext();
		//下面就是把config file里面的那些东西给一个一个读出来
	    String adsDataFilePath = application.getInitParameter("adsDataFilePath");
	    String budgetDataFilePath = application.getInitParameter("budgetDataFilePath");
	    String uiTemplateFilePath = application.getInitParameter("uiTemplateFilePath");
	    String adTemplateFilePath = application.getInitParameter("adTemplateFilePath");
	    String memcachedServer = application.getInitParameter("memcachedServer");
	    String mysqlHost = application.getInitParameter("mysqlHost");
	    String mysqlDb = application.getInitParameter("mysqlDB");
	    String mysqlUser = application.getInitParameter("mysqlUser");
	    String mysqlPass = application.getInitParameter("mysqlPass");
	    int memcachedPortal = Integer.parseInt(application.getInitParameter("memcachedPortal"));
	    //做完上面的initialization以后，要做一个adsEngine
	    //adsEngine里面的是所有广告相关的workflow
	    //建立adsEngine的时候，要把所有config file里面的参数，也就是刚刚initialization时的参数都传进去
	    //就是把广告数据、budget数据、mysql数据库还有其他很多东西传进去，从而准备开始进行workflow
		this.adsEngine = new AdsEngine(adsDataFilePath,budgetDataFilePath,memcachedServer,memcachedPortal,mysqlHost,mysqlDb,mysqlUser,mysqlPass);
		//建立完以后，initialize这个adsEngine，就可以开始workflow了
		//现在去AdsEngine类看看
		this.adsEngine.init();  
		//执行完了以后，就说明是数据准备好了，web service也搭建好了
		System.out.println("adsEngine initilized");
		//然后要进行UI template
		//这个是干嘛的？
		//UI template在web.xml里面也配置了，分别是ui.html和item.html，是一个输出广告结果的时候的一个界面
		//load UI template
		try {
			byte[] uiData;
			byte[] adData;
			//下面创造的两个uiTemplate和adTemplate，就是respond请求的时候输出去的，类似输出模板
			//uiTemplate和adTemplate分别对应两个html文件，可以去看看是什么样的
			uiData = Files.readAllBytes(Paths.get(uiTemplateFilePath));
			uiTemplate = new String(uiData, StandardCharsets.UTF_8);
			adData = Files.readAllBytes(Paths.get(adTemplateFilePath));
			adTemplate = new String(adData, StandardCharsets.UTF_8);
			//建造好了两个obj，现在去看下面的doGet方法
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("UI template initilized");
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	//doGet里面有request参数，这个就是从request path后面跟着的那个parameter得到的，也就是用户输入的query
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//首先从request里面，get到parameter，存储到一个query对象里面
		String query = request.getParameter("q");
		//然后，根据这些query，利用adsEngine的selectAds方法，把候选的广告都给选择出来
		//总之现在就是选择出来了候选广告，放在adsCandidates里面
		List<Ad> adsCandidates = adsEngine.selectAds(query);
		//然后好好看看这个selectAds方法，这是一个最核心的方法
		
		
		
		//现在从上面获取到的uiTemplate，放到一个String里面作为最终结果
		String result = uiTemplate;
        String list = "";
		for(Ad ad : adsCandidates)
		{	
			//从adsCandidates里面一个一个把候选的广告拿出来
			System.out.println("final selected ad id = " + ad.adId);
			System.out.println("final selected ad rank score = " + ad.rankScore);
			String adContent = adTemplate;
			//把这些广告的关键信息，比如title, brand, url等等拿出来，放到以adTemplate为模板的String里面
			//也就是填充好，item.html里面的placeholder，可以点开文件看看什么样的结构
			adContent = adContent.replace("$title$", ad.title);
			adContent = adContent.replace("$brand$", ad.brand);
			adContent = adContent.replace("$img$", ad.thumbnail);
			adContent = adContent.replace("$link$", ad.detail_url);
			adContent = adContent.replace("$price$", Double.toString(ad.price));
			//System.out.println("adContent: " + adContent);
			//然后一个一个都存到list里面
			list = list + adContent;
		}
		//等到把所有的候选广告都收集完了，就把收集好广告各种信息的上面的那个list，填充到result，也就是ui.html里面
		//从下面可以看出，就是把list放到了ui.html对应的placeholder的位置
		result = result.replace("$list$", list);
		//System.out.println("list: " + list);
		//System.out.println("RESULT: " + result);
		response.setContentType("text/html; charset=UTF-8");
		response.getWriter().write(result);	
	}
}
