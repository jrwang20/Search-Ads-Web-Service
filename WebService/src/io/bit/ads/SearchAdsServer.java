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
//��������/SearchAds����request path
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
	//��������������config�ļ�������һ�������ļ�
    //��������ļ�Ҫ������WEB_INF���棬�����Ǹ�web.xml
    //����ȥ��������Ǹ����
    //�����Ժ󣬾�֪�����������õ�parameter����������Ҫ���õ�
	public void init(ServletConfig config) throws ServletException {
		// TODO Auto-generated method stub
		//Ȼ�󣬴���һϵ�е�obj����config�ļ����Ȼ�ȡcontext��Ȼ����һ��һ��ȥ�Ҷ�Ӧ��λ�ã���ȡ��Ӧ������
		//����adsDataFilePath��ʵ�����Ǵ�config�ļ������ȡ�������ݵ�·���ļ���Ȼ��������洢��obj���棬�Ա�����ʹ��
		//���Կ�������adsDataFilePath����application�����ģ���application��config��context����config���������ļ�
		this.config =  config;
		super.init(config);
		ServletContext application = config.getServletContext();
		//������ǰ�config file�������Щ������һ��һ��������
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
	    //���������initialization�Ժ�Ҫ��һ��adsEngine
	    //adsEngine����������й����ص�workflow
	    //����adsEngine��ʱ��Ҫ������config file����Ĳ�����Ҳ���Ǹո�initializationʱ�Ĳ���������ȥ
	    //���ǰѹ�����ݡ�budget���ݡ�mysql���ݿ⻹�������ܶණ������ȥ���Ӷ�׼����ʼ����workflow
		this.adsEngine = new AdsEngine(adsDataFilePath,budgetDataFilePath,memcachedServer,memcachedPortal,mysqlHost,mysqlDb,mysqlUser,mysqlPass);
		//�������Ժ�initialize���adsEngine���Ϳ��Կ�ʼworkflow��
		//����ȥAdsEngine�࿴��
		this.adsEngine.init();  
		//ִ�������Ժ󣬾�˵��������׼�����ˣ�web serviceҲ�����
		System.out.println("adsEngine initilized");
		//Ȼ��Ҫ����UI template
		//����Ǹ���ģ�
		//UI template��web.xml����Ҳ�����ˣ��ֱ���ui.html��item.html����һ������������ʱ���һ������
		//load UI template
		try {
			byte[] uiData;
			byte[] adData;
			//���洴�������uiTemplate��adTemplate������respond�����ʱ�����ȥ�ģ��������ģ��
			//uiTemplate��adTemplate�ֱ��Ӧ����html�ļ�������ȥ������ʲô����
			uiData = Files.readAllBytes(Paths.get(uiTemplateFilePath));
			uiTemplate = new String(uiData, StandardCharsets.UTF_8);
			adData = Files.readAllBytes(Paths.get(adTemplateFilePath));
			adTemplate = new String(adData, StandardCharsets.UTF_8);
			//�����������obj������ȥ�������doGet����
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("UI template initilized");
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	//doGet������request������������Ǵ�request path������ŵ��Ǹ�parameter�õ��ģ�Ҳ�����û������query
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		//���ȴ�request���棬get��parameter���洢��һ��query��������
		String query = request.getParameter("q");
		//Ȼ�󣬸�����Щquery������adsEngine��selectAds�������Ѻ�ѡ�Ĺ�涼��ѡ�����
		//��֮���ھ���ѡ������˺�ѡ��棬����adsCandidates����
		List<Ad> adsCandidates = adsEngine.selectAds(query);
		//Ȼ��úÿ������selectAds����������һ������ĵķ���
		
		
		
		//���ڴ������ȡ����uiTemplate���ŵ�һ��String������Ϊ���ս��
		String result = uiTemplate;
        String list = "";
		for(Ad ad : adsCandidates)
		{	
			//��adsCandidates����һ��һ���Ѻ�ѡ�Ĺ���ó���
			System.out.println("final selected ad id = " + ad.adId);
			System.out.println("final selected ad rank score = " + ad.rankScore);
			String adContent = adTemplate;
			//����Щ���Ĺؼ���Ϣ������title, brand, url�ȵ��ó������ŵ���adTemplateΪģ���String����
			//Ҳ�������ã�item.html�����placeholder�����Ե㿪�ļ�����ʲô���Ľṹ
			adContent = adContent.replace("$title$", ad.title);
			adContent = adContent.replace("$brand$", ad.brand);
			adContent = adContent.replace("$img$", ad.thumbnail);
			adContent = adContent.replace("$link$", ad.detail_url);
			adContent = adContent.replace("$price$", Double.toString(ad.price));
			//System.out.println("adContent: " + adContent);
			//Ȼ��һ��һ�����浽list����
			list = list + adContent;
		}
		//�ȵ������еĺ�ѡ��涼�ռ����ˣ��Ͱ��ռ��ù�������Ϣ��������Ǹ�list����䵽result��Ҳ����ui.html����
		//��������Կ��������ǰ�list�ŵ���ui.html��Ӧ��placeholder��λ��
		result = result.replace("$list$", list);
		//System.out.println("list: " + list);
		//System.out.println("RESULT: " + result);
		response.setContentType("text/html; charset=UTF-8");
		response.getWriter().write(result);	
	}
}
