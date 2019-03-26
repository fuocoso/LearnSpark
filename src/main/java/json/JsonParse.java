import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class JsonParse {
    public static void main(String[] args) {
        String data = "{\"sites\":{\"site\":[{\"id\":\"1\",\"name\":\"菜鸟教程\",\"url\":\"www.runoob.com\"},{\"id\":\"2\",\"name\":\"菜鸟工具\",\"url\":\"c.runoob.com\"},{\"id\":\"3\",\"name\":\"Google\",\"url\":\"www.google.com\"}]}}";

        JSONObject jsonObject = JSONObject.parseObject(data);

        JSONObject sites = jsonObject.getJSONObject("sites");

        JSONArray site = sites.getJSONArray("site");
        site.size();

        for (int i = 0;i<site.size();i++){
            JSONObject site2 =  site.getJSONObject(i);

            String id = site2.getString("id");
            String name = site2.getString("name");
            String url = site2.getString("url");

            System.err.println(id+"\t"+name+"\t"+url);
        }




        JSONObject js1 = new JSONObject();
        js1.put("name","百度");
        js1.put("url","www.baidu.com");

        JSONObject js2 = new JSONObject();
        js2.put("name","京东");
        js2.put("url","www.jd.com");

        JSONObject js3 = new JSONObject();
        js3.put("name","淘宝");
        js3.put("url","www.taobao.com");

        JSONArray sitesArray = new JSONArray();
        sitesArray.add(0,js1);
        sitesArray.add(1,js2);
        sitesArray.add(2,js3);


        JSONObject res = new JSONObject();
        res.put("sites",sitesArray);

        //System.err.println(res.toJSONString());

    }
}
