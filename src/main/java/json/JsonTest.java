package json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class JsonTest {
    public static void main(String[] args) {
        //json的解析
        String info = "{\"stuInfo\":[{\"name\":\"张三\",\"age\":14,\"gender\":\"male\"},{\"name\":\"李四\",\"age\":15,\"gender\":\"female\"}]}";
        String stu = "{'name':'lisa','age':12}";

        JSONObject obj = JSON.parseObject(info);
        JSONArray stuInfo =obj.getJSONArray("stuInfo");

       JSONObject zhangsan = stuInfo.getJSONObject(0);
       JSONObject lisi = stuInfo.getJSONObject(1);

      int age = zhangsan.getIntValue("age");
      String gender = lisi.getString("gender");

        System.out.println("张三的年龄是："+age);
        System.out.println("李四的性别是: "+gender);

        JSONObject  obj2 = JSONObject.parseObject(stu);
        System.out.println(obj2.getString("name")+"  "+ obj2.getIntValue("age"));

        //json封装
        JSONObject obj3 = new JSONObject();
        JSONObject obj4 = new JSONObject();
        obj4.put("site1","www.hao123.com");
        obj4.put("site2","www.baidu.com");
        obj4.put("site3","www.taobao.com");

        obj3.put("name","jsonTest");
        obj3.put("count",2);

        obj3.put("sites",obj4);
        System.out.println(obj3.toJSONString());

        obj4.remove("site1");
        System.out.println(obj3.toString());

    }
}
