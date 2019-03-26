import com.alibaba.fastjson.JSON

object JsonParseTesst {
  def main(args: Array[String]): Unit = {
    val data = "{\"sites\":{\"site\":[{\"id\":\"1\",\"name\":\"菜鸟教程\",\"url\":\"www.runoob.com\"},{\"id\":\"2\",\"name\":\"菜鸟工具\",\"url\":\"c.runoob.com\"},{\"id\":\"3\",\"name\":\"Google\",\"url\":\"www.google.com\"}]}}"

    val jsonObject = JSON.parseObject(data)

    val sites = jsonObject.getJSONObject("sites")

    val site = sites.getJSONArray("site")

    for (i <- 0 until site.size()){
      val website = site.getJSONObject(i)
      val id  = website.getString("id")
      val name = website.getString("name")
      val url = website.getString("url")

      println(s"$id \t $name \t $url")
    }
  }

}
