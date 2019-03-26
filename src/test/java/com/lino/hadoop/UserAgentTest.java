package com.lino.hadoop;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author LINO
 * @Title: UserAgentTest
 * @ProjectName hadoop
 * @Description: 单机版日志分析
 * @date 2019/3/23
 */
public class UserAgentTest {
    /**
    　　* @Description: 读取文件
    　　* @author Lino
    　　* @date 2019/3/25
    　　*/
    @Test
    public void readFile() throws Exception {
        String path="D:\\JetBrains\\hadoop\\src\\main\\resources\\log.txt";
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
        String line="";
        UserAgentParser userAgentParser  = new UserAgentParser();
        int i=0;
        Map<String ,Integer> phoneMap = new HashMap<>();

        while(line!=null){
            line = reader.readLine();

            if(StringUtils.isNotBlank(line)){
                i++;
               String source =  line.substring(getCharPostit(line,"\"", 7));
                UserAgent agent = userAgentParser.parse(source);
                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();

                String os = agent.getOs();
                String platform = agent.getPlatform();
                System.out.println("browser: "+browser+"   "+"engine: "+engine+"   "+"engineVersion: "+engineVersion+"   "+"os: "+os+"   "+"platform: "+platform+"   ");
                Integer isHavePhone = phoneMap.get(platform);
                if(isHavePhone!=null){
                    phoneMap.put(platform,phoneMap.get(platform)+1);
                }else{
                    phoneMap.put(platform,1);
                }
            }
        }

        System.out.println("日志处理总数："+i);
        for(Map.Entry<String,Integer> entry:phoneMap.entrySet()){
            System.out.println("phonseOs:"+entry.getKey()+"   "+entry.getValue());
        }

    }
    @Test
    public void testGetChar(){
        String value="183.162.52.7 - - [10/Nov/2016:00:01:02 +0800] \"POST /api3/getadv HTTP/1.1\" 200 813 \"www.imooc.com\" \"-\" cid=0&timestamp=1478707261865&uid=2871142&marking=androidbanner&secrect=a6e8e14701ffe9f6063934780d9e2e6d&token=f51e97d1cb1a9caac669ea8acc162b96 \"mukewang/5.0.0 (Android 5.1.1; Xiaomi Redmi 3 Build/LMY47V),Network 2G/3G\" \"-\" 10.100.134.244:80 200 0.027 0.027";
        int charPostit = getCharPostit(value, "\"", 7);
        System.out.println(charPostit+"   ==============");
    }
/**
　　* @Description: 获取指定字符串的索引地址
　　* @author Lino
　　* @date 2019/3/25
　　*/
    public int getCharPostit(String value,String operator,int index){
        Matcher matcher= Pattern.compile(operator).matcher(value);
        int mInd=0;
        while(matcher.find()){
            mInd++;
            if(mInd==index){
                break;
            }
        }
        return matcher.start();
    }

    @Test
    public void testmUserAgent(){
   /* public static void main(String args[]){*/
        String source="mukewang/5.0.0 (Android 5.1.1; Xiaomi Redmi 3 Build/LMY47V),Network 2G/3G";
        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);
        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();

        String os = agent.getOs();
        String platform = agent.getPlatform();
        System.out.println("browser: "+browser+"   "+"engine: "+engine+"   "+"engineVersion: "+engineVersion+"   "+"os: "+os+"   "+"platform: "+platform+"   ");

    }
}
