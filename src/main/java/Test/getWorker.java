package Test;

import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;
import java.util.Random;

/**
 * Created by H77 on 2017/11/5.
 */
public class getWorker implements Runnable{
    int num;
    Client client;
    String[] urls;
    public getWorker(int num) {
        this.num = num;
        client = new Client();
        urls = new String[]{
        "http://localhost:8500/process", "http://localhost:8501/process","http://localhost:8502/process"
//                "http://192.168.0.104:8500/process", "http://192.168.0.103:8500/process","http://192.168.0.105:8500/process"
        };
    }

    public void run() {
        Random r = new Random();
        int count = 0;
        int miss = 0;
        for (int i = 1 ; i < 2024 ;i+=1) {
            JSONObject js = new JSONObject();
            try {
                int tag = num*2024+i;
                String key = "key"+tag;
                String url = urls[r.nextInt(3)];
                String result = client.httpGet(key,url);
                count++;
                if(result == null || result.equals("find nothing")){
                    System.out.println(url);
                    System.out.println(key);
//                    System.out.println(result);
                    int num = ( Integer.parseInt(key.substring(3,key.length())) % 2024) % 3;
                    System.out.println(num);
                    miss++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("total"+count);
        System.out.println("miss"+miss);
    }
    public static void main(String[] args){
        for(int i = 0 ; i < 3 ;i++){
            Thread r = new Thread(new getWorker(i));
            r.start();
        }
        System.out.println("end");
    }

}
