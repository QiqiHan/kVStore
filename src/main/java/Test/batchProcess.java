package Test;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;

/**
 * Created by H77 on 2017/11/14.
 */
public class batchProcess implements Runnable {
    int num;
    Client client;
    String[] urls;
    public batchProcess(int num){
        this.num = num;
        client = new Client();
        urls = new String[]{
                "http://localhost:8500/batchProcess", "http://localhost:8501/batchProcess","http://localhost:8502/batchProcess"
//                "http://192.168.0.104:8500/batchProcess", "http://192.168.0.103:8500/batchProcess","http://192.168.0.105:8500/batchProcess"
        };
    }

    @Override
    public void run() {
        for(int i = 0 ; i < 1000 ; i+=1){
            JSONObject js = new JSONObject();
            for(int j = 0 ; j <400 ;j++){
                int key = num*i*400+j;
                String name = key+"";
                HashMap<String,String> maps = new HashMap<>();
                maps.put("aaa"+key,"bbb");
                maps.put("bbb"+key,"ccc");
                try {
                    js.put(name,maps);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("Thread"+num+"Post"+i);
            client.postForm(urls[i%3],js);
        }
    }

    public static void main(String[] args){
        for(int i =1 ; i < 5 ;i++){
            Thread r = new Thread(new batchProcess(i));
            r.start();
        }
        System.out.println("end");
    }

}
