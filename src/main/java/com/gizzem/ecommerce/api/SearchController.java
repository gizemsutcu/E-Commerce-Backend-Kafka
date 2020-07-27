package com.gizzem.ecommerce.api;

import com.gizzem.ecommerce.SearchProducer;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RestController
public class SearchController {

    @Autowired
    SearchProducer searchProducer;

    //For Spark Batch Analysis
    @GetMapping("/search")
    public void searchIndex(@RequestParam String term){
        List<String> cities = Arrays.asList("İstanbul", "Ankara", "Bursa", "İzmir", "Malatya", "Konya");
        List<String> products = Arrays.asList("defter", "kalem", "fincan", "bardak", "elbise", "ayakkabı", "çorap",
                "bilgisayar", "iphone", "koltuk", "masa", "sandalye", "parfüm", "deterjan");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        while (true){
            Random random = new Random();
            int i = random.nextInt(cities.size());
            int j = random.nextInt(products.size());

            long offset = Timestamp.valueOf("2020-07-25 02:00:00").getTime();
            long end = Timestamp.valueOf("2020-07-25 23:59:00").getTime();
            long diff = end - offset + 1;
            Timestamp rand = new Timestamp(offset + (long)(Math.random() * diff));

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("userId",random.nextInt(15000-1000)+1000);
            jsonObject.put("search",products.get(j));
            jsonObject.put("current_ts",rand.toString());
            jsonObject.put("region",cities.get(i));

            System.out.println(jsonObject.toJSONString());
            searchProducer.send(jsonObject.toJSONString());
        }
    }

    //For Spark Streaming Analysis
    @GetMapping("/search/stream")
    public void searchIndexStream(@RequestParam String term){
        List<String> cities = Arrays.asList("İstanbul", "Ankara", "Bursa", "İzmir", "Malatya", "Konya");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        Random random = new Random();
        int i = random.nextInt(cities.size());

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("userId",random.nextInt(15000-1000)+1000);
        jsonObject.put("search",term);
        jsonObject.put("current_ts",timestamp.toString());
        jsonObject.put("region",cities.get(i));

        searchProducer.send(jsonObject.toJSONString());
    }
}
