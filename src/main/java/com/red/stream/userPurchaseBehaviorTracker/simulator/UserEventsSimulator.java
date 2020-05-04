package com.red.stream.userPurchaseBehaviorTracker.simulator;

import com.cloudwise.sdg.dic.DicInitializer;
import com.cloudwise.sdg.template.TemplateAnalyzer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;

/**
 * 模拟产生数据
 *
 * @author red
 * @create 2019-06-23-16:37
 */
public class UserEventsSimulator {
    public static void main(String[] args) throws Exception{
        //加载词典(只需执行一次即可)
        DicInitializer.init();

        //编辑模版
        String userEventTpl = "{\"userId\":\"$Dic{userId}\",\"channel\":\"$Dic{channel}\",\"eventType\":\"$Dic{eventType}\",\"eventTime\":\"$Dic{eventTime}\",\"data\":{\"productId\":$Dic{productId}}}";

        String purchaseUserEventTpl = "{\"userId\":\"$Dic{userId}\",\"channel\":\"$Dic{channel}\",\"eventType\":\"PURCHASE\",\"eventTime\":\"$Dic{eventTime}\",\"data\":{\"productId\":$Dic{productId},\"price\":$Dic{price},\"amount\":$Dic{amount}}}";

        //创建模版分析器
        TemplateAnalyzer userEventTplAnalyzer = new TemplateAnalyzer("userEvent", userEventTpl);

        TemplateAnalyzer purchaseUserEventTplAnalyzer = new TemplateAnalyzer("purchaseUserEventTpl", purchaseUserEventTpl);

//        System.out.println("---------------------模拟数据！--------------------"+userEventTplAnalyzer.analyse());

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.28.128:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record;
        String userEvent ="{\"userId\":\"64\",\"channel\":\"APP\",\"eventType\":\"ADD_TO_CART\",\"eventTime\":\"1581677659763\",\"data\":{\"productId\":148}}";
        String purchase = "{\"userId\":\"64\",\"channel\":\"APP\",\"eventType\":\"PURCHASE\",\"eventTime\":\"1581677659761\",\"data\":{\"productId\":135,\"price\":300,\"amount\":37}}";
        for(int i=1;i<=100000;i++){
            //分析模版生成模拟数据
            //打印分析结果
            System.out.println("##########################1111"+userEventTplAnalyzer.analyse());
            System.out.println("##########################1111"+userEvent);
            record = new ProducerRecord<>(
                    "purchasePathAnalysisInPut",
                    null,
                    new Random().nextInt()+"",
                    userEvent);
            producer.send(record);
            long sleep = (long) (Math.random()*2000);
            Thread.sleep(sleep);

            if(sleep%2==0&&sleep>800){

                System.out.println("###########################2222"+purchaseUserEventTplAnalyzer.analyse());
                System.out.println("###########################2222"+purchase);
                record = new ProducerRecord<>(
                        "purchasePathAnalysisInPut",
                        null,
                        new Random().nextInt()+"",
                        purchase);
                producer.send(record);

            }
            System.out.println("------数据发送完成------");
        }
    }
}
