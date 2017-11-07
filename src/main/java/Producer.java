import com.yahoo.labs.samoa.instances.Instance;
import moa.streams.ArffFileStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;


/**
 * Created by loezerl-fworks on 17/10/17.
 */

public class Producer {

    public static String DIABETES_DATABASE = "/home/loezerl-fworks/IdeaProjects/Experimenter/diabetes.arff";
    public static String KYOTO_DATABASE = "/home/loezerl-fworks/Downloads/kyoto.arff";

    public static void main(String[] args) throws IOException {

        // set up the producer
        KafkaProducer<String, String> producer;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 0);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);

        try {
            ArffFileStream file = new ArffFileStream(KYOTO_DATABASE, -1);
            long i =0;
            while(file.hasMoreInstances() && i < 10000000){
                Instance element = file.nextInstance().getData();
                producer.send(new ProducerRecord<String, String>(
                    "instances",
                        InstanceSerializer.Serialize(element)
                ));
                i++;
                System.out.println("Sent instance: " + i);
            }
        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            producer.close();
        }

    }

    public static class InstanceSerializer{
        public static String Serialize(Instance a){
            StringBuffer instance = new StringBuffer();
            for(int i=0; i<a.numAttributes(); i++){
                if(i == a.numAttributes()-1)
                    instance.append(a.value(i));
                else
                    instance.append(a.value(i) + ", ");
            }
            return instance.toString();
        }
    }
}
