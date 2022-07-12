package bootcamp;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

class myCallBack implements Callback {

    private static final Logger LOG = LoggerFactory.getLogger(myCallBack.class);

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if(Objects.isNull(exception)) {
            System.out.println("onCompletion Topic " + metadata.topic() + " partition " + metadata.partition()
                    + " Offset " + metadata.offset());
        }else {
            System.out.println("Error ");
            exception.printStackTrace();
        }
    }
}