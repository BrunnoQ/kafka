package br.com.brunno;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Realiza adicao de mensagens no tópico criado no Kafka
 */
public class NovaOrdem {

    public static void main(String[] args) {
        try (var producer = new KafkaProducer<String, String>(propiedades())) {
            var chave = "1";
            var mensagem = "tenis,vendido";
            var record = new ProducerRecord<String, String>("ECOMMERCE_NOVA_ORDEM", chave, mensagem);
            producer.send(record);
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    private static Properties propiedades() {

        var propiedades = new Properties();
        propiedades.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        propiedades.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());// serializar
                                                                                                              // a CHAVE
                                                                                                              // da
                                                                                                              // propriedade
        propiedades.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());// serializar
                                                                                                                // o
                                                                                                                // VALOR
                                                                                                                // da
                                                                                                                // propriedade

        return propiedades;
    }

}