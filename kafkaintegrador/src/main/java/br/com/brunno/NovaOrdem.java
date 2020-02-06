package br.com.brunno;

import java.util.Objects;
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
            var mensagem = "roupa,vendido";
            var record = new ProducerRecord<String, String>("ECOMMERCE_NOVA_ORDEM", chave, mensagem);
            //producer.send(record); Forma assincrona de processamento
            producer.send(record, (metada,ex) -> {
                if (Objects.nonNull(ex)){
                    ex.printStackTrace();
                    return;
                }
                System.out.println("sucesso enviado--> " + metada.topic() + ":::partition " + metada.partition() + "/ offset "+ metada.offset());
            }).get(); //Implementado a INTERFACE Callback com uma classe Anonima.
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