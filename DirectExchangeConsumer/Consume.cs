using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace DirectExchangeConsumer;

public class Consume
{
    private IConnection _connection;
    private string exchangeName = "direct-exchange";

    public async Task ConnectAsync()
    {
        ConnectionFactory conn = new ConnectionFactory();

        _connection = await conn.CreateConnectionAsync();
    }


    public async Task ConsumeAsync()
    {
        //using kullanmıyoruz çünkü sürekli açık kalması gerekiyor 
        var ch = await _connection.CreateChannelAsync();

        // Exchange'i tekrar tanımlıyoruz. Bu bir "best practice"tir. 
        //  cünkü publisherdan önce consumer ayaga kalkarsa program hata vermesin diye 
        await ch.ExchangeDeclareAsync(
            exchange: exchangeName,
            ExchangeType.Direct,
            durable: false,
            autoDelete: true,
            arguments: null);


        // QueueDeclareAsync: Tüketici için bir kuyruk oluşturuyoruz.
        // Direct Exchange senaryolarında genelde her tüketicinin kendi geçici kuyruğu olur.
        // Parametreler:
        // - queue: "" (Boş string) -> RabbitMQ'nun rastgele bir isim (örn: amq.gen-Xa2...) atamasını isteriz. [19]
        // - durable: false -> Geçici bir kuyruk olduğu için sunucu restartında silinebilir.
        // - exclusive: true -> Bu kuyruk SADECE bu bağlantıya özeldir. Bağlantı kapanınca kuyruk silinir. [20, 21]
        // - autoDelete: true -> Tüketici aboneliği bitirince kuyruk silinir.

        var x = await ch.QueueDeclareAsync(
            queue: "",
            durable: false,
            autoDelete: true,
            exclusive: false,
            arguments: null);

        // Sunucunun atadığı kuyruk adını alıyoruz (Binding işleminde lazım olacak).
        var queueName = x.QueueName;


        Console.WriteLine($"Geçici kuyruk oluşturuldu. Adı: {queueName}");


        // ADIM 3: Binding (Bağlama) İşlemi
        // Tüketicinin hangi mesajları istediğini burada belirliyoruz.
        // Örnek olarak "error" ve "warning" mesajlarını dinleyelim.
        List<string> bindingKeys = new List<string>()
        {
            "error", "warning",
        };


        foreach (var key in bindingKeys)
        {
            // QueueBindAsync: Exchange ile Kuyruk arasında bir köprü kurar.
            // Mantık: "ExchangeName" isimli exchange'e gelen mesajlardan, 
            // routing key'i "bindingKey" (örn: error) olanları "queueName" kuyruğuna kopyala.
            await ch.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: key, arguments: null);
            Console.WriteLine($"Binding oluşturuldu -> Key: {key}");
        }
        // Not: "info" key'i için binding yapmadığımızdan, üretici "info" gönderirse bu tüketiciye GELMEZ.
        // İşte Direct Exchange'in filtreleme olayı budur. 


        // ADIM 4: QoS (Quality of Service) Ayarları - Prefetch Count
        // BasicQosAsync: Tüketicinin yük dengesini ayarlar.
        // prefetchCount: 1 -> RabbitMQ'ya "Bana aynı anda en fazla 1 tane Onaylanmamış (Unacked) mesaj gönder" diyoruz.
        // Ben o mesajı işleyip Ack gönderene kadar bana yeni mesaj gönderme.
        // Bu, tüketicinin hafızasının şişmesini engeller ve yükü adil dağıtır (Round-robin). 
        await ch.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

        // AsyncEventingBasicConsumer: Olay tabanlı (event-driven) asenkron tüketici sınıfı.

        var consumer = new AsyncEventingBasicConsumer(channel: ch);

        // Received eventi: RabbitMQ kuyruğa bir mesaj bıraktığında bu metod tetiklenir.

        consumer.ReceivedAsync += async (model, ea) =>
        {
            // Mesajın içeriğini (Body) alıyoruz. Memory<byte> olarak gelir.
            var body = ea.Body.ToArray();
            var msg = Encoding.UTF8.GetString(body);

            var routingKey = ea.RoutingKey;
            
            Console.WriteLine($" [x] Alındı -> '{msg}' (Key: {routingKey})");
            
            // Simülasyon: Mesajı işlemek zaman alıyormuş gibi davranıyoruz (I/O işlemi).
            await Task.Delay(250); 

            // ADIM 6: Manuel Onay (Acknowledgement) [11, 24]
            // BasicAckAsync: RabbitMQ'ya "Bu mesajı başarıyla işledim, kuyruktan silebilirsin" bilgisini verir.
            // deliveryTag: Hangi mesajı onayladığımızı belirten ID.
            // multiple: false -> Sadece bu mesajı onayla (öncekileri toplu onaylama).

            await ch.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
        };
        
        
        // ADIM 7: Tüketimi Başlatma (Subscribe)
        // BasicConsumeAsync: Tüketiciyi kuyruğa abone eder.
        // autoAck: false -> Otomatik onayı kapatıyoruz. Yukarıda manuel olarak (BasicAck) yapıyoruz.
        // Bu, güvenilirlik için kritik öneme sahiptir. Tüketici işlerken çökerse mesaj kaybolmaz, kuyruğa döner.
        await ch.BasicConsumeAsync(queue:queueName,autoAck:false,consumer:consumer);

        await Task.Delay(-1);
    }
}