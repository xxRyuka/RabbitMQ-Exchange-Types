using System.Text;
using RabbitMQ.Client;

namespace DirectExchangePublisher;

public class Publish
{
    private IConnection _connection;

    private const string exchangeName = "direct-exchange";

    // ADIM 1: Bağlantı Fabrikasının (ConnectionFactory) Yapılandırılması
    // ConnectionFactory, RabbitMQ sunucusuna (Broker) yapılacak TCP bağlantısının 
    // konfigürasyon ayarlarını barındıran nesnedir.
    public async Task ConnectAsync()
    {
        _connection = await new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
                UserName = "guest",
                Password = "guest",
            }
            // ADIM 2: TCP Bağlantısının ve Kanalın Oluşturulması
            // CreateConnectionAsync: Fabrika ayarlarını kullanarak fiziksel TCP bağlantısını asenkron olarak açar.
            // "using" anahtar kelimesi, IDisposable arayüzünü uygulayan bu nesnenin 
            // blok sonunda bellekten temizlenmesini ve bağlantının kapatılmasını garanti eder. 
            .CreateConnectionAsync();
    }


    public async Task PublishAsync(string message)
    {
        // CreateChannelAsync: Açık olan TCP bağlantısı üzerinden sanal bir kanal (AMQP Channel) oluşturur.
        // Tüm mesajlaşma işlemleri (Publish, Consume, Declare) bu kanal üzerinden yürütülür.
        // Kanal oluşturmak ucuzdur, bağlantı oluşturmak pahalıdır. 
        // Multiplexing yapiyoruz TCP bağlantısında sanal tüneller açarak çoğullama yapıyoruz
        using var channel = await _connection.CreateChannelAsync();


        // ADIM 3: Exchange (Santral) Tanımlama (Topology Declaration)
        // ExchangeDeclareAsync: RabbitMQ üzerinde bir exchange'in varlığını garanti altına alır.
        // Eğer exchange yoksa oluşturur; varsa ve parametreleri aynıysa işlem yapmaz (idempotent).

        await channel.ExchangeDeclareAsync(
            exchange: exchangeName, // - exchange: Exchange'in adı (ExchangeName sabiti).
            type: ExchangeType
                .Direct, // - type: ExchangeType.Direct -> Mesajın routing key'i ile kuyruğun binding key'i TAM EŞLEŞMELİDİR. 
            durable: false, // - durable: false -> Sunucu yeniden başlatılırsa bu exchange silinir. (Kalıcılık için true olmalı).
            autoDelete: true, // - autoDelete: false -> Kimse kullanmadığında otomatik silinmez.
            arguments: null // - arguments: null -> Ekstra eklenti argümanı yok.
        );


        // ADIM 4: Geri İade (BasicReturn) Mekanizmasının Kurulması
        // "Mandatory" flag true olarak set edildiğinde, eğer mesaj hiçbir kuyruğa yönlendirilemezse
        // sunucu mesajı bize geri gönderir. Bu olayı yakalamak için BasicReturn event'ine abone oluyoruz.
        channel.BasicReturnAsync += async (sender, ea) =>
        {
            // Geri dönen mesajın gövdesini byte dizisinden string'e çeviriyoruz.
            var returnedMessage = Encoding.UTF8.GetString(ea.Body.ToArray());

            // ReplyText: Sunucunun neden iade ettiğini açıklayan hata kodu/mesajı (örn: NO_ROUTE).
            Console.WriteLine($" Mesaj iletilemedi ve iade edildi: '{returnedMessage}'");
            Console.WriteLine($" -> Neden: {ea.ReplyText}, Kod: {ea.ReplyCode}, RoutingKey: {ea.RoutingKey}");
        };


        Console.Write("Routing Key > ");
        string rk = Console.ReadLine();


        // RabbitMQ metin değil, binary veri (byte array) taşır. 
        // Bu yüzden string mesajımızı UTF-8 encoding ile byte dizisine çeviriyoruz. 
        byte[] msgByte = Encoding.UTF8.GetBytes(message);


        // BasicProperties: Mesajın meta verilerini (headerlarını) tanımlar.
        // Yeni bir properties nesnesi oluşturuyoruz. 
        var properties = new BasicProperties
        {
            // Persistent = true: Mesajın diskte saklanmasını talep eder (DeliveryMode = 2).
            // Kuyruk da durable ise, RabbitMQ restart olsa bile mesaj kaybolmaz.
            Persistent = true,

            // ContentType: Mesajın içeriği hakkında tüketiciye bilgi verir (MIME type).
            ContentType = "text/plain",

            // MessageId: Mesajın tekil kimliği. İzleme (tracing) ve deduplication için kullanılır.
            MessageId = Guid.NewGuid().ToString(),

            // AppId: Mesajı üreten uygulamanın adı.
            AppId = "ProducerApp_v1"
        };


        // ----------------------------------------------------------------------------------
        // ADIM 6: Mesajın Yayınlanması (Publishing)
        // ----------------------------------------------------------------------------------

        // BasicPublishAsync: Mesajı exchange'e gönderir.
        // Parametreler:
        // - exchange: Hedef exchange adı.
        // - routingKey: Mesajın yönlendirme anahtarı. Direct exchange bunu kullanarak kuyruk seçer.
        // - mandatory: true -> Mesaj yönlendirilemezse BasicReturn tetiklensin. (false olursa sessizce silinir). [11, 17]
        // - basicProperties: Yukarıda tanımladığımız meta veriler.
        // - body: Mesajın kendisi (byte array).

        await channel.BasicPublishAsync(
            exchange: exchangeName,
            routingKey: rk,
            mandatory: true,
            basicProperties: properties,
            body: msgByte);

        Console.WriteLine($" [x] Gönderildi -> Key: '{rk}', Mesaj: '{message}'");
    }
}