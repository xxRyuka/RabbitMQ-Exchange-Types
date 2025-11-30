using System.Text;
using RabbitMQ.Client;

ConnectionFactory connectionFactory = new ConnectionFactory
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest",
};

// TCP bağlantısını acıyoruz 
using var connection = await connectionFactory.CreateConnectionAsync();

const string exchangeName = "fanout-exchange";

var ch = await connection.CreateChannelAsync();


// Producer, göndereceği yerin varlığından emin olmalıdır.
// ExchangeType.Fanout: Mesajın tüm kuyruklara kopyalanacağını belirten kritik parametre.
// durable: false (Geçici exchange, sunucu restart olursa silinir. Kalıcılık için true olmalı.)
// autoDelete: false (Bağlı kuyruk kalmayınca silinmesin)
await ch.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout, durable: false, arguments: null, autoDelete: false);

Console.WriteLine("Log sistemi Producer aktif. Mesaj yazıp Enter'a basın (Çıkış: 'exit').");

while (true)
{
    var message = Console.ReadLine();
    if (string.IsNullOrWhiteSpace(message) || message.ToLower() == "exit") break;

    // Mesajın Hazırlanması (Encoding)
    // RabbitMQ byte array (binary) taşır. UTF8 encoding evrensel standarttır.
    ReadOnlyMemory<byte> body = Encoding.UTF8.GetBytes(message);

    // Mesajın Yayınlanması (BasicPublishAsync)
    // Bu metot v7.0'ın en önemli değişikliğidir.
    // exchange: "logs" -> Hedef exchange.
    // routingKey: "" -> Fanout exchange routing key'i görmezden gelir. Boş string standarttır.
    // mandatory: false -> Mesaj bir kuyruğa gitmezse (kimse dinlemiyorsa) hata verme, sil.
    // basicProperties: null -> Mesaj kalıcılığı (Persistent) gibi ayarlar burada yapılır.
    
    BasicProperties props = new BasicProperties();
    props.Persistent = true;
    
    // exchange: "logs" -> Hedef exchange.
    // routingKey: "" -> Fanout exchange routing key'i görmezden gelir. Boş string standarttır.
    // mandatory: false -> Mesaj bir kuyruğa gitmezse (kimse dinlemiyorsa) hata verme, sil.
    // => true yaptım ve BasicReturn Eventini yukarda tanımladım
    // basicProperties: null -> Mesaj kalıcılığı (Persistent) gibi ayarlar burada yapılır.
    await ch.BasicPublishAsync(
        exchange: exchangeName,
        routingKey: "",
        body: body,
        mandatory: true,
        basicProperties: props);

    Console.WriteLine("mesaj gonderildi");
    
    
    // Geri Gelen mesajların loglanmasini yapiyoruz burda 
    ch.BasicReturnAsync += async (sender, ea) =>
    {
        Console.WriteLine("\n @@@@@@@ Mesaj Hergangi Bir Kuyruga Gonderilemedi @@@@@@@ \n");
        Console.WriteLine($"Reply Text: {ea.ReplyText}");
        var body = Encoding.UTF8.GetString(ea.Body.ToArray());
        Console.WriteLine($"Body: {body}");
        var exchange = ea.Exchange;
        var routingKey = ea.RoutingKey;
        var replyCode = ea.ReplyCode;
        Console.WriteLine($" Exchange : {exchange} \n RoutingKey : {routingKey} \n ReplyCode : {replyCode}");
    };
}
