using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;


// Uygulama başlarken hangi konuyu dinlemek istediğimizi soralım
if (args.Length < 1)
{
    Console.Error.WriteLine("Kullanım: dotnet run -- <binding_key>");
    Console.Error.WriteLine("Örnek: dotnet run -- backend.#");
    Environment.ExitCode = 1;
    return;
}
var lsit = args;

foreach (var arg in args)
{
    Console.WriteLine(arg);
}
// Komut satırından gelen desen (* veya # içerebilir)
var bindingKey = args[0];

ConnectionFactory factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest",
};
var connectionAsync = await factory.CreateConnectionAsync();


var ch = await connectionAsync.CreateChannelAsync();

//(Publisher ile aynı olmalı)
// Consumer publisherdan erken calısırsa hata almamak için exchangeyi burdada tanımlıyoruz 
await ch.ExchangeDeclareAsync(
    exchange: "topic-logs",
    durable: true,
    autoDelete: false,
    arguments: null,
    type: ExchangeType.Topic
);


var queueDeclareOk = await ch.QueueDeclareAsync();
var queueName = queueDeclareOk.QueueName;


// 3. KRİTİK NOKTA: Kuyruğu Exchange'e bağla (Binding)
// Burada Exchange'e diyoruz ki: "Şu desene uyan mesajları benim kuyruğuma yönlendir."
await ch.QueueBindAsync(queue: queueName,
    exchange: "topic-logs",
    routingKey: bindingKey);

Console.WriteLine($"[*] '{bindingKey}' deseni ile dinleniyor. Mesajlar bekleniyor...");

var consumer = new AsyncEventingBasicConsumer(ch);
consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = Encoding.UTF8.GetString(body);
    var routingKey = ea.RoutingKey;
    Console.WriteLine($" [x] ALINDI | Key: {routingKey} | Msg: {message}");

    await ch.BasicAckAsync(
        deliveryTag: ea.DeliveryTag,
        multiple: false);
};

await ch.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);

Console.WriteLine(" Çıkış için [enter] tuşuna basın.");
Console.ReadLine();