using System.Text;
using RabbitMQ.Client;

ConnectionFactory factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest",
};
var connectionAsync = await factory.CreateConnectionAsync();


var ch = await connectionAsync.CreateChannelAsync();

await ch.ExchangeDeclareAsync(
    exchange: "topic-logs",
    durable: true,
    autoDelete: false,
    arguments: null,
    type: ExchangeType.Topic
);

Console.WriteLine("Log göndermek için format: <routingKey> <mesaj>");
Console.WriteLine("Örnek: 'backend.error.payment Ödeme servisi çöktü'");
Console.WriteLine("Çıkış için 'exit' yazın.\n");
while (true)
{
    var input = Console.ReadLine();
    if (input == "exit") break;

    // Girdiyi RoutingKey ve Mesaj olarak ayır
    var parts = input!.Split(' ', 2);

    var routingKey = parts[0];
    var message = parts[1];

    var body = Encoding.UTF8.GetBytes(message);

    BasicProperties properties = new BasicProperties();
    properties.Persistent = true;
    // Mesajı Exchange'e gönder (Routing Key burada kritik)
    await ch.BasicPublishAsync(
        exchange: "topic-logs",
        routingKey: routingKey,
        basicProperties: properties,
        mandatory: true,
        body: body
    );

    Console.WriteLine($"[x] Gönderildi -> Key: '{routingKey}', Mesaj: '{message}'");


}
    ch.BasicReturnAsync += async (model, ea) =>
    {
        var replyText = ea.ReplyText;
        var replyCode = ea.ReplyCode;
        var routingKey = ea.RoutingKey;
        var msg = Encoding.UTF8.GetString(ea.Body.ToArray());


        Console.WriteLine("Message Returned");
        Console.WriteLine($"Props \n" +
                          $"msg: {msg} \n" +
                          $"replyText : {replyText} \n" +
                          $"replyCode : {replyCode} \n" +
                          $"routingKey : {routingKey} \n");
    };
