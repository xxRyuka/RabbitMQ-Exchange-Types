using System.ComponentModel.DataAnnotations;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

ConnectionFactory factory = new ConnectionFactory()
{
    HostName = "localhost",
    UserName = "guest",
    Password = "guest",
};
const string exchangeName = "fanout-exchange";


var connection = await factory.CreateConnectionAsync();

var channel = await connection.CreateChannelAsync();


// 1. Exchange Tanımlama
// Consumer da Exchange'i tanımlamalıdır. Çünkü Consumer, Producer'dan önce başlatılabilir.
// Eğer exchange yoksa bind işlemi başarısız olur.
await channel.ExchangeDeclareAsync(exchangeName, ExchangeType.Fanout, durable: false, arguments: null,
    autoDelete: false);


// 2. Geçici Kuyruk (Temporary Queue) Oluşturma
// Parametre vermeden çağrılan QueueDeclareAsync() şu özelliklerde bir kuyruk yaratır:
// - Non-durable (Sunucu kapanırsa silinir)
// - Exclusive (Sadece bu connection kullanabilir)
// - Auto-delete (Connection koparsa kuyruk silinir)
// - Rastgele İsim (Örn: amq.gen-JzTY20BRgKO...)
// Fanout deseninde her consumer'ın KENDİNE AİT kuyruğu olması şarttır.
var queue = await channel.QueueDeclareAsync();

string queueName = queue.QueueName;
Console.WriteLine($"[*] Geçici kuyruk oluşturuldu: {queueName}");

// await channel.QueueDeclareAsync(queue:queueName, durable: true, exclusive: false, autoDelete: false, arguments: null);
await channel.QueueDeclareAsync(queue: "second", durable: true, exclusive: false, autoDelete: false, arguments: null);
// 3. Bağlama (Binding)
// Kuyruğu Exchange'e bağlayarak "bu exchange'e gelen mesajların bir kopyasını bana ver" diyoruz.
await channel.QueueBindAsync(queue: queueName, exchange: exchangeName, routingKey: string.Empty);
Console.WriteLine(" [*] Loglar bekleniyor...");


// 4. Asenkron Tüketici Tanımlama
// v7.0 ile gelen AsyncEventingBasicConsumer.
var consumer = new AsyncEventingBasicConsumer(channel);


await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);
consumer.ReceivedAsync += async (model, ea) =>
{
    // 5. Mesajın Okunması (ReadOnlyMemory<byte>)
    // ea.Body v7.0'da ReadOnlyMemory<byte> türündedir.
    // ToArray() metodu veriyi yeni bir byte dizisine kopyalar.
    // Span<byte> ile çalışmak daha performanslıdır ancak Encoding.GetString için array veya pointer gerekir.
    var body = ea.Body.ToArray();
    var msg = Encoding.UTF8.GetString(body);
    Console.WriteLine($" [x] Alındı: {msg}");
    Console.WriteLine("Kuyruk adı : " + queueName);


    await Task.Delay(250);


    try
    {
        if (!(msg != string.Empty && msg.Length > 3))
        {
            throw new ValidationException($"msg lenght : {msg.Length} < 0 oldugu için mesaj kabul edilmedi");
        }

        Console.WriteLine($"mesaj uzunlugu kabul edilebilir bir degerde mesaj alındı {msg.Length} \n");
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch (ValidationException ve)
    {
        Console.WriteLine(ve.Message);
        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
    }
};

// 6. Tüketimi Başlatma
// autoAck: true -> Mesaj tüketiciye ulaştığı an RabbitMQ kuyruktan siler.
// Loglama gibi kayıpların tolere edilebildiği senaryolarda performans için autoAck=true kullanılır.
await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer: consumer);


Console.ReadLine();