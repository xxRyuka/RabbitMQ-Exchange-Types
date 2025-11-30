// See https://aka.ms/new-console-template for more information

using DirectExchangePublisher;

Console.WriteLine("Publisher, Starting!");


Publish pub = new Publish();

await pub.ConnectAsync();

for (int i = 0; i < 15; i++)
{
    await pub.PublishAsync(i.ToString());
}