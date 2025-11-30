// See https://aka.ms/new-console-template for more information

using DirectExchangeConsumer;

Console.WriteLine("Consumer, Starting");

Consume consumer = new Consume();

await consumer.ConnectAsync();

await consumer.ConsumeAsync();