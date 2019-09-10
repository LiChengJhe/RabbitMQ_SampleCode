using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApp
{


    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
        }
        public void Publisher()
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    channel.ExchangeDeclare(exchange: "test-queue-direct", type: ExchangeType.Direct, durable: true, autoDelete: false);
                    channel.QueueDeclare(queue: "test-queue", durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueBind(queue: "test-queue", exchange: "test-queue.direct", routingKey: "routing-1");

                    string message = "Hello World";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "test-queue.direct",
                                         routingKey: "routing-1",
                                         basicProperties: null,
                                         body: body);
                    Console.WriteLine($"Sent:{message}");
                }
            }

        }
        public void Receiver()
        {

            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost", DispatchConsumersAsync = true };
            IConnection connection = factory.CreateConnection();
            IModel channel = connection.CreateModel();

            channel.QueueDeclare(queue: "test-queue", durable: false, exclusive: false, autoDelete: false, arguments: null);

            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            consumer.Received += (sender, e) =>
           {
               byte[] body = e.Body;
               string message = Encoding.UTF8.GetString(body);
               Console.WriteLine($"Received:{message}");
                //channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
                //channel.BasicNack(deliveryTag:e.DeliveryTag, multiple:false, requeue:true);
            };
            channel.BasicConsume(queue: "test-queue", autoAck: true, consumer: consumer);

        }
        public void ReceiverAsync()
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost", DispatchConsumersAsync = true };
            IConnection connection = factory.CreateConnection();
            IModel channel = connection.CreateModel();

            channel.QueueDeclare(queue: "test-queue", durable: false, exclusive: false, autoDelete: false, arguments: null);

            AsyncEventingBasicConsumer consumer = new AsyncEventingBasicConsumer(channel);
            consumer.Received += async (sender, e) =>
            {
                byte[] body = e.Body;
                string message = Encoding.UTF8.GetString(body);
                await Task.Delay(5000);
                Console.WriteLine($"Received:{message}");
                    //channel.BasicAck(deliveryTag: e.DeliveryTag, multiple: false);
                    //channel.BasicNack(deliveryTag:e.DeliveryTag, multiple:false, requeue:true);
                };
            channel.BasicConsume(queue: "test-queue", autoAck: true, consumer: consumer);


        }
    }
}
