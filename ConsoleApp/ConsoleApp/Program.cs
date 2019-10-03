using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
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

        public void DirectPublisher()
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost", RequestedHeartbeat = 60, AutomaticRecoveryEnabled = true };
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "queue-1", durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: "queue-2", durable: false, exclusive: false, autoDelete: false, arguments: null);

                    channel.ExchangeDeclare(exchange: "queue-direct", type: ExchangeType.Direct, durable: true, autoDelete: false);

                    channel.QueueBind(queue: "queue-1", exchange: "queue.direct", routingKey: "xml");
                    channel.QueueBind(queue: "queue-2", exchange: "queue.direct", routingKey: "json");

                    string message = "Hello World";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "queue.direct",
                                         routingKey: "xml",
                                         basicProperties: null,
                                         body: body);

                    channel.BasicPublish(exchange: "queue.direct",
                                routingKey: "json",
                                basicProperties: null,
                                body: body);
                }
            }

        }

        public void TopicPublisher()
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "queue-1", durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: "queue-2", durable: false, exclusive: false, autoDelete: false, arguments: null);

                    channel.ExchangeDeclare(exchange: "queue-topic", type: ExchangeType.Topic, durable: true, autoDelete: false);

                    channel.QueueBind(queue: "queue-1", exchange: "queue.topict", routingKey: "*.json");
                    channel.QueueBind(queue: "queue-2", exchange: "queue.topic", routingKey: "#.json");

                    string message = "Hello World";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "queue.topic",
                                         routingKey: "data.json",
                                         basicProperties: null,
                                         body: body);

                    channel.BasicPublish(exchange: "queue.topic",
                                routingKey: "my.data.json",
                                basicProperties: null,
                                body: body);
                }
            }

        }


        public void FanoutPublisher()
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "queue-1", durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: "queue-2", durable: false, exclusive: false, autoDelete: false, arguments: null);

                    channel.ExchangeDeclare(exchange: "queue-fanout", type: ExchangeType.Fanout, durable: true, autoDelete: false);

                    channel.QueueBind(queue: "queue-1", exchange: "queue.fanout", routingKey: null);
                    channel.QueueBind(queue: "queue-2", exchange: "queue.fanout", routingKey: null);

                    string message = "Hello World";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "queue.fanout",
                                         routingKey: null,
                                         basicProperties: null,
                                         body: body);
                }
            }

        }

        public void HeaderPublisher()
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            using (IConnection connection = factory.CreateConnection())
            {
                using (IModel channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "queue-1", durable: false, exclusive: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(queue: "queue-2", durable: false, exclusive: false, autoDelete: false, arguments: null);

                    channel.ExchangeDeclare(exchange: "queue-header", type: ExchangeType.Headers, durable: true, autoDelete: false);

                    Dictionary<string, object> xmlHeader = new Dictionary<string, object>{
                        { "format", "xml" },
                        { "type", "report" },
                        { "x-match", "all" }
                    };
                    channel.QueueBind(queue: "queue-1", exchange: "queue.header", routingKey: null, arguments: xmlHeader);

                    Dictionary<string, object> jsonHeader = new Dictionary<string, object> {
                        { "format", "json" },
                        { "type", "report" },
                        { "x-match", "any" }
                    };
                    channel.QueueBind(queue: "queue-2", exchange: "queue.header", routingKey: null, arguments: jsonHeader);

                    string message = "Hello World";
                    var body = Encoding.UTF8.GetBytes(message);


                    Dictionary<string, object> myHeader = new Dictionary<string, object>
                    {
                        { "type", "report" }
                    };

                    IBasicProperties properties = channel.CreateBasicProperties();
                    properties.Persistent = true;
                    properties.Headers = myHeader;
                    channel.BasicPublish(exchange: "queue.header",
                                         routingKey: null,
                                         basicProperties: properties,
                                         body: body);
                }
            }

        }


        public void Publisher()
        {
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost", RequestedHeartbeat = 60, AutomaticRecoveryEnabled = true };
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

            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost", RequestedHeartbeat = 60, AutomaticRecoveryEnabled = true };
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
            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost", DispatchConsumersAsync = true, RequestedHeartbeat = 60, AutomaticRecoveryEnabled = true };
            IConnection connection = factory.CreateConnection();
            IModel channel = connection.CreateModel();
            channel.BasicQos(prefetchCount: 1, prefetchSize: 1, global: true);
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
