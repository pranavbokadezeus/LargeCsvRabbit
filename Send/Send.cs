using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
  
using System.Data;
using System.Text.Json;
using MySql.Data.MySqlClient;

using Send.Models;


    var factory = new ConnectionFactory { HostName = "localhost" };
    using var connection = factory.CreateConnection();
    using var channel = connection.CreateModel();
    channel.QueueDeclare(queue: "file_uploads",
                         durable: false,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);

    var consumer = new EventingBasicConsumer(channel);
    consumer.Received += async (model, ea) => 
    {
        var body = ea.Body.ToArray();
        var filePath = Encoding.UTF8.GetString(body);
        
        Console.WriteLine($" [x] Received {filePath}");

        await ProcessFile(filePath);
    };
    channel.BasicConsume(queue: "file_uploads",
                         autoAck: true,
                         consumer: consumer);

    Console.WriteLine(" [*] Waiting for messages.");
    Console.ReadLine();

 async Task ProcessFile(string filePath) {
    var models = new List<Employee>();

    using (var stream = new FileStream(filePath, FileMode.Open))
    using (var reader = new StreamReader(stream)) 
    {
        string line;
        bool isHeader = true;

        while((line = reader.ReadLine()) != null)
        {
            if(isHeader) {
                isHeader = false;
                continue;
            }
            try {
                models.Add(line.ToCsvData());
            }
            catch (Exception ex) {
                Console.WriteLine($"Error parsing line: {line}. Exception: {ex.Message}");
            }
        }
    }

    var batchSize = 10000;

    var totalBatches = (int)Math.Ceiling((double)models.Count / batchSize);

    for(int i=0; i<totalBatches; i++) {
        var batch = models.Skip(i * batchSize).Take(batchSize).ToList();
        var batchid = Guid.NewGuid().ToString();

        await SendBatchToQueue(batch);
    }
    if (File.Exists(filePath))
    {
        File.Delete(filePath);
    }
}

async Task SendBatchToQueue(List<Employee> batch) 
{
    var factory = new ConnectionFactory { HostName = "localhost" };
    using var connection = factory.CreateConnection();
    using var channel = connection.CreateModel();
    channel.QueueDeclare(queue: "batch_uploads",
                         durable: false,
                         exclusive: false,
                         autoDelete: false,
                         arguments: null);

    var batchData = JsonSerializer.Serialize(batch);                         
    var message = Encoding.UTF8.GetBytes($"{batchData}");
    channel.BasicPublish(exchange: string.Empty,
                         routingKey: "batch_uploads",
                         basicProperties: null,
                         body: message);

    Console.WriteLine($" [x] Sent batch of size {batch.Count} to batch_uploads")                         ;
}