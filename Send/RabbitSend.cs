using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Polly;
using RabbitMQ.Client.Exceptions;
using System.Data;
using System.Text.Json;
using MySql.Data.MySqlClient;
using MongoDB.Driver;
using Send.Models;
using Send.Services;


public class RabbitSend {
    static async Task Main(string[] args) {

        var factory = new ConnectionFactory { HostName = "localhost" };
        var retryPolicy = Policy
            .Handle<BrokerUnreachableException>()
            .Or<IOException>()
            .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, timeSpan, retryCount, context) =>
            {
                Console.WriteLine($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
            });

        await retryPolicy.ExecuteAsync(async () =>
        {
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
                var msg = Encoding.UTF8.GetString(body);
                var part = msg.Split("|");
                var filePath = part[0];
                var Uid = part[1];
                var Fid = part[2];
                
                Console.WriteLine($" [x] Received {filePath}");

                await ProcessFile(filePath, Uid, Fid);
            };
            channel.BasicConsume(queue: "file_uploads",
                                autoAck: true,
                                consumer: consumer);

            Console.WriteLine(" [*] Waiting for messages.");
            Console.ReadLine();
        });
    }


    static async Task ProcessFile(string filePath, string uid, string fid) {
        var statusService = new StatusService();

        var models = new List<Employee>();

        var processingStatus = new StatusModel{
            Status = "Processing",
        };

        await statusService.UpdateAsync(uid, fid, processingStatus);

        var retryPolicy = Policy
            .Handle<IOException>()
            .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, timeSpan, retryCount, context) =>
            {
                Console.WriteLine($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
            });

        await retryPolicy.ExecuteAsync(async () =>
        {
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
        });

        var batchSize = 10000;
        var totalBatches = (int)Math.Ceiling((double)models.Count / batchSize);

        var batchingStatus = new StatusModel{
            Status = "Batching",
            TotalBatches = totalBatches
        };

        await statusService.UpdateAsync(uid, fid, batchingStatus);
        var newBatchSize = batchSize;

        for(int i=0; i<totalBatches; i++) {
            if(i==totalBatches-1){
                newBatchSize = models.Count - (i*batchSize);
            }
            var batch = models.Skip(i * batchSize).Take(batchSize).ToList();
            var batchid = Guid.NewGuid().ToString();
            await SendBatchToQueue(batch, uid, fid, batchid);
            var batchStatus = new Batch{
               BId = batchid,
               BatchStatus = "queued",
               BatchStart = i*batchSize,
               BatchEnd = (i*batchSize)+newBatchSize
            };
            await statusService.AddBatchAsync(uid, fid, batchStatus);
        }

        var uploadingStatus = new StatusModel{
            Status = "Uploading",
            TotalBatches = totalBatches
        };

        await statusService.UpdateAsync(uid, fid, uploadingStatus);


        if (File.Exists(filePath))
        {
            File.Delete(filePath);
        }
    }



    static async Task SendBatchToQueue(List<Employee> batch, string uid, string fid, string bid)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        var retryPolicy = Policy
            .Handle<BrokerUnreachableException>()
            .Or<IOException>()
            .WaitAndRetryAsync(3, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, timeSpan, retryCount, context) =>
            {
                Console.WriteLine($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
            });

        await retryPolicy.ExecuteAsync(async () =>
        {
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDeclare(queue: "batch_uploads",
                                durable: false,
                                exclusive: false,
                                autoDelete: false,
                                arguments: null);

            var batchData = JsonSerializer.Serialize(batch);
            var message = Encoding.UTF8.GetBytes($"{batchData}|{uid}|{fid}|{bid}");
            channel.BasicPublish(exchange: string.Empty,
                                routingKey: "batch_uploads",
                                basicProperties: null,
                                body: message);

            Console.WriteLine($" [x] Sent batch of size {batch.Count} to batch_uploads");
        });
    }
}
