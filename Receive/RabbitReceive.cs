using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;
using RabbitMQ.Client.Exceptions;
using System.Data;
using System.Text.Json;
using MySql.Data.MySqlClient;
using Receive.StatusServices;
using Receive.Models;

public class RabbitReceive {
    private static readonly string _connectionString = "Server=localhost;Database=largedatasetdb;User=root;Password=root;AllowUserVariables=True;UseAffectedRows=False";

    private static StatusService statusService;

    static RabbitReceive() {
        statusService = new StatusService();
    }

    // private static readonly string _connectionString = "";

    static async Task Main(string[] args) {

        var factory = new ConnectionFactory { HostName = "localhost" };


        var delay = Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: TimeSpan.FromSeconds(1), retryCount: 10);
            AsyncRetryPolicy retryPolicy = Policy
            .Handle<BrokerUnreachableException>()
            .Or<IOException>()
            .WaitAndRetryAsync(delay,  (exception, timeSpan, retryCount, context) =>
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

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += async (model, ea) => 
            {
                var body = ea.Body.ToArray();
                var msg = Encoding.UTF8.GetString(body);

                var part = msg.Split("|");
                var batchData = part[0];
                var Uid = part[1];
                var Fid = part[2];
                var Bid = part[3];

                Console.WriteLine($" [x] Received batch of size {batchData.Length} from batch_uploads");

                var batch = JsonSerializer.Deserialize<List<Employee>>(batchData);
                await ProcessBatch(batch, Uid, Fid, Bid);
            };

            channel.BasicConsume(queue: "batch_uploads",
                                autoAck: true,
                                consumer: consumer);

            Console.WriteLine(" [*] Waiting for batch message.");
            Console.ReadLine();

        });
                            

        
    }
    static async Task ProcessBatch(List<Employee> batch, string Uid, string Fid, string Bid)
    {
        StringBuilder sCommand = new StringBuilder("REPLACE INTO employees (Id,Email,Name,Country,State,City,Telephone,AddressLine1,AddressLine2,DOB,FY2019_20,FY2020_21,FY2021_22,FY2022_23,FY2023_24) VALUES ");
        List<string> Rows = new List<string>();
        for (int i = 0; i < batch.Count; i++)
        {
            Rows.Add(string.Format("('{0}','{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}','{10}','{11}','{12}','{13}','{14}')", 
                    batch[i].ID,
                    MySqlHelper.EscapeString(batch[i].Email), 
                    MySqlHelper.EscapeString(batch[i].Name), 
                    MySqlHelper.EscapeString(batch[i].Country), 
                    MySqlHelper.EscapeString(batch[i].State), 
                    MySqlHelper.EscapeString(batch[i].City), 
                    MySqlHelper.EscapeString(batch[i].Telephone), 
                    MySqlHelper.EscapeString(batch[i].AddressLine1), 
                    MySqlHelper.EscapeString(batch[i].AddressLine2), 
                    batch[i].DOB.ToString("yyyy-MM-dd"),
                    batch[i].FY2019_20,
                    batch[i].FY2020_21,
                    batch[i].FY2021_22,
                    batch[i].FY2022_23,
                    batch[i].FY2023_24
                ));
        }
        sCommand.Append(string.Join(",", Rows));
        sCommand.Append(";");

        var retryPolicy = Policy
            .Handle<MySqlException>()
            .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, timeSpan, retryCount, context) =>
            {
                Console.WriteLine($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
            });

        await retryPolicy.ExecuteAsync(async () =>
        {
                
            
            using (MySqlConnection mConnection = new MySqlConnection(_connectionString))
            {
                await mConnection.OpenAsync();

                using var transaction = await mConnection.BeginTransactionAsync();
                using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection, transaction))
                {
                    myCmd.CommandType = CommandType.Text;
                    try
                    {
                        await myCmd.ExecuteNonQueryAsync();
                        await transaction.CommitAsync();
                        Console.WriteLine(Bid);
                        await statusService.UpdateBatchAsync(Uid, Fid, Bid, "Completed");

                        await statusService.UpdateBatchCount(Uid, Fid);
                        Console.WriteLine("Batch processed successfully");
                         

                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                        await transaction.RollbackAsync();
                        await statusService.UpdateBatchAsync(Uid, Fid, Bid, "Error");
                    }
                }
                await mConnection.CloseAsync();

            }
            await statusService.Check(Uid, Fid);

        });

        

        
    }


}