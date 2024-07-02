using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

using System.Data;
using System.Text.Json;
using MySql.Data.MySqlClient;

using Receive.Models;

string _connectionString = "Server=localhost;Database=largedatasetdb;User=root;Password=root;AllowUserVariables=True;UseAffectedRows=False";

var factory = new ConnectionFactory { HostName = "localhost" };
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
    var batchData = Encoding.UTF8.GetString(body);
    
    Console.WriteLine($" [x] Received batch of size {batchData.Length} from batch_uploads");

    var batch = JsonSerializer.Deserialize<List<Employee>>(batchData);

    await ProcessBatch(batch);

    
} ;     

channel.BasicConsume(queue: "batch_uploads",
                         autoAck: true,
                         consumer: consumer);

    Console.WriteLine(" [*] Waiting for batch message.")                         ;
    Console.ReadLine();

 async Task ProcessBatch(List<Employee> batch)
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

       

        using (MySqlConnection mConnection = new MySqlConnection(_connectionString))
        {
            await mConnection.OpenAsync();
            using var transaction = await mConnection.BeginTransactionAsync();
            using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), mConnection))
            {
                myCmd.Transaction = transaction;
                myCmd.CommandType = CommandType.Text;
                try
                {
                    await myCmd.ExecuteNonQueryAsync();
                    await transaction.CommitAsync();
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    await transaction.RollbackAsync();
                }
            }
        }

        Console.WriteLine("Batch processed successfully");
    }