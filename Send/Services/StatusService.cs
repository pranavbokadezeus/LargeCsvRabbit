using Send.Models;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Polly;

namespace Send.Services;

public class StatusService
{
    private readonly IMongoCollection<StatusModel> _statusCollection;
    private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);


    public StatusService()
    {
        var mongoClient = new MongoClient("mongodb://pranav:root@localhost:27017");

        var mongoDatabase = mongoClient.GetDatabase("CsvProcessStatus");

        _statusCollection = mongoDatabase.GetCollection<StatusModel>("ProcessStatus");
    }

   
    

    public async Task AddBatchAsync(string uid,string fid,Batch batch){

        var retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, timeSpan, retryCount, context) =>
            {
                log.Warn($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
                // Console.WriteLine($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
            });

        await retryPolicy.ExecuteAsync(async () =>
        {
       try
            {
                var filter = Builders<StatusModel>.Filter.Eq(s => s.UId, uid) &
                             Builders<StatusModel>.Filter.Eq(s => s.FId, fid);
                var update = Builders<StatusModel>.Update.Push(s => s.Batches,batch);
 
                var result = await _statusCollection.UpdateOneAsync(filter, update);
                if (result.MatchedCount > 0)
                {
                    log.Info($"Successfully added batch for UId: {uid}, FId: {fid}");
                    // Console.WriteLine($"Successfully added batch for UId: {uid}, FId: {fid}");
                }
                else
                {
                    // Console.WriteLine("-------------------------------------------------------------------------------------------------");
                    throw new Exception($"Failed add batch : No document found with UId: {uid}, FId: {fid}");
                }
                
            }
            catch (Exception e)
            {
                log.Error(e.Message);
                // Console.WriteLine(e.Message);
                throw;
            }

        });
    }

    public async Task UpdateAsync(string Uid, string Fid, StatusModel updatedStatusModel) {

        var retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(10, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (exception, timeSpan, retryCount, context) =>
            {
                log.Warn($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
                // Console.WriteLine($"Retry {retryCount} encountered an error: {exception.Message}. Waiting {timeSpan} before next retry.");
            });

        await retryPolicy.ExecuteAsync(async () =>
        {
            try {
                 var filter = Builders<StatusModel>.Filter.Eq(s => s.UId, Uid) &
                             Builders<StatusModel>.Filter.Eq(s => s.FId, Fid);
                var update = Builders<StatusModel>.Update.Set(s => s.Status, updatedStatusModel.Status) ;
                var update1 = Builders<StatusModel>.Update.Set(s => s.TotalBatches, updatedStatusModel.TotalBatches);
 
                var result = await _statusCollection.UpdateOneAsync(filter, update) ;
                var result1 = await _statusCollection.UpdateOneAsync(filter, update1);
                if (result.MatchedCount > 0)
                {
                    log.Info($"Successfully updated file status as {updatedStatusModel.Status} for UId: {Uid}, FId: {Fid}");
                    // Console.WriteLine($"Successfully updated file status for UId: {Uid}, FId: {Fid}");
                }
                else
                {
                    // Console.WriteLine("-------------------------------------------------------------------------------------------------");
                    throw new Exception($"Failed updated file totalBatches : No document found with UId: {Uid}, FId: {Fid}");
                }

            }
            catch(Exception e)       {
                log.Error(e.Message);
                // Console.WriteLine(e.Message);
                throw;
            }
        });
    }


}