using Receive.Models;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;


namespace Receive.StatusServices;

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



    public async Task UpdateBatchAsync(string Uid, string Fid, string Bid, string updatedStatus) {
    
        
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
                            Builders<StatusModel>.Filter.Eq(s => s.FId, Fid) &
                            Builders<StatusModel>.Filter.ElemMatch(s => s.Batches, b => b.BId == Bid);

                var update = Builders<StatusModel>.Update.Set("Batches.$.BatchStatus", updatedStatus);
                var result = await _statusCollection.UpdateOneAsync(filter, update);

            

                if (result.MatchedCount > 0)
                {
                    log.Info($"Successfully updated batch status to {updatedStatus} for UId: {Uid}, FId: {Fid}, BId: {Bid}");
                    // Console.WriteLine($"Successfully updated batch status for UId: {Uid}, FId: {Fid}, BId: {Bid}");
                }
                else
                {
                    // Console.WriteLine("-------------------------------------------------------------------------------------------------");
                    throw new Exception($"Failed to update batch status : No document found with UId: {Uid}, FId: {Fid}, and BId: {Bid}");
                }
            } catch (Exception e) {
                log.Error(e.Message);
                // Console.WriteLine(e.Message);
                throw;
            }
         });

            // if (result.ModifiedCount == 0) {
            //     throw new Exception("Batch status update failed");
            // }
       
}


    public async Task UpdateBatchCount(string uid , string fid ) {
       


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
            var filter = Builders<StatusModel>.Filter.Eq(s => s.UId, uid) &
                             Builders<StatusModel>.Filter.Eq(s => s.FId, fid);
                var update = Builders<StatusModel>.Update.Inc(s => s.BatchCount, 1);
 
                var result = await _statusCollection.UpdateOneAsync(filter, update);

                    if (result.MatchedCount > 0)
                    {
                        log.Info($"Successfully updated batch Count for UId: {uid}, FId: {fid}");
                        // Console.WriteLine($"Successfully updated batch Count for UId: {uid}, FId: {fid}");
                    }
                    else
                    {
                        // Console.WriteLine("###################################################################################");
                        throw new Exception($"Failed update batch count : No document found with UId: {uid}, FId: {fid}");
                    }
        }
        catch(Exception e) {
            log.Error(e.Message);
            // Console.WriteLine(e.Message);
            throw;
        }

        });
    }

    
    public async Task Check(string uid, string fid)
    {

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
            var filter = Builders<StatusModel>.Filter.And(
                Builders<StatusModel>.Filter.Eq(s => s.UId, uid),
                Builders<StatusModel>.Filter.Eq(s => s.FId, fid)
            );
    
            var document = await _statusCollection.Find(filter).FirstOrDefaultAsync();
    
            if (document == null)
            {
                log.Error($"No document found with UId: {uid} and FId: {fid}");
                // Console.WriteLine($"No document found with UId: {uid} and FId: {fid}");
                
                return;
            }

            if(document.Batches.Count != document.TotalBatches) {
                return;
            }
    
            bool allCompleted = true;
            List<string> errorBatchIds = new List<string>();
    
            foreach (var batch in document.Batches)
            {
                if (batch.BatchStatus != "Completed")
                {
                    allCompleted = false;
                    if (batch.BatchStatus == "Error")
                    {
                        errorBatchIds.Add(batch.BId);
                    }
                }
            }
    
            var updateDefinition = new List<UpdateDefinition<StatusModel>>();
    
            if (allCompleted)
            {
                updateDefinition.Add(Builders<StatusModel>.Update.Set(s => s.Status, "Completed"));
                // await _statusCollection.UpdateOneAsync(document.Status,"Completed");
            }
            else if (errorBatchIds.Count > 0)
            {
                var errorMessage = $"Error occurred in batches: {string.Join(", ", errorBatchIds)}";
                updateDefinition.Add(Builders<StatusModel>.Update.Set(s => s.Status, errorMessage));
            }
    
            if (updateDefinition.Count > 0)
            {
                var combinedUpdate = Builders<StatusModel>.Update.Combine(updateDefinition);
                var updateResult = await _statusCollection.UpdateOneAsync(filter, combinedUpdate);
    
                if (updateResult.MatchedCount > 0)
                {
                    log.Info($"Successfully updated status for UId: {uid} and FId: {fid}");
                    // Console.WriteLine($"Successfully updated status for UId: {uid} and FId: {fid}");
                }
                else
                {
                    // Console.WriteLine("=========================================================================");
                    throw new Exception($"Failed to update _Completed_ status for UId: {uid} and FId: {fid}");

                }
            }
        }
        catch (Exception e)
        {
            // Console.WriteLine("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            log.Error($"Error occurred while updating status: {e.Message}");
            // Console.WriteLine($"Error occurred while updating status: {e.Message}");
            throw;
        }

        });
    }
    


}