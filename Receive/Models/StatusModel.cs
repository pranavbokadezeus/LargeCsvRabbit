using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Receive.Models;

public class StatusModel
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; set; }

    [BsonElement("UId")]
    public string UId { get; set; }
    [BsonElement("FId")]
    public string FId { get; set; } 
    [BsonElement("Status")]
    public string Status { get; set; } = string.Empty;
    [BsonElement("TotalBatches")]
    public int TotalBatches { get; set;}
    [BsonElement("BatchCount")]

    public int BatchCount { get; set; }
    [BsonElement("Batches")]
    public List<Batch> Batches { get; set; } = [];

    
}

public class Batch {
     [BsonElement("BId")]

        public string BId { get; set; }
    [BsonElement("BatchStatus")]

        public string BatchStatus { get; set; } = string.Empty;
    [BsonElement("BatchStart")]

        public int BatchStart { get; set; }
    [BsonElement("BatchEnd")]

        public int BatchEnd { get; set; }
    }