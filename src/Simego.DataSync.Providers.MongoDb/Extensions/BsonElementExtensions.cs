using MongoDB.Bson;
using System;

namespace Simego.DataSync.Providers.MongoDb.Extensions
{
    static class BsonElementExtensions
    {
        public static Type GetDataSyncType(this BsonElement element)
        {
            if (element.Value.IsBoolean) return typeof(bool);
            if (element.Value.IsInt32) return typeof(int);
            if (element.Value.IsInt64) return typeof(long);
            if (element.Value.IsDouble) return typeof(double);
            if (element.Value.IsNumeric) return typeof(decimal);
            if (element.Value.IsGuid) return typeof(Guid);
            if (element.Value.IsBsonDateTime) return typeof(DateTime);
            
            return typeof(string);
        }
    }
}
