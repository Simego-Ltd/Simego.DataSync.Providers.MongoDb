using System;
using System.Collections.Generic;
using MongoDB.Bson;
using Simego.DataSync.Providers.MongoDb.Extensions;

namespace Simego.DataSync.Providers.MongoDb
{
    [ProviderInfo(Name = "MongoDb - Podio Data", Description = "Reads Podio App data stored in MongoDb")]
    public class MongoDbPodioDatasourceReader : MongoDbDatasourceReader
    {
        protected override object GetRowValue(string columnName, string[] columnParts, Dictionary<string, object> item)
        {
            if (columnParts[0] == "fields")
            {
                if (item[columnParts[0]] is object [] arr)
                {
                    foreach (var field in arr)
                    {
                        if (field is Dictionary<string, object> f)
                        {
                            var label = (string)f["label"];
                            var type = (string)f["type"];
                            
                            if (label == columnParts[1])
                            {
                                var value = (Dictionary<string, object>)((object[])f["values"])[0];
                                
                                switch (type)
                                {
                                    case "app":
                                    {
                                        if (columnParts[columnParts.Length - 1] == "id")
                                        {
                                            return ((Dictionary<string, object>)value["value"])["item_id"];    
                                        }
                                        return ((Dictionary<string, object>)value["value"])["title"];
                                    }
                                    case "money":
                                    {
                                        if (columnParts[columnParts.Length - 1] == "currency")
                                        {
                                            return value["currency"];    
                                        }
                                        return value["value"];
                                    }
                                    default:
                                    {
                                        return value["value"];
                                    }
                                }

                                break;
                            }

                        }
                    }
                }
            }
            
            return null;
        }

        protected override void ProcessSchemaItem(DataSchema schema, string name, BsonValue value)
        {
            if (name == "fields" && value is BsonArray fields)
            {
                foreach (var field in fields)
                {
                    var label = field["label"].AsString;
                    var type = field["type"].AsString;
                
                    switch (type)
                    {
                        case "app":
                        {
                            schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|id", typeof(int), false, false,true, -1));
                            schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(string), false, false,true, -1));
                            break;
                        }
                        case "money":
                        {
                            schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|currency", typeof(string), false, false,true, -1));
                            schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(decimal), false, false,true, -1));
                            break;
                        }
                        default:
                        {
                            schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(string), false, false, true, -1));
                            break;
                        }
                    }
                }    
            }
        }
    }
}