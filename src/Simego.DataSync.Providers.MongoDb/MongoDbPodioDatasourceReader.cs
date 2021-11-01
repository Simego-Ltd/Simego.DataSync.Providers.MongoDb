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
            if (columnParts.Length < 2) return null; // Podio columns have at least 2 parts fields|name|subname
            if (columnParts[0] != "fields") return null;

            if (item[columnParts[0]] is object [] arr)
            {
                foreach (var field in arr)
                {
                    if (field is Dictionary<string, object> values)
                    {
                        var label = (string)values["label"]; // podio column name
                        var type = (string)values["type"]; // type of podio column
                        var name = columnParts[columnParts.Length - 1]; // podio subname of field to return

                        if (label == columnParts[1])
                        {
                            var value = GetFirstValuesDictionary(values);
                            if (value != null)
                            {
                                // Return value based on Podio DataType
                                switch (type)
                                {
                                    case "app":
                                    {
                                        return GetDictionaryValue(GetDictionary(value, "value"),
                                            name == "id" ? "item_id" : "title");
                                    }
                                    case "money":
                                    {
                                        return GetDictionaryValue(value, name == "currency" ? name : "value");
                                    }
                                    case "category":
                                    {
                                        return GetDictionaryValue(GetDictionary(value, "value"), name == "id" ? name : "text");
                                    }
                                    default:
                                    {
                                        return GetDictionaryValue(value, "value");
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return null;
        }

        private object GetDictionaryValue(Dictionary<string, object> dictionary, string name)
        {
            if (dictionary != null && dictionary.TryGetValue(name, out var val))
            {
                return val;
            }
            return null;
        }
        private Dictionary<string, object> GetDictionary(Dictionary<string, object> dictionary, string name)
        {
            if (dictionary != null && dictionary.TryGetValue(name, out var val))
            {
                return val as Dictionary<string, object>;
            }
            return null;
        }
        private Dictionary<string, object> [] GetArray(Dictionary<string, object> dictionary, string name)
        {
            if (dictionary != null && dictionary.TryGetValue(name, out var val))
            {
                return val as Dictionary<string, object> [];
            }
            return null;
        }

        private Dictionary<string, object> GetFirstValuesDictionary(Dictionary<string, object> f)
        {
            if (f["values"] is object[] values && values.Length > 0)
            {
                return values[0] as Dictionary<string, object>;
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
                        case "category":
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