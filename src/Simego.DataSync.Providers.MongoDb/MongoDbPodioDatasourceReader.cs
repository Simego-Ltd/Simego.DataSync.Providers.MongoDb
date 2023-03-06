using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using MongoDB.Bson;
using Newtonsoft.Json.Linq;
using Simego.DataSync.Core;
using Simego.DataSync.Interfaces;
using Simego.DataSync.Providers.MongoDb.Extensions;
using Simego.DataSync.Providers.MongoDb.TypeConverters;

namespace Simego.DataSync.Providers.MongoDb
{
    [ProviderInfo(Name = "MongoDb - Podio Data", Description = "Reads Podio App Items data stored in MongoDb", Group = "MongoDb")]
    public class MongoDbPodioDatasourceReader : MongoDbDatasourceReader, IDataSourceRegistry, IDataSourceRegistryView
    {
        [Category("Settings.Podio")]
        [Description("Json Element containing Podio Json Data")]
        public string PodioDataElement { get; set; } = "json";

        protected override object GetRowValue(string columnName, string[] columnParts, Dictionary<string, object> item)
        {
            if (columnParts.Length < 2) return null; // Podio columns have at least 2 parts fields|name|subname
            if (columnParts[0] != "fields") return null;

            // Reference the Podio Data Element in the Document.
            item = string.IsNullOrEmpty(PodioDataElement) ? item : GetElement(item, PodioDataElement);

            if (item[columnParts[0]] is object[] arr)
            {
                foreach (var field in arr)
                {
                    if (field is Dictionary<string, object> values)
                    {
                        var label = (string)values["label"]; // podio column name
                        var type = (string)values["type"]; // type of podio column
                        var name = columnParts.Length > 2 ? columnParts[columnParts.Length - 1] : null; // podio subname of field to return

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
                                            if (string.IsNullOrEmpty(name))
                                            {
                                                var token = JToken.FromObject(GetValuesDictionaryArray(values, "value"));
                                                return token.HasValues ? token : null;
                                            }

                                            var list = new List<object>();
                                            foreach (var v in GetValuesDictionaryArray(values, "value"))
                                            {
                                                list.Add(v[name == "id" ? "item_id" : "title"]);
                                            }
                                            list.Sort();
                                            return list.ToArray();
                                        }
                                    case "embed":
                                        {
                                            if (string.IsNullOrEmpty(name))
                                            {
                                                var token = JToken.FromObject(GetValuesDictionaryArray(values));
                                                return token.HasValues ? token : null;
                                            }
                                            var list = new List<object>();
                                            foreach (var v in GetValuesDictionaryArray(values, "embed"))
                                            {
                                                list.Add(v[name]);
                                            }
                                            list.Sort();
                                            return list.ToArray();
                                        }
                                    case "money":
                                        {
                                            return GetDictionaryValue(value, name == "currency" ? name : "value");
                                        }
                                    case "question":
                                    case "category":
                                        {
                                            if (string.IsNullOrEmpty(name))
                                            {
                                                var token = JToken.FromObject(GetValuesDictionaryArray(values, "value"));
                                                return token.HasValues ? token : null;
                                            }

                                            var list = new List<object>();
                                            foreach (var v in GetValuesDictionaryArray(values, "value"))
                                            {
                                                list.Add(v[name == "id" ? name : "text"]);
                                            }
                                            list.Sort();
                                            return list.ToArray();
                                        }
                                    case "phone":
                                    case "email":
                                        {
                                            var list = new List<object>();
                                            foreach (var v in GetValuesDictionaryArray(values))
                                            {
                                                list.Add(v["value"]);
                                            }
                                            list.Sort();
                                            return list.ToArray();
                                        }
                                    case "image":
                                        {
                                            if (string.IsNullOrEmpty(name))
                                            {
                                                var token = JToken.FromObject(GetValuesDictionaryArray(values, "value"));
                                                return token.HasValues ? token : null;
                                            }
                                            return GetDictionaryValue(GetDictionary(value, "value"), name);
                                        }
                                    case "contact":
                                        {
                                            if (string.IsNullOrEmpty(name))
                                            {
                                                var token = JToken.FromObject(GetValuesDictionaryArray(values, "value"));
                                                return token.HasValues ? token : null;
                                            }
                                            return GetDictionaryValue(GetDictionary(value, "value"), name);
                                        }
                                    case "date":
                                        {
                                            if (string.IsNullOrEmpty(name))
                                            {
                                                var token = JToken.FromObject(value);
                                                return token.HasValues ? token : null;
                                            }

                                            switch (name)
                                            {
                                                case "start":
                                                    {
                                                        if (value.TryGetValue("start_date_utc", out var val) && val is string s && !string.IsNullOrEmpty(s))
                                                        {
                                                            return Date.Parse(s);
                                                        }
                                                        return null;
                                                    }
                                                case "startdatetimeutc":
                                                    {
                                                        if (value.TryGetValue("start_utc", out var val) && val is string s && !string.IsNullOrEmpty(s))
                                                        {
                                                            return DateTime.SpecifyKind(DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal), DateTimeKind.Utc);
                                                        }
                                                        return null;
                                                    }
                                                case "end":
                                                    {
                                                        if (value.TryGetValue("end_date_utc", out var val) && val is string s && !string.IsNullOrEmpty(s))
                                                        {
                                                            return Date.Parse(s);
                                                        }
                                                        return null;
                                                    }
                                                case "enddatetimeutc":
                                                    {
                                                        if (value.TryGetValue("end_utc", out var val) && val is string s && !string.IsNullOrEmpty(s))
                                                        {
                                                            return DateTime.SpecifyKind(DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal), DateTimeKind.Utc);
                                                        }
                                                        return null;
                                                    }
                                            }

                                            return null;
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

        private Dictionary<string, object> GetElement(Dictionary<string, object> item, string name)
        {
            if(item.TryGetValue(name, out var value))
            {
                return value as Dictionary<string, object>;
            }
            throw new KeyNotFoundException($"Key '{name}' not found in Dictionary");
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
        
        private Dictionary<string, object> GetFirstValuesDictionary(Dictionary<string, object> f)
        {
            if (f.ContainsKey("values") && f["values"] is object[] values)
            {                
                return values.Length > 0 ? values[0] as Dictionary<string, object> : null;
            }
            return f;
        }

        private IEnumerable<Dictionary<string, object>> GetValuesDictionaryArray(Dictionary<string, object> f)
        {
            if (f.ContainsKey("values") && f["values"] is object[] values && values.Length > 0)
            {
                foreach(var value in values)
                {
                    yield return value as Dictionary<string, object>;
                }                
            }            
        }

        private IEnumerable<Dictionary<string, object>> GetValuesDictionaryArray(Dictionary<string, object> f, string name)
        {           
            foreach(var value in GetValuesDictionaryArray(f))
            {
                yield return value[name] as Dictionary<string, object>;
            }
        }

        protected override void ProcessSchemaItem(DataSchema schema, string name, BsonValue value)
        {
            if(name == PodioDataElement)
            {
                var fieldsElement = value["fields"] as BsonValue;
                if(fieldsElement != null)
                {
                    ProcessSchemaItem(schema, "fields", fieldsElement);
                    return;
                }                
            }

            if (name == "fields" && value is BsonArray fields)
            {
                foreach (var field in fields)
                {
                    var label = field["label"].AsString;
                    var type = field["type"].AsString;

                    switch (type)
                    {
                        case "question":
                        case "category":
                        case "app":
                            {
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(JToken), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|id", typeof(long[]), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|text", typeof(string[]), false, false, true, -1));

                                break;
                            }
                        case "money":
                            {
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|currency", typeof(string), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(decimal), false, false, true, -1));
                                break;
                            }
                        case "image":
                            {
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(JToken), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|file_id", typeof(string), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|external_file_id", typeof(string), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|link", typeof(string), false, false, true, -1));
                                break;
                            }
                        case "date":
                            {
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(JToken), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|start", typeof(Date), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|end", typeof(Date), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|startdatetimeutc", typeof(DateTime), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|enddatetimeutc", typeof(DateTime), false, false, true, -1));

                                break;
                            }
                        case "contact":
                            {
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(JToken), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|profile_id", typeof(long), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|user_id", typeof(long), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|name", typeof(string), false, false, true, -1));

                                break;
                            }
                        case "embed":
                            {
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}", typeof(JToken), false, false, true, -1));
                                schema.Map.AddIfNotExists(new DataSchemaItem($"fields|{label}|url", typeof(string[]), false, false, true, -1));
                                
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

        public override void Initialize(List<ProviderParameter> parameters)
        {
            base.Initialize(parameters);
            foreach(var p in parameters)
            {
                switch (p.Name)
                {
                    case nameof(PodioDataElement):
                        {
                            PodioDataElement = p.Value;
                            break;
                        }
                }
            }
        }

        public override List<ProviderParameter> GetInitializationParameters()
        {
            var parameters = base.GetInitializationParameters();
            parameters.Add(new ProviderParameter(nameof(PodioDataElement), PodioDataElement));
            return parameters;
        }
        
        public override void InitializeFromRegistry(IDataSourceRegistryProvider provider)
        {
            var registry = provider.Get(RegistryKey);

            if (registry != null)
            {
                foreach (ProviderParameter p in registry.Parameters)
                {
                    switch (p.Name)
                    {
                        case nameof(ConnectionString):
                            {
                                ConnectionString = p.Value;
                                break;
                            }
                        default:
                            {
                                break;
                            }
                    }
                }
            }
        }

        public override List<ProviderParameter> GetRegistryInitializationParameters()
        {
            return new List<ProviderParameter> { new ProviderParameter(nameof(ConnectionString), ConnectionString) };
        }

        public override IDataSourceReader ConnectFromRegistry(IDataSourceRegistryProvider provider)
        {
            InitializeFromRegistry(provider);
            return this;
        }

        public override object GetRegistryInterface() => new MongoDbPodioDatasourceReaderWithRegistry(this);

        public RegistryConnectionInfo GetRegistryConnectionInfo()
        {
            return new RegistryConnectionInfo { GroupName = "MongoDb Podio Schema Connnections", ConnectionGroupImage = RegistryImageEnum.Podio, ConnectionImage = RegistryImageEnum.Podio };
        }

        public RegistryViewContainer GetRegistryViewContainer(string parent, string id, object state)
        {
            if (parent == null)
            {
                var rootFolder = new RegistryFolderType
                {
                    Image = RegistryImageEnum.Database,
                    Preview = false,
                };

                var databases = new RegistryFolder
                {
                    FolderName = "Databases",
                    Image = RegistryImageEnum.Folder,
                    Completed = false,
                    FolderObjectType = new RegistryFolderType { Preview = false, ParameterName = nameof(Database), Image = RegistryImageEnum.Database }
                };

                foreach (var database in GetDatabases().OrderBy(k => k))
                {
                    databases.AddFolderItem(database, database);
                }

                return new RegistryViewContainer(rootFolder, new[] { databases });
            }

            if (parent == "Databases")
            {
                var collections = new RegistryFolder
                {
                    FolderName = "Collections",
                    Image = RegistryImageEnum.Folder,
                    Completed = true,
                    FolderObjectType = new RegistryFolderType { Preview = true, DataType = InstanceHelper.GetTypeNameString(typeof(MongoDbDatasourceReader)), ParameterName = nameof(Collection), Image = RegistryImageEnum.Table }
                };

                collections.FolderObjectType.AddConnectionParameter(nameof(Database), id);


                foreach (var collection in GetCollections(id))
                {
                    collections.AddFolderItem(collection, collection);
                }


                return new RegistryViewContainer(null, new[] { collections });
            }

            return null;
        }

        public object GetRegistryViewConfigurationInterface()
        {
            return null;
        }
    }

    public class MongoDbPodioDatasourceReaderWithRegistry : DataReaderRegistryView<MongoDbPodioDatasourceReader>
    {
        [Category("Settings")]
        [ReadOnly(true)]
        public string ConnectionString { get { return _reader.ConnectionString; } set { _reader.ConnectionString = value; } }

        [Category("Settings")]
        [Description("MongoDb Database")]
        [TypeConverter(typeof(DatabaseTypeConverter))]
        public string Database { get { return _reader.Database; } set { _reader.Database = value; } }

        [Category("Settings")]
        [Description("MongoDb Collection")]
        [TypeConverter(typeof(CollectionTypeConverter))]
        public string Collection { get { return _reader.Collection; } set { _reader.Collection = value; } }

        [Category("Schema.Settings")]
        [Description("The number of Documents to look at to discover the Schema.")]
        public int SchemaDiscoveryMaxRows { get { return _reader.SchemaDiscoveryMaxRows; } set { _reader.SchemaDiscoveryMaxRows = value; } }

        [Category("Schema.Settings")]
        [Description("Use a Data Type discovered from the Schema.")]
        public bool UseSchemaDataTypes { get { return _reader.UseSchemaDataTypes; } set { _reader.UseSchemaDataTypes = value; } }

        [Category("Filter")]
        [Description("MongoDb Document Filter Expression")]
        public string DocumentFilter { get { return _reader.DocumentFilter; } set { _reader.DocumentFilter = value; } }

        [Category("Settings")]
        [Description("MongoDb Database")]
        public int UpdateBatchSize { get { return _reader.UpdateBatchSize; } set { _reader.UpdateBatchSize = value; } }

        [Category("Settings.Podio")]
        [Description("Json Element containing Podio Json Data")]
        public string PodioDataElement { get { return _reader.PodioDataElement; } set { _reader.PodioDataElement = value; } }

        public MongoDbPodioDatasourceReaderWithRegistry(MongoDbPodioDatasourceReader reader) : base(reader)
        {
        }

        public List<string> GetDatabases() => _reader.GetDatabases();
        public List<string> GetCollections() => _reader.GetCollections();        
    }
}