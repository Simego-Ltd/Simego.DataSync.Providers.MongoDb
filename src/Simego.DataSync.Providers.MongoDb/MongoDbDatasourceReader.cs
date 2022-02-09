using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json.Linq;
using Simego.DataSync.Interfaces;
using Simego.DataSync.Providers.MongoDb.Extensions;
using Simego.DataSync.Providers.MongoDb.TypeConverters;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Linq;
using System.Windows.Forms;

namespace Simego.DataSync.Providers.MongoDb
{
    [ProviderInfo(Name = "MongoDb", Description = "Read and Write data stored in a MongoDb Database")]
    public class MongoDbDatasourceReader : DataReaderProviderBase, IDataSourceSetup
    {
        private ConnectionInterface _connectionIf;

        [Category("Settings")]
        //[PasswordPropertyText(true)]
        public string ConnectionString { get; set; }

        [Category("Settings")]
        [Description("MongoDb Database")]
        [TypeConverter(typeof(DatabaseTypeConverter))]
        public string Database { get; set; }
        
        [Category("Settings")]
        [Description("MongoDb Collection")]
        [TypeConverter(typeof(CollectionTypeConverter))]
        public string Collection { get; set; }

        [Category("Schema.Settings")]
        [Description("The number of Documents to look at to discover the Schema.")]
        public int SchemaDiscoveryMaxRows { get; set; } = 10;
        
        [Category("Schema.Settings")]
        [Description("Use a Data Type discovered from the Schema.")]
        public bool UseSchemaDataTypes { get; set; }

        [Category("Filter")]
        [Description("MongoDb Document Filter Expression")]
        public string DocumentFilter { get; set; }

        [Category("Settings")]
        [Description("MongoDb Database")]
        public int UpdateBatchSize { get; set; } = 1;

        public override DataTableStore GetDataTable(DataTableStore dt)
        {
            // Store the MongoDb _id against rows
            dt.AddIdentifierColumn(typeof(string));
            
            var mapping = new DataSchemaMapping(SchemaMap, Side);
            var columns = SchemaMap.GetIncludedColumns();

            var filter = string.IsNullOrEmpty(DocumentFilter) ? FilterDefinition<BsonDocument>.Empty : DocumentFilter;
           
            var client = GetClient();            
            var database = client.GetDatabase(Database);
            
            var collection = database.GetCollection<BsonDocument>(Collection);            
            var cursor = collection.Find(filter).ToCursor();
                       
            foreach (var itemRow in cursor.ToEnumerable())
            {
                var d = itemRow.ToDictionary();
                try
                {
                    if (dt.Rows.AddWithIdentifier(mapping, columns,
                        (item, columnName) =>
                        {
                            var parts = columnName.Split('|');
                            var element = GetDocumentElement(d, columnName);
                            if (element != null && element.TryGetValue(parts[parts.Length - 1], out var value) && value != null)
                            {
                                if (item.DataType == typeof(JToken))
                                {
                                    var token = JToken.FromObject(value);
                                    return token.HasValues ? token : null;
                                }
                                if (item.DataType == typeof(DateTime))
                                {
                                    var f = BsonValue.Create(value);
                                    if (f.IsBsonDateTime || f.IsBsonTimestamp)
                                    {
                                        return f.ToNullableUniversalTime();
                                    }
                                    if (f.IsString)
                                    {                                        
                                        //"Fri May 25 21:22:15 UTC 2007"
                                        if (DateTime.TryParseExact(f.AsString, "ddd MMM dd HH:mm:ss UTC yyyy", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var dateTimeValue))
                                        {
                                            return dateTimeValue;
                                        }

                                        // return default DateTime conversion from string value ....
                                        return f.AsString;
                                    }
                                }
                                return value;
                            }
                            return GetRowValue(columnName, parts, d);
                        }
                        , DataSchemaTypeConverter.ConvertTo<string>(d["_id"])) == DataTableStore.ABORT)
                    {
                        break;
                    }
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"{e.Message} _id: {d["_id"]}", e);
                }
            }
            
            return dt;
        }

        protected virtual object GetRowValue(string columnName, string[] columnParts, Dictionary<string, object> item)
        {
            return null;
        }
        private Dictionary<string, object> GetDocumentElement(Dictionary<string, object> source, string columnName)
        {
            var parts = columnName.Split('|');
            if(source != null && parts.Length > 1)
            {
                if(source.TryGetValue(parts[0], out var value))
                {
                    if (value is Dictionary<string, object> dictionaryValue)
                    {
                        return GetDocumentElement(dictionaryValue,string.Join("|", parts, 1, parts.Length - 1));
                    }
                }
                return null;
            }
            return source;
        }
        
        public override DataSchema GetDefaultDataSchema()
        {
            //Return the Data source default Schema.
            DataSchema schema = new DataSchema();
            
            var client = GetClient();
            var database = client.GetDatabase(Database);            
            var collection = database.GetCollection<BsonDocument>(Collection);
            
            // Query Last 10 rows to build the schema ....
            var filter = string.IsNullOrEmpty(DocumentFilter) ? FilterDefinition<BsonDocument>.Empty : DocumentFilter;
            var cursor = collection.Find(filter).Sort("{ _id: -1 }").Limit(SchemaDiscoveryMaxRows).ToCursor();

            foreach (var itemRow in cursor.ToEnumerable())
            {
                foreach (var column in itemRow)
                {
                    // Ignore null values
                    if (column.Value.IsBsonNull) continue;

                    if (column.Value.IsBsonDocument)
                    {
                        GetDefaultDataSchema(schema, column.Name, column.Value.AsBsonDocument);
                    }
                    else if (column.Value.IsBsonArray)
                    {
                        foreach (var item in column.Value.AsBsonArray)
                        {
                            if (item.IsBsonDocument)
                            {
                                schema.Map.AddIfNotExists(new DataSchemaItem(column.Name, typeof(JToken), false, false,
                                    true, -1));
                            }
                            else
                            {
                                schema.Map.AddIfNotExists(new DataSchemaItem(column.Name, typeof(string[]), false,
                                    false, true, -1));
                            }

                            break;
                        }
                    }
                    else if (column.Value.IsObjectId)
                    {
                        schema.Map.AddIfNotExists(new DataSchemaItem(column.Name, typeof(string), true, false, false,
                            -1));
                    }
                    else
                    {
                        schema.Map.AddIfNotExists(new DataSchemaItem(column.Name,
                            UseSchemaDataTypes ? column.GetDataSyncType() : typeof(string), false, false, true, -1));
                    }

                    ProcessSchemaItem(schema, column.Name, column.Value);
                }
            }

            schema.Map = schema.Map.OrderBy(p => p.ColumnName).ToList();
            return schema;
        }

        protected virtual void ProcessSchemaItem(DataSchema schema, string name, BsonValue value)
        {
            
        } 
        private void GetDefaultDataSchema(DataSchema schema, string name, BsonDocument document)
        {
            // Add a Top Level node to store the Json for this document.
            schema.Map.AddIfNotExists(new DataSchemaItem($"{name}", typeof(JToken), false, false, true, -1));

            foreach (var column in document)
            {
                if (column.Value.IsBsonDocument)
                {
                    GetDefaultDataSchema(schema, $"{name}|{column.Name}", column.Value.AsBsonDocument);
                }
                else if (column.Value.IsBsonArray)
                {
                    foreach(var item in column.Value.AsBsonArray)
                    {
                        if (item.IsBsonDocument)
                        {
                            schema.Map.AddIfNotExists(new DataSchemaItem($"{name}|{column.Name}", typeof(JToken), false, false, true, -1));
                        } 
                        else
                        {
                            schema.Map.AddIfNotExists(new DataSchemaItem($"{name}|{column.Name}", typeof(string[]), false, false, true, -1));
                        }
                        break;
                    }
                }
                else
                {
                    schema.Map.AddIfNotExists(new DataSchemaItem($"{name}|{column.Name}", UseSchemaDataTypes ? column.GetDataSyncType() : typeof(string), false, false, true, -1));
                }
            }                       
        }
        
        public override List<ProviderParameter> GetInitializationParameters()
        {
            //Return the Provider Settings so we can save the Project File.
            return new List<ProviderParameter>
                       {
                            new ProviderParameter(nameof(ConnectionString), ConnectionString),
                            new ProviderParameter(nameof(Database), Database),
                            new ProviderParameter(nameof(Collection), Collection),
                            new ProviderParameter(nameof(DocumentFilter), DocumentFilter),
                            new ProviderParameter(nameof(UseSchemaDataTypes), UseSchemaDataTypes.ToString()),
                            new ProviderParameter(nameof(SchemaDiscoveryMaxRows), SchemaDiscoveryMaxRows.ToString()),
                            new ProviderParameter(nameof(UpdateBatchSize), UpdateBatchSize.ToString()),
                       };
        }

        public override void Initialize(List<ProviderParameter> parameters)
        {
            //Load the Provider Settings from the File.
            foreach (ProviderParameter p in parameters)
            {
                AddConfigKey(p.Name, p.ConfigKey);

                switch (p.Name)
                {
                    case nameof(ConnectionString):
                        {
                            ConnectionString = p.Value;
                            break;
                        }
                    case nameof(Database):
                        {
                            Database = p.Value;
                            break;
                        }
                    case nameof(Collection):
                        {
                            Collection = p.Value;
                            break;
                        }
                    case nameof(DocumentFilter):
                        {
                            DocumentFilter = p.Value;
                            break;
                        }
                    case nameof(UseSchemaDataTypes):
                        {
                            if(bool.TryParse(p.Value, out var val))
                            {
                                UseSchemaDataTypes = val;
                            }                            
                            break;
                        }
                    case nameof(SchemaDiscoveryMaxRows):
                        {
                            if (int.TryParse(p.Value, out var val))
                            {
                                SchemaDiscoveryMaxRows = val;
                            }
                            break;
                        }
                    case nameof(UpdateBatchSize):
                        {
                            if (int.TryParse(p.Value, out var val))
                            {
                                UpdateBatchSize = val;
                            }
                            break;
                        }
                }
            }
        }
        
        #region IDataSourceSetup - Render Custom Configuration UI

        public void DisplayConfigurationUI(Control parent)
        {
            if (_connectionIf == null)
            {
                _connectionIf = new ConnectionInterface();
                _connectionIf.PropertyGrid.SelectedObject = new ConnectionProperties(this);
            }

            _connectionIf.Font = parent.Font;
            _connectionIf.Size = new Size(parent.Width, parent.Height);
            _connectionIf.Location = new Point(0, 0);
            _connectionIf.Dock = DockStyle.Fill;

            parent.Controls.Add(_connectionIf);
        }

        public bool Validate()
        {
            try
            {
                if (string.IsNullOrEmpty(ConnectionString))
                {
                    throw new ArgumentException($"You must specify a valid {nameof(ConnectionString)}.");
                }

                return true;
            }
            catch (Exception e)
            {
                MessageBox.Show(e.Message, nameof(MongoDbDatasourceReader), MessageBoxButtons.OK, MessageBoxIcon.Information);
            }

            return false;
        }

        public IDataSourceReader GetReader()
        {
            return this;
        }

        #endregion
        public override IDataSourceWriter GetWriter() => new MongoDbDataSourceWriter { SchemaMap = SchemaMap };

        public MongoClient GetClient() => new MongoClient(MongoClientSettings.FromConnectionString(ConnectionString));
        public List<string> GetDatabases() => GetClient().ListDatabaseNames().ToList();
        public List<string> GetCollections() => GetClient().GetDatabase(Database).ListCollectionNames().ToList();

    }
}
