using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Simego.DataSync.Engine;
using Simego.DataSync.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Simego.DataSync.Providers.MongoDb
{
    public class MongoDbDataSourceWriter : DataWriterProviderBase
    {
        private MongoDbDatasourceReader DataSourceReader { get; set; }
        private DataSchemaMapping Mapping { get; set; }

        private IMongoClient Client;
        private IMongoDatabase Database;
        private IMongoCollection<BsonDocument> Collection;

        public override void AddItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if(DataSourceReader.UpdateBatchSize > 1)
            {
                AddItemsBatch(items, status);
                return;
            }

            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                
                foreach (var item in items)
                {
                    if (!status.ContinueProcessing)
                        break;

                    try
                    {
                        var itemInvariant = new DataCompareItemInvariant(item);

                        // Call the Automation BeforeAddItem
                        Automation?.BeforeAddItem(this, itemInvariant, null);

                        if (itemInvariant.Sync)
                        {                           
                            // Get the Target Item Data
                            Dictionary<string, object> targetItem = AddItemToDictionary(Mapping, itemInvariant);

                            // Create a BsonDocument from the Json
                            var document = BsonDocument.Parse(JsonConvert.SerializeObject(targetItem, Formatting.None));

                            // Assign the _id as an ObjectId
                            if (targetItem.TryGetValue("_id", out var id))
                            {
                                document["_id"] = ObjectId.Parse(DataSchemaTypeConverter.ConvertTo<string>(id));
                            }

                            Collection.InsertOne(document);

                            //Call the Automation AfterAddItem
                            Automation?.AfterAddItem(this, itemInvariant, DataSchemaTypeConverter.ConvertTo<string>(document["_id"]));
                        }
                        
                        ClearSyncStatus(item); //Clear the Sync Flag on Processed Rows

                    }
                    catch (SystemException e)
                    {
                        HandleError(status, e);
                    }
                    finally
                    {
                        status.Progress(items.Count, ++currentItem); //Update the Sync Progress
                    }
                }
            }
        }

        private void AddItemsBatch(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                foreach (var batch in items.Chunk(DataSourceReader.UpdateBatchSize))
                {
                    if (!status.ContinueProcessing)
                        break;

                    try
                    {
                        var documents = new List<BsonDocument>();
                        foreach (var item in batch)
                        {
                            // Get the Target Item Data
                            Dictionary<string, object> targetItem = AddItemToDictionary(Mapping, item);

                            // Create a BsonDocument from the Json
                            var document = BsonDocument.Parse(JsonConvert.SerializeObject(targetItem, Formatting.None));

                            // Assign the _id as an ObjectId
                            if (targetItem.TryGetValue("_id", out var id))
                            {
                                document["_id"] = ObjectId.Parse(DataSchemaTypeConverter.ConvertTo<string>(id));
                            }

                            documents.Add(document);
                        }

                        Collection.InsertMany(documents);

                        currentItem += documents.Count;

                        ClearSyncStatus(batch); //Clear the Sync Flag on Processed Rows

                    }
                    catch (SystemException e)
                    {
                        HandleError(status, e);
                    }
                    finally
                    {
                        status.Progress(items.Count, currentItem); //Update the Sync Progress
                    }
                }
            }
        }

        public override void UpdateItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                foreach (var item in items)
                {
                    if (!status.ContinueProcessing)
                        break;

                    try
                    {
                        var itemInvariant = new DataCompareItemInvariant(item);

                        var item_id = itemInvariant.GetTargetIdentifier<string>();

                        Automation?.BeforeUpdateItem(this, itemInvariant, item_id);

                        if (itemInvariant.Sync)
                        {
                            Dictionary<string, object> targetItem = UpdateItemToDictionary(Mapping, itemInvariant);

                            var filter = Builders<BsonDocument>.Filter.Eq("_id", ObjectId.Parse(item_id));                            
                            var update = Builders<BsonDocument>.Update;
                            var updates = new List<UpdateDefinition<BsonDocument>>();
                            
                            foreach(var key in targetItem.Keys)
                            {
                                var value = targetItem[key];
                                if (value is JToken j)
                                {
                                    updates.Add(update.Set(key, BsonDocument.Parse(JsonConvert.SerializeObject(j, Formatting.None))));
                                }
                                else if (value is JArray jarr)
                                {
                                    updates.Add(update.Set(key, BsonDocument.Parse(JsonConvert.SerializeObject(jarr, Formatting.None))));
                                }
                                else
                                {
                                    updates.Add(update.Set(key, value));
                                }
                            }
                                                        
                            Collection.UpdateOne(filter, update.Combine(updates));
                            
                            //Call the Automation AfterUpdateItem 
                            Automation?.AfterUpdateItem(this, itemInvariant, item_id);                          
                        }

                        ClearSyncStatus(item); //Clear the Sync Flag on Processed Rows
                    }
                    catch (SystemException e)
                    {
                        HandleError(status, e);
                    }
                    finally
                    {
                        status.Progress(items.Count, ++currentItem); //Update the Sync Progress
                    }

                }
            }
        }

        public override void DeleteItems(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if(DataSourceReader.UpdateBatchSize > 1)
            {
                DeleteItemsBatch(items, status);
                return;
            }

            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                foreach (var item in items)
                {
                    if (!status.ContinueProcessing)
                        break;

                    try
                    {
                        var itemInvariant = new DataCompareItemInvariant(item);

                        var item_id = itemInvariant.GetTargetIdentifier<string>();

                        Automation?.BeforeDeleteItem(this, itemInvariant, item_id);

                        if (itemInvariant.Sync)
                        {                            
                            Collection.DeleteOne(p => p["_id"] == ObjectId.Parse(item_id));                           
                            
                            Automation?.AfterDeleteItem(this, itemInvariant, item_id);
                        }

                        ClearSyncStatus(item); //Clear the Sync Flag on Processed Rows
                    }
                    catch (SystemException e)
                    {
                        HandleError(status, e);
                    }
                    finally
                    {
                        status.Progress(items.Count, ++currentItem); //Update the Sync Progress
                    }

                }
            }
        }

        private void DeleteItemsBatch(List<DataCompareItem> items, IDataSynchronizationStatus status)
        {
            if (items != null && items.Count > 0)
            {
                int currentItem = 0;

                foreach (var batch in items.Chunk(DataSourceReader.UpdateBatchSize))
                {
                    if (!status.ContinueProcessing)
                        break;

                    try
                    {
                        var ids = batch.Select(p => ObjectId.Parse(p.GetTargetIdentifier<string>())).ToList();
                        
                        Collection.DeleteMany(Builders<BsonDocument>.Filter.In("_id", ids));

                        currentItem += ids.Count;

                        ClearSyncStatus(batch); //Clear the Sync Flag on Processed Rows
                    }
                    catch (SystemException e)
                    {
                        HandleError(status, e);
                    }
                    finally
                    {
                        status.Progress(items.Count, currentItem); //Update the Sync Progress
                    }
                }
            }
        }

        public override void Execute(List<DataCompareItem> addItems, List<DataCompareItem> updateItems, List<DataCompareItem> deleteItems, IDataSourceReader reader, IDataSynchronizationStatus status)
        {
            DataSourceReader = reader as MongoDbDatasourceReader;

            if (DataSourceReader != null)
            {
                Mapping = new DataSchemaMapping(SchemaMap, DataCompare);

                // Create Connection
                Client = DataSourceReader.GetClient();
                Database = Client.GetDatabase(DataSourceReader.Database);
                Collection = Database.GetCollection<BsonDocument>(DataSourceReader.Collection);

                //Process the Changed Items
                if (addItems != null && status.ContinueProcessing) AddItems(addItems, status);
                if (updateItems != null && status.ContinueProcessing) UpdateItems(updateItems, status);
                if (deleteItems != null && status.ContinueProcessing) DeleteItems(deleteItems, status);

            }
        }

        private static void HandleError(IDataSynchronizationStatus status, Exception e)
        {
            if (!status.FailOnError)
            {
                status.LogMessage(e.Message);
            }
            if (status.FailOnError)
            {
                throw e;
            }
        }
    }
}
