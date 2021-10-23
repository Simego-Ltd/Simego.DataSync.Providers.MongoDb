using System;
using Simego.DataSync.Providers.MongoDb.TypeConverters;
using System.Collections.Generic;
using System.ComponentModel;

namespace Simego.DataSync.Providers.MongoDb
{
    class ConnectionProperties
    {
        private readonly MongoDbDatasourceReader _reader;
        
        [Category("Settings")]
        [Description("MongoDb Server Connection String")]
        public string ConnectionString 
        { 
            get => _reader.ConnectionString;
            set => _reader.ConnectionString = value;
        }
        
        [Category("Settings")]
        [Description("MongoDb Database")]
        [TypeConverter(typeof(DatabaseTypeConverter))]
        public string Database 
        { 
            get => _reader.Database;
            set => _reader.Database = value;
        }
        
        [Category("Settings")]
        [Description("MongoDb Collection")]
        [TypeConverter(typeof(CollectionTypeConverter))]
        public string Collection 
        { 
            get => _reader.Collection;
            set => _reader.Collection = value;
        }

        [Category("Filter")]
        [Description("MongoDb Document Filter Expression")]
        public string DocumentFilter 
        { 
            get => _reader.DocumentFilter;
            set => _reader.DocumentFilter = value;
        }

        public ConnectionProperties(MongoDbDatasourceReader reader)
        {
            _reader = reader ?? throw new ArgumentNullException((nameof(reader)));
        }

        public List<string> GetDatabases() => _reader.GetDatabases();

        public List<string> GetCollections() => _reader.GetCollections();
    }
}
