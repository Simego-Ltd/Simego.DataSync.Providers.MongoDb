using System.Collections.Generic;

namespace Simego.DataSync.Providers.MongoDb.Extensions
{
    static class SchemaMapExtensions
    {
        public static void AddIfNotExists(this List<DataSchemaItem> schema, DataSchemaItem item)
        {
            if (schema.Exists(p => p.ColumnName == item.ColumnName)) return;
            schema.Add(item);
        }
    }
}
