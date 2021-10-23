using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Reflection;
using System.Windows.Forms;

namespace Simego.DataSync.Providers.MongoDb.TypeConverters
{
    class CollectionTypeConverter : StringConverter
    {
        public override bool GetStandardValuesSupported(ITypeDescriptorContext context)
        {
            //true means show a combobox
            return true;
        }

        public override bool GetStandardValuesExclusive(ITypeDescriptorContext context)
        {
            //true will limit to list. false will show the list, 
            //but allow free-form entry
            return true;
        }

        public override StandardValuesCollection GetStandardValues(ITypeDescriptorContext context)
        {
            PropertyGrid grid = context.GetType().InvokeMember(
                    "OwnerGrid",
                    BindingFlags.GetProperty | BindingFlags.Instance | BindingFlags.Public,
                    null,
                    context,
                    null) as PropertyGrid;

            List<string> vCol = new List<string>();
            try
            {
                if (grid != null)
                    grid.Cursor = Cursors.WaitCursor;

                dynamic reader = context.Instance;
                if (reader != null)
                {
                    if (!string.IsNullOrEmpty(reader.ConnectionString) && !string.IsNullOrEmpty(reader.Database))
                    {
                        var lists = Cache.GetCacheItem($"MongoDb.DatabaseTypeConverter.{reader.Database}",
                            () => reader.GetCollections());

                        vCol.AddRange(lists);

                        vCol.Sort();
                    }
                }
            }
            catch (ArgumentException)
            {

            }
            finally
            {
                if (grid != null)
                    grid.Cursor = Cursors.Default;
            }

            return new StandardValuesCollection(vCol);
        }
    }
}
