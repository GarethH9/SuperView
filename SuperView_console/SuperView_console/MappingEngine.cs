using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace SuperView_console
{
    class MappingEngine
    {
        // Use a wrapper to query a data source
        public static DataTable queryWrapper(Wrapper wrapper, string query)
        {
            return wrapper.query(query);
        }

        // Gets the name of a mapped table by its ID
        public static string getTableName(int tableID)
        {
            return Utilities.getTableName(tableID);
        }

        // Store a table in the mapping engine
        public static bool mapTable(Wrapper wrapper, string tableName)
        {
            return Utilities.storeTable(wrapper.getName(), tableName);
        }

        // Gets the source of a mapping
        public static DataTable getMappingSource(string targetField)
        {
            Console.WriteLine(targetField);
            return Utilities.getMappingSource(targetField);
        }

        // Gets the data source of a mapping
        public static Wrapper getMappingDataSource(string targetField)
        {
            //Find the mapping, get the ID of the first source data source
            DataTable mapping = MappingEngine.getMappingSource(targetField);
            int sourceDataSource = (int)mapping.Rows[0]["SourceDataSource"];

            return getDataSourceWrapper(sourceDataSource);
        }

        // Gets the target of a mapping
        public static DataTable getMappingTarget(Wrapper wrapper, string sourceTable, string sourceName, string sourceType)
        {
            return Utilities.getMappingTarget(wrapper.getName(), sourceTable, sourceName, sourceType);
        }

        // Gets a wrapper based on a datasource ID
        public static Wrapper getDataSourceWrapper(int dataSourceID)
        {
            //Get the name of the data source
            string dataSourceName = Utilities.getDataSourceName(dataSourceID);

            Console.WriteLine(dataSourceName);

            //Return the data source
            return Program.dataSources[dataSourceName];

        }

        // Maps a field in a table in a data source
        public static void mapField(Wrapper wrapper, string sourceTable, string sourceName, string sourceType, string targetName)
        {
            Utilities.storeMapping(wrapper.getName(), sourceTable, sourceName, sourceType, targetName);
        }

        // Maps the tables in a data source
        public static void mapDataSourceTables(Wrapper wrapper)
        {            
            // Loop through the tables and add them to the mapping engine
            foreach (string table in wrapper.getTables())
            {
                mapTable(wrapper, table);
            }
        }

        // Gets all of the mapped tables for a data source
        public static DataTable getMappedTables(Wrapper wrapper = null)
        {
            // If we are not getting the table for specific data source then we will get all of them
            if (wrapper == null)
            {
                return Utilities.getAllTables();
            }
            // Otherwise just get the table for the specified data source
            else
            {
                return Utilities.getDataSourceTables(wrapper.getName());
            }
        }

        // Gets all of the mapped fields
        public static DataTable getMappings(Wrapper wrapper = null, int tableID = 0)
        {
            // If we have specified a wrapper
            if (wrapper != null)
            {
                // If we have specified a table name
                if (tableID != 0)
                {
                    // Data source and table specified
                    return Utilities.getMappingsForTable(wrapper.getName(), tableID);
                }
                else
                {
                    // Data source only
                    return Utilities.getMappingsForDataSource(wrapper.getName());
                }
            }
            else
            {
                // All mappings
                return Utilities.getAllMappings();
            }
        }

        // Store a new relation
        public static bool storeRelation(int table1, int table2, string column1, string column2)
        {
            Utilities.storeRelation(table1, table2, column1, column2);

            return true;
        }

        // Get relations for a table
        public static DataTable getRelationsForTable(int tableID)
        {
            return Utilities.getRelationsForTable(tableID);
        }
 
        //Get fields from data source

        //Create relationship

        //Get data source from wrapper

        //Transmit query to wrappers

        //Receieve data from wrappers
    }
}
