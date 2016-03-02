using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace SuperView_console
{
    class MappingEngine
    {
        // Use a wrapper to query a data source
        public static DataTable queryWrapper(Wrapper wrapper, string query)
        {
            return wrapper.query(query);
        }

        // Store a table in the mapping engine
        public static bool mapTable(Wrapper wrapper, string tableName)
        {
            return Utilities.storeTable(wrapper.getName(), tableName);
        }

        // Gets the source of a mapping
        public static DataTable getMappingSource(string targetField)
        {
            return Utilities.getMappingSource(targetField);
        }

        // Gets the target of a mapping
        public static DataTable getMappingTarget(Wrapper wrapper, string sourceTable, string sourceName, string sourceType)
        {
            return Utilities.getMappingTarget(wrapper.getName(), sourceTable, sourceName, sourceType);
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
        public static DataTable getMappings(Wrapper wrapper = null, string tableName = "")
        {
            // If we have specified a wrapper
            if (wrapper != null)
            {
                // If we have specified a table name
                if (tableName != "")
                {
                    // Data source and table specified
                    return Utilities.getMappingsForTable(wrapper.getName(), tableName);
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
        public static bool storeRelation(int table1, int table2, string relation)
        {
            Utilities.storeRelation(table1, table2, relation);

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
