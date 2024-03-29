﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data.SqlServerCe;
using System.Data;
using System.Diagnostics;

namespace SuperView_console
{
    class Utilities
    {     
  
        // Variable to control whether debug write is enabled/disabled
        static bool debug = true;

        // Function to write a data source into the database
        public static bool storeDataSource(string dataSourceName, string connectionString, string wrapper)
        {
            // Set up a new connection to the local db and connect
            SqlCeConnection dbConnection = connectToLocalDatabase();

            // Check that this data source doesn't already exist
            if (getDataSource(dataSourceName).Rows.Count > 0)
            {
                return false;
            }
            // If it doesn't then add it
            else
            {
                // Create a new command
                SqlCeCommand dbCmd = new SqlCeCommand(@"
                INSERT INTO DataSources
                    (Name, ConnectionString, Wrapper)
                VALUES (@Name, @ConnectionString, @Wrapper)", dbConnection);

                // Add our parameters
                dbCmd.Parameters.AddWithValue("Name", dataSourceName);
                dbCmd.Parameters.AddWithValue("ConnectionString", connectionString);
                dbCmd.Parameters.AddWithValue("Wrapper", wrapper);

                // Try to add the data source to the database
                try
                {
                    dbCmd.ExecuteNonQuery();
                }
                // Catch the exception if it occurs and return false
                catch (SqlCeException ex)
                {
                    Program.writeToDebug("Connection failed: " + ex.Message);
                    return false;
                }

                // Finish up and return success
                disconnectFromLocalDatabase(dbConnection);

                return true;
            }

        }

        // Function to store a data source table in the database
        public static bool storeTable(string dataSourceName, string tableName)
        {
            // Set up a new connection to the local db and connect
            SqlCeConnection dbConnection = connectToLocalDatabase();

            // Check that this table doesn't already exist
            if (getTable(dataSourceName, tableName).Rows.Count > 0)
            {
                return false;
            }
            // If it doesn't then add it
            else
            {
                // Get the ID of the data source
                int dataSourceID = getDataSourceID(dataSourceName);
                
                // Create a new command
                SqlCeCommand dbCmd = new SqlCeCommand(@"
                INSERT INTO DataSourceTables
                    (DataSource, Name)
                VALUES (@DataSource, @Name)", dbConnection);

                // Add our parameters
                dbCmd.Parameters.AddWithValue("DataSource", dataSourceID);
                dbCmd.Parameters.AddWithValue("Name", tableName);

                // Try to add the data source to the database
                try
                {
                    dbCmd.ExecuteNonQuery();
                }
                // Catch the exception if it occurs and return false
                catch (SqlCeException ex)
                {
                    Program.writeToDebug("Connection failed: " + ex.Message);
                    return false;
                }

                // Finish up and return success
                disconnectFromLocalDatabase(dbConnection);

                return true;
            }
        }

        // Function to write a new mapping to the database
        public static bool storeMapping(string sourceDataSource, string sourceTable, string sourceName, string sourceType, string targetName)
        {
            // Set up a new connection to the local db and connect
            SqlCeConnection dbConnection = connectToLocalDatabase();

            // Check that a mapping for this source field doesn't already exists
            if (getMappingTarget(sourceDataSource, sourceTable, sourceName, sourceType).Rows.Count > 0)
            {
                return false;
            }
            // If it doesn't then add it
            else
            {
                // Get the data source and table ids
                int dataSourceID = getDataSourceID(sourceDataSource);
                int tableID = getTableID(sourceDataSource, sourceTable);
                
                // Create a new command
                SqlCeCommand dbCmd = new SqlCeCommand(@"
                INSERT INTO Mappings
                    (SourceDataSource, SourceTable, SourceName, SourceType, TargetName)
                VALUES (@SourceDataSource, @SourceTable, @SourceName, @SourceType, @TargetName)", dbConnection);

                // Add our parameters
                dbCmd.Parameters.AddWithValue("SourceDataSource", dataSourceID);
                dbCmd.Parameters.AddWithValue("SourceTable", tableID);
                dbCmd.Parameters.AddWithValue("SourceName", sourceName);
                dbCmd.Parameters.AddWithValue("SourceType", sourceType);
                dbCmd.Parameters.AddWithValue("TargetName", targetName);

                // Try to add the data source to the database
                try
                {
                    dbCmd.ExecuteNonQuery();
                }
                // Catch the exception if it occurs and return false
                catch (SqlCeException ex)
                {
                    Program.writeToDebug("Connection failed: " + ex.Message);
                    return false;
                }

                // Finish up and return success
                disconnectFromLocalDatabase(dbConnection);

                return true;
            }
        }
        
        /********************************************************
         * DATASOURCES
         ******************************************************** */

        // Gets a specific data source from the local database
        public static DataTable getDataSource(string name)
        {
            SqlCeCommand dbCmd = new SqlCeCommand("SELECT * FROM DataSources WHERE Name = @name");
            dbCmd.Parameters.AddWithValue("name", name);

            return getLocalDatabaseData(dbCmd);
        }

        // Gets the id of a specific data source
        public static int getDataSourceID(string name)
        {
            DataTable results = getDataSource(name);
            int id = Int32.Parse(results.Rows[0]["ID"].ToString());
            return id;
        }

        // Gets all of the data sources stored in the local database
        public static DataTable getAllDataSources()
        {
            SqlCeCommand dbCmd = new SqlCeCommand("SELECT * FROM DataSources");

            return getLocalDatabaseData(dbCmd);
        }

        // Get the text name of a data source by the id
        public static string getDataSourceName(int id)
        {
            SqlCeCommand dbCmd = new SqlCeCommand("SELECT * FROM DataSources WHERE ID = @ID");
            dbCmd.Parameters.AddWithValue("ID", id);
            DataTable results = getLocalDatabaseData(dbCmd);

            string name = results.Rows[0]["Name"].ToString();

            return name;
        }

        /********************************************************
         * TABLES
         ******************************************************** */
        // Gets a specific table from the local database
        public static DataTable getTable(string dataSourceName, string tableName)
        {
            SqlCeCommand dbCmd = new SqlCeCommand("SELECT * FROM DataSourceTables WHERE DataSource = @DataSource AND Name = @Name");
            dbCmd.Parameters.AddWithValue("DataSource", getDataSourceID(dataSourceName));
            dbCmd.Parameters.AddWithValue("Name", tableName);

            return getLocalDatabaseData(dbCmd);
        }

        // Get the ID of a specific table in the local database
        public static int getTableID(string dataSourceName, string tableName)
        {
            DataTable results = getTable(dataSourceName, tableName);
            int id = Int32.Parse(results.Rows[0]["ID"].ToString());
            return id;
        }

        // Gets all of the tables stored in the database
        public static DataTable getAllTables()
        {
            SqlCeCommand dbCmd = new SqlCeCommand("SELECT * FROM DataSourceTables");

            return getLocalDatabaseData(dbCmd);
        }

        // Get the tables for a specific data source
        public static DataTable getDataSourceTables(string dataSourceName)
        {
            SqlCeCommand dbCmd = new SqlCeCommand("SELECT * FROM DataSourceTables WHERE DataSource = @DataSource");
            dbCmd.Parameters.AddWithValue("DataSource", getDataSourceID(dataSourceName));

            return getLocalDatabaseData(dbCmd);
        }

        // Get the text name of a table by id
        public static string getTableName(int id)
        {
            SqlCeCommand dbCmd = new SqlCeCommand("SELECT * FROM DataSourceTables WHERE ID = @ID");
            dbCmd.Parameters.AddWithValue("ID", id);
            DataTable results = getLocalDatabaseData(dbCmd);

            string name = results.Rows[0]["Name"].ToString();

            return name;
        }

        /********************************************************
         * MAPPINGS
         ******************************************************** */
        
        // Get the source of a mapping from the target name
        public static DataTable getMappingSource(string targetName)
        {
            SqlCeCommand dbCmd = new SqlCeCommand("SELECT * FROM Mappings WHERE TargetName = @TargetName");
            dbCmd.Parameters.AddWithValue("TargetName", targetName);

            return getLocalDatabaseData(dbCmd);
        }

        // Get the target name of a mapping from the source information
        public static DataTable getMappingTarget(string sourceDataSource, string sourceTable, string sourceName, string sourceType)
        {
            SqlCeCommand dbCmd = new SqlCeCommand(@"
                    SELECT * FROM Mappings 
                    WHERE 
                        SourceDataSource = @SourceDataSource 
                        AND SourceTable = @SourceTable 
                        AND SourceName = @SourceName 
                        AND SourceType = @SourceType");
            dbCmd.Parameters.AddWithValue("SourceDataSource", getDataSourceID(sourceDataSource));
            dbCmd.Parameters.AddWithValue("SourceTable", getTableID(sourceDataSource, sourceTable));
            dbCmd.Parameters.AddWithValue("SourceName", sourceName);
            dbCmd.Parameters.AddWithValue("SourceType", sourceType);

            return getLocalDatabaseData(dbCmd);
        }

        // Get all of the mappings for a data source
        public static DataTable getMappingsForDataSource(string dataSourceName)
        {
            SqlCeCommand dbCmd = new SqlCeCommand(@"
                    SELECT * FROM Mappings 
                    WHERE 
                        SourceDataSource = @SourceDataSource");
            dbCmd.Parameters.AddWithValue("SourceDataSource", getDataSourceID(dataSourceName));

            return getLocalDatabaseData(dbCmd);
        }

        // Get all of the mappings for a table
        public static DataTable getMappingsForTable(string dataSourceName, int tableID)
        {
            SqlCeCommand dbCmd = new SqlCeCommand(@"
                    SELECT * FROM Mappings 
                    WHERE 
                        SourceDataSource = @SourceDataSource
                        AND SourceTable = @SourceTable");
            dbCmd.Parameters.AddWithValue("SourceDataSource", getDataSourceID(dataSourceName));
            dbCmd.Parameters.AddWithValue("SourceTable", tableID);

            return getLocalDatabaseData(dbCmd);
        }

        // Get all of the mappings in the system
        public static DataTable getAllMappings()
        {
            SqlCeCommand dbCmd = new SqlCeCommand(@"
                    SELECT * FROM Mappings");

            return getLocalDatabaseData(dbCmd);
        }

        // Removes the mappings for a table
        public static void deleteMappingsForTable(string dataSourceName, string tableName)
        {
            SqlCeCommand dbCmd = new SqlCeCommand(@"
                    DELETE FROM Mappings 
                    WHERE 
                        SourceDataSource = @SourceDataSource
                        AND SourceTable = @SourceTable");

            dbCmd.Parameters.AddWithValue("SourceDataSource", getDataSourceID(dataSourceName));
            dbCmd.Parameters.AddWithValue("SourceTable", getTableID(dataSourceName, tableName));

            getLocalDatabaseData(dbCmd);
        }

        /********************************************************
         * RELATIONS
         ******************************************************** */

        // Writes a relation to the database
        public static bool storeRelation(int table1, int table2, string column1, string column2)
        {
            // Set up a new connection to the local db and connect
            SqlCeConnection dbConnection = connectToLocalDatabase();

            // Create a new command
            SqlCeCommand dbCmd = new SqlCeCommand(@"
                INSERT INTO Relations
                    (Table1, Table2, Column1, Column2)
                VALUES (@Table1, @Table2, @Column1, @Column2)", dbConnection);

            // Add our parameters
            dbCmd.Parameters.AddWithValue("Table1", table1);
            dbCmd.Parameters.AddWithValue("Table2", table2);
            dbCmd.Parameters.AddWithValue("Column1", column1);
            dbCmd.Parameters.AddWithValue("Column2", column2);

            // Try to add the data source to the database
            try
            {
                dbCmd.ExecuteNonQuery();
            }
            // Catch the exception if it occurs and return false
            catch (SqlCeException ex)
            {
                Program.writeToDebug("Connection failed: " + ex.Message);
                return false;
            }

            // Finish up and return success
            disconnectFromLocalDatabase(dbConnection);

            return true;
        }

        // Get all relations for a table
        public static DataTable getRelationsForTable(int tableID)
        {
            SqlCeCommand dbCmd = new SqlCeCommand(@"
                    SELECT * FROM Relations 
                    WHERE 
                        Table1 = @Table1
                        OR Table2 = @Table2");
            dbCmd.Parameters.AddWithValue("Table1", tableID);
            dbCmd.Parameters.AddWithValue("Table2", tableID);

            return getLocalDatabaseData(dbCmd);
        }

        // Get all the relations for a data source
        public static DataTable getRelationsForDataSource(int dataSourceID)
        {
            SqlCeCommand dbCmd = new SqlCeCommand(@"
                    SELECT Relations.*, Table1.DataSource AS DataSource1, Table2.DataSource AS DataSource2 
                    FROM Relations 
                    INNER JOIN DataSourceTables Table1 ON Table1.ID = Relations.Table1
                    INNER JOIN DataSourceTables Table2 ON Table2.ID = Relations.Table2");
            dbCmd.Parameters.AddWithValue("DataSourceID", dataSourceID);

            return getLocalDatabaseData(dbCmd);
        }


        /********************************************************
         * GENERAL FUNCTIONS
         ******************************************************** */

        public static void resetLocalDatabase()
        {
            // Delete data sources
            SqlCeCommand dbCmd = new SqlCeCommand("DELETE FROM DataSources");
            getLocalDatabaseData(dbCmd);
            
            // Delete data source tables
            dbCmd = new SqlCeCommand("DELETE FROM DataSourceTables");
            getLocalDatabaseData(dbCmd);

            // Delete mappings
            dbCmd = new SqlCeCommand("DELETE FROM Mappings");
            getLocalDatabaseData(dbCmd);

            // Delete relations
            dbCmd = new SqlCeCommand("DELETE FROM Relations");
            getLocalDatabaseData(dbCmd);
        }

        // General function to query and return data based on a SqlCeCommand passed in
        public static DataTable getLocalDatabaseData(SqlCeCommand inputDbCmd)
        {
            // Create a connection and connect
            SqlCeConnection dbConnection = connectToLocalDatabase();

            // Set up our passed in command
            SqlCeCommand dbCmd = inputDbCmd;
            dbCmd.Connection = dbConnection;

            // Create a new reader
            SqlCeDataReader dbReader;         

            // Create a new DataTable we will use to hold the results
            DataTable results = new DataTable();

            try
            {
                // Execute the query and return the data to the reader
                dbReader = dbCmd.ExecuteReader();

                results.Load(dbReader);

                // Close the reader
                dbReader.Close();
            }
            catch (SqlCeException ex)
            {
                Program.writeToDebug("Read failed: " + ex.Message);
            }

            

            // Close the connection
            disconnectFromLocalDatabase(dbConnection);

            // Return the DataTable containing the results
            return results;
        }

        // Creates and returns a new connection to the local database
        public static SqlCeConnection connectToLocalDatabase()
        {
            SqlCeConnection dbConnection = new SqlCeConnection();
            dbConnection.ConnectionString = "Data Source = SuperView.sdf";
            dbConnection.Open();

            return dbConnection;
        }

        // Function to disconnect from the passed in conection
        public static void disconnectFromLocalDatabase(SqlCeConnection dbConnection)
        {
            dbConnection.Close();
        }

        // Function for writing a message to debug
        public static void writeToDebug(string output)
        {
            if (debug)
            {
                Debug.WriteLine(output);
            }
        }
    }
}
