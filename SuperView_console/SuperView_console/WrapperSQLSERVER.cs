using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace SuperView_console
{
    class WrapperSQLSERVER : Wrapper
    {
        private SqlConnection dbConnection;

        public WrapperSQLSERVER(string newDataSourceName, string newConnectionString, int newDataSourceID = -1) : base(newDataSourceName, newConnectionString, newDataSourceID)
        {
            // Build a new SQL connection
            dbConnection = new SqlConnection();
            dbConnection.ConnectionString = this.getConnectionString();
        }

        public override bool connect()
        {
            // Try to connect to the database
            try
            {
                dbConnection.Open();
                return true;
            }
            // Catch any errors that occur
            catch (SqlException ex)
            {
                Program.writeToDebug("Connection failed: " + ex.Message);
                return false;
            }
        }

        public override bool disconnect()
        {
            // Try to disconnect to the database
            try
            {
                dbConnection.Close();
                return true;
            }
            // Catch any errors that occur
            catch (SqlException ex)
            {
                Program.writeToDebug("Connection failed: " + ex.Message);
                return false;
            }
        }

        public override DataTable query(string query)
        {
            // Create a new SQL command and reader
            SqlCommand dbCmd = new SqlCommand();
            SqlDataReader dbReader;

            // Define the query
            dbCmd.CommandText = query;
            dbCmd.CommandType = CommandType.Text;
            dbCmd.Connection = dbConnection;

            // Open the connection
            this.connect();

            // Execute the query and return the data to the reader
            dbReader = dbCmd.ExecuteReader();

            // Create a new DataTable we will use to hold the results
            DataTable results = new DataTable();
            results.Load(dbReader);

            // Close the reader
            dbReader.Close();

            // Close the connection
            this.disconnect();

            // Return the DataTable containing the results
            return results;
        }

        public override IList<string> getTables()
        {
            // Create a placeholder list
            List<string> tables = new List<string>();

            // Open the connection
            this.connect();

            // Create a DataTable which we will use to store the results of the schema get
            DataTable results = dbConnection.GetSchema("Tables");

            // Now populate the list with the table names
            foreach (DataRow row in results.Rows)
            {
                tables.Add((string)row[2]);
            }

            // Disconnect
            this.disconnect();

            return tables;
        }

        public override Dictionary<string, string> getColumns(string tableName)
        {
            string[] restrictions = new string[4];
            restrictions[2] = tableName;

            // Open the connection
            this.connect();

            // Get the columns
            DataTable results = dbConnection.GetSchema("Columns", restrictions);
            
            // Create a dictionary to store the results
            Dictionary<string, string> columns = new Dictionary<string, string>();

            // Create a new data table with just the columns we need
            foreach (DataRow row in results.Rows)
            {
                columns.Add(row["column_name"].ToString(), row["data_type"].ToString());
            }

            // Disconnect
            this.disconnect();

            return columns;
        }
    }
}
