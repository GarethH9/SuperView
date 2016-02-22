using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace SuperView_console
{
    class DataSource
    {
        // Private variables to store information about the data source
        private string dataSourceName;
        private string connectionString;
        public static SqlConnection dataSourceConnection;

        // Class constructor
        public DataSource(string newDataSourceName, string newConnectionString)
        {            
            this.dataSourceName = newDataSourceName;
            this.connectionString = newConnectionString;
        }

        // Getter for the connection string for the data source
        public string getConnectionString()
        {
            return connectionString;
        }

        // Getter for the connection object
        public SqlConnection getConnection()
        {
            return dataSourceConnection;
        }

        // Setter for the connection object
        public static void setConnection(SqlConnection dbConnection)
        {
            dataSourceConnection = dbConnection;
        }

        //Load data sources from the config location
        public static DataSource loadDataSourceDefintion(string configFile)
        {
            // Read the data from the specified text file
            string path = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), @"Config", configFile + ".txt");
            string[] configSettings = System.IO.File.ReadAllLines(path);

            // Create new DataSource based on this wrapper information
            DataSource newDataSource = new DataSource(configSettings[0], configSettings[1]);

            // Use the wrapper to connect to the data source

            return newDataSource;
        }

    }
}
