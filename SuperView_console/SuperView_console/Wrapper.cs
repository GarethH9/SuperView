using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace SuperView_console
{
    class Wrapper
    {      
        // Private variables used to store information on a wrapper
        private string dataSourceName;
        private int dataSourceID;
        private string connectionString;

        // Constructor for a wrapper
        public Wrapper(string newDataSourceName, string newConnectionString, int dataSourceID = -1)
        {            
            this.dataSourceName = newDataSourceName;
            this.connectionString = newConnectionString;
            this.dataSourceID = dataSourceID;
        }

        // Getter for the data source name
        public string getName()
        {
            return dataSourceName;
        }

        // Getter for the data source ID
        public int getID()
        {
            return dataSourceID;
        }

        // Getter for the connection string of the wrapper
        public string getConnectionString()
        {
            return connectionString;
        }

        // Connect method
        public virtual bool connect()
        {
            return false;
        }

        // Disconnect method
        public virtual bool disconnect()
        {
            return false;
        }

        // Query method
        public virtual DataTable query(string query)
        {
            return new DataTable();
        }

        // Get tables
        public virtual IList<string> getTables()
        {
            return new List<string>();
        }

        // Get columns
        public virtual Dictionary<string, Dictionary<string,string>> getColumns(string tableName)
        {
            return new Dictionary<string, Dictionary<string,string>>();
        }

        //Get the status of a data source

        //Transmit data to the data source

        //Retrieve data from the data source

        //Direct pass through for servers that don't require wrappers

    }
}
