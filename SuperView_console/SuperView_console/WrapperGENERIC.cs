using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;

namespace SuperView_console
{
    class WrapperGENERIC : Wrapper
    {
        // Private variable to hold a connection object of the relevant type

        public WrapperGENERIC(string newDataSourceName, string newConnectionString) : base(newDataSourceName, newConnectionString)
        {
            // Build your conection object
            // Try to use other wrapper methods (e.g. this.getConnectionString() to do this)
        }

        public override bool connect()
        {
            // Try to connect to the database
            // Return: true - connection successful, false - conection failed
            try
            {
                //Connect
                return true;
            }
            // Catch any errors that occur (replace ex with a relevant exception)
            catch (SqlException ex)
            {
                //Output error
                Program.writeToDebug("Connection failed: " + ex.Message);
                return false;
            }
        }

        public override bool disconnect()
        {
            // Try to disconnect from the database
            // Return: true - connection successful, false - conection failed
            try
            {
                //Disconnect
                return true;
            }
            // Catch any errors that occur (replace ex with a relevant exception)
            catch (SqlException ex)
            {
                //Output error
                Program.writeToDebug("Connection failed: " + ex.Message);
                return false;
            }
        }

        public override DataTable query(string query)
        {
            // Query the data source and return a DataTable containing the results of the query
            // Each column in the data table should represent a column that wil be mapped by the system
            
            DataTable results = new DataTable();
            return results;
        }
    }
}
