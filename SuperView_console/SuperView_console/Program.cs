using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace SuperView_console
{
    class Program
    {
        public static bool debug = true;
        
        static void Main(string[] args)
        {
            // Launch the program
            Console.WriteLine("Welcome to SuperView \n");

            // Display the menu of SuperView options
            showMenu();
        }

        // Function to show the menu of SuperView options
        static void showMenu()
        {
            Console.WriteLine("");
            Console.WriteLine("SuperView Options:");
            Console.WriteLine("1. System demo");
            Console.WriteLine("2. System test");
            Console.WriteLine("3. Start system");
            Console.WriteLine("4. Exit");
            Console.Write("Please enter a number: ");

            switch (Console.ReadLine())
            {
                case "1":
                    Console.WriteLine("Loading system demo... \n");
                    systemDemo();
                    showMenu();
                    break;
                case "2":
                    Console.WriteLine("Loading the system test... \n");
                    testSystem();
                    showMenu();
                    break;
                case "3":
                    Console.WriteLine("Loading system... \n");
                    startSuperView();
                    showMenu();
                    break;
                case "4":
                    exitSystem();
                    break;
                default:
                    Console.WriteLine("Loading system demo... \n");
                    systemDemo();
                    showMenu();
                    break;

            }
        }

        static void testSystem()
        {
            // Create a new SQLSERVER wrapper
            WrapperSQLSERVER SuperViewTest = new WrapperSQLSERVER("SuperViewTest", "Data Source=(local);Initial Catalog=SuperView;User id=sa;Password=Pa55w0rd;");

            // Attempt a connection to the database
            if (SuperViewTest.connect())
            {
                Console.WriteLine("Connection successful  \n");
                SuperViewTest.disconnect();
            }

            // QUERY TEST
            Console.WriteLine("QUERY TEST");
            string query = "SELECT * FROM Test;";
            Console.WriteLine("Query: " + query);

            // Query the database
            DataTable results = SuperViewTest.query(query);

            // Output the results of the query
            DisplayDataTable(results);
            // END QUERY TEST

            Console.WriteLine("");

            // GET SCHEMA TEST
            Console.WriteLine("GET SCHEMA TEST");
            IList<string> tables = SuperViewTest.getTables();

            // Display the list
            DisplayIList(tables);
            // END GET SCHEMA TEST

            Console.WriteLine("");

            //MAPPING ENGINE TEST
            Console.WriteLine("MAPPING ENGINE TEST");
            results = MappingEngine.queryWrapper(SuperViewTest, "SELECT * FROM Test");

            DisplayDataTable(results);
            //END MAPPING ENGINE TEST

        }

        static void startSuperView()
        {
            // Create a new SQLSERVER wrapper
            WrapperSQLSERVER SuperViewTest = new WrapperSQLSERVER("SuperViewTest", "Data Source=(local);Initial Catalog=SuperView;User id=sa;Password=Pa55w0rd;");

            //Write the new datasource to the database
            Console.WriteLine("Storing data source information...");
            bool success = Utilities.storeDataSource("SuperViewTest", "Data Source=(local);Initial Catalog=SuperView;User id=sa;Password=Pa55w0rd;", "WrapperSQLSERVER");
            Console.WriteLine("Data source added to database: " + success.ToString());
            Console.WriteLine("");
            Console.WriteLine("Data sources stored in system:");
            DataTable results = Utilities.getAllDataSources();
            DisplayDataTable(results);
            Console.WriteLine("");
            Console.WriteLine(Utilities.getDataSourceID("SuperViewTest").ToString());    

        }

        // Function to display a DataTable in the console
        public static void DisplayDataTable(DataTable table)
        {
            foreach (DataRow dataRow in table.Rows)
            {
                foreach (var item in dataRow.ItemArray)
                {
                    Console.WriteLine(item);
                }
            }
        }

        // Function to display a list in the console
        public static void DisplayIList(IList<string> list)
        {
            List<string> newList = list.ToList();
            newList.ForEach(i => Console.WriteLine("{0}\t", i));
        }

        //Load a data source configuration so that we can create a wrapper
        public static Wrapper loadDataSourceDefintion(string configFile)
        {
            // Read the data from the specified text file
            string path = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location), @"Config", configFile + ".txt");
            string[] configSettings = System.IO.File.ReadAllLines(path);

            // Create a new wrapper baswed on this config information
            Wrapper newWrapper = new Wrapper(configSettings[0], configSettings[2]);

            // Use the wrapper to connect to the data source

            return newWrapper;
        }

        // Launch a basic demo of the system
        static void systemDemo()
        {
            Console.WriteLine("");

            //Create a new data source
            DataSource SuperViewTest = new DataSource("SuperView Test", "Data Source=(local);Initial Catalog=SuperView;User id=sa;Password=Pa55w0rd;");

            //Connect to the database
            Console.WriteLine("Attempting to connect to the database... \n");

            //Call our connect function
            SqlConnection dbConnection = connectToDatabase(SuperViewTest.getConnectionString());

            //We can now allow the user to query the database
            Console.WriteLine("You may now enter a query to be executed on the datasource:");
            string query = Console.ReadLine();

            //Display the query results
            displayQuery(dbConnection, query);
        }

        static void exitSystem()
        {
            //Exit the program
            Console.WriteLine("Press any key to exit...");
            Console.ReadLine();
        }

        static SqlConnection connectToDatabase(string connectionString)
        {
            //Build a new connection
            SqlConnection dbConnection = new SqlConnection();
            dbConnection.ConnectionString = connectionString;
                
            //Try connection to the database
            try
            {
                dbConnection.Open();
                Console.WriteLine("Database connection successful");
                dbConnection.Close();
            }
            //Catch any connection errors
            catch (SqlException ex)
            {
                Console.WriteLine("Connection failed: " + ex.Message);
            }

            //We will return the database connection object so we can use it again
            return dbConnection;
        }

        static SqlDataReader executeQuery(SqlConnection dbConnection, string query)
        {
            //Set up a new database command and reader
            SqlCommand dbCmd = new SqlCommand();
            SqlDataReader dbReader;

            //Define the query and the connection
            dbCmd.CommandText = query;
            dbCmd.CommandType = CommandType.Text;
            dbCmd.Connection = dbConnection;

            //Open the connection
            dbConnection.Open();

            //Execute the query and return the data to the reader
            dbReader = dbCmd.ExecuteReader();

            //Close the connection
            dbConnection.Close();

            //Return the database reader
            return dbReader;
        }

        static void displayQuery(SqlConnection dbConnection, string query)
        {
            //Set up a new database command and reader
            SqlCommand dbCmd = new SqlCommand();
            SqlDataReader dbReader;

            //Define the query and the connection
            dbCmd.CommandText = query;
            dbCmd.CommandType = CommandType.Text;
            dbCmd.Connection = dbConnection;

            //Open the connection
            dbConnection.Open();

            //Execute the query and return the data to the reader
            dbReader = dbCmd.ExecuteReader();

            Console.WriteLine("");

            //If we have rows then we will display them
            if (dbReader.HasRows)
            {
                while (dbReader.Read())
                {
                    Console.WriteLine("{0}\t{1}", dbReader.GetInt32(0), dbReader.GetString(1));
                }
            }
            else
            {
                Console.WriteLine("No rows found \n");
            }

            //Close the reader and connection
            dbReader.Close();
            dbConnection.Close();
        }

        public static void writeToDebug(string output)
        {
            if (debug)
            {
                Debug.WriteLine(output);
            }
        }
    }
}
