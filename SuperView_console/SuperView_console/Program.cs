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

        public static Dictionary<string, Wrapper> dataSources = new Dictionary<string, Wrapper>();
        
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
            Console.WriteLine("4. Use Superview");
            Console.WriteLine("5. Reset");
            Console.WriteLine("6. Exit");
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
                    useSuperView();
                    showMenu();
                    break;
                case "5":
                    reset();
                    showMenu();
                    break;
                case "6":
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

            // GET COLUMNS TEST
            Console.WriteLine("GET COLUMNS TEST");
            Dictionary<string, Dictionary<string, string>> columns = SuperViewTest.getColumns("Test");

            // Display the columns
            foreach (KeyValuePair<string, Dictionary<string,string>> column in columns)
            {
                Console.WriteLine("Column Name: " + column.Key.ToString() + " Data Type: " + column.Value["data_type"].ToString());
            }
            //DisplayDataTable(columns);

            // END GET COLUMNS TEST

            Console.WriteLine("");

            //MAPPING ENGINE TEST
            Console.WriteLine("MAPPING ENGINE TEST");
            results = MappingEngine.queryWrapper(SuperViewTest, "SELECT * FROM Test");

            DisplayDataTable(results);
            //END MAPPING ENGINE TEST

        }

        // Function to start the SuperView system including initialising and mapping all defined data sources
        static void startSuperView()
        {
            string response;
            
            Console.WriteLine("Loading data source configurations...");

            // Load the data source definitions and create the required objects
            loadDataSourceDefinitions();
            createDataSourceObjects();
            Console.WriteLine(dataSources.Count().ToString() + " data source(s) loaded");

            Console.WriteLine("");

            // Loop through each of the data sources
            foreach (KeyValuePair<string, Wrapper> dataSource in dataSources)
            {
                Wrapper wrapper = dataSource.Value;
                
                Console.WriteLine("Data Source: " + wrapper.getName() + " (ID: " + wrapper.getID().ToString() + ")");
                Console.Write("Would you like to configure mappings for this data source? (Y/N): ");

                string reponse = Console.ReadLine();

                // Perform the mappings
                if (reponse.ToString().ToLower() == "y")
                {
                    // Map the tables
                    Console.WriteLine("Mapping tables...");
                    
                    // Give the user the option to preserve table mappings
                    if (MappingEngine.getMappedTables(wrapper).Rows.Count > 0)
                    {
                        Console.WriteLine("Table mappings already exist for '" + wrapper.getName() + "'");
                    }
                    else
                    {
                        // Loop through the tables
                        foreach (string table in wrapper.getTables())
                        {
                            // Ask the user if they want to map the table
                            Console.Write("Would you like to map the '" + table + "' table? (Y/N): ");
                            response = Console.ReadLine().ToString();

                            // If they do then map the table
                            if (response.ToLower() == "y")
                            {
                                MappingEngine.mapTable(wrapper, table);
                                Console.WriteLine(table);
                            }
                        }

                        // Display how many tables we have mapped in this data source
                        Console.WriteLine(MappingEngine.getMappedTables(wrapper).Rows.Count.ToString() + " table(s) mapped");
                    }

                    Console.WriteLine("");

                    // Map the fields
                    Console.WriteLine("Mapping fields...");

                    // Loop through every table that we have mapped for this data source
                    foreach (DataRow mapping in MappingEngine.getMappedTables(wrapper).Rows)
                    {
                        string table = mapping["Name"].ToString();
                        
                        // Check if there are any mappings for this table, if there are then we can't change them
                        if (MappingEngine.getMappings(wrapper, table).Rows.Count > 0)
                        {
                            Console.WriteLine("Mappings already exist for the '" + table + "' table.");
                        }
                        else
                        {
                            // Loop through each column in the data source
                            foreach (KeyValuePair<string, Dictionary<string, string>> column in wrapper.getColumns(table))
                            {
                                Console.WriteLine("Column Name: " + column.Key.ToString() + " | Data Type: " + column.Value["data_type"].ToString());
                                Console.Write("Enter a mapping name for this column (leave blank to ignore): ");
                                string mappingName = Console.ReadLine().ToString();

                                // If we entered a mapping name then add the mapping to the database
                                if (mappingName != "")
                                {
                                    MappingEngine.mapField(wrapper, table, column.Key.ToString(), column.Value.ToString(), mappingName);
                                }

                                Console.WriteLine("");
                            }
                            Console.WriteLine(MappingEngine.getMappings(wrapper, table).Rows.Count.ToString() + " field(s) mapped");
                        }
                    }

                }
            }

            Console.WriteLine("");

            // Set up relations
            Console.WriteLine("Creating relations...");

            Console.Write("Would you like to add a relation? (Y/N): ");
            response = Console.ReadLine();

            // While the user wants to add more relations
            while (response.ToString().ToLower() == "y")
            {
                // Get all of the mappings we have produced
                DataTable mappings = MappingEngine.getMappings();

                // Display the mappings (along with their ID)
                foreach (DataRow mapping in mappings.Rows)
                {
                    string dataSourceName = Utilities.getDataSourceName((int)mapping["sourceDataSource"]);
                    string tableName = Utilities.getTableName((int)mapping["sourceTable"]);
                    Console.WriteLine("ID: " + mapping["ID"].ToString() + " Name: " + mapping["targetName"].ToString() + " (Data Source: " + dataSourceName + "| Table: " + tableName + " Table ID: " + mapping["sourceTable"]+ ")");
                }

                Console.Write("Enter table 1 ID: ");
                int table1 = Int32.Parse(Console.ReadLine().ToString());
                Console.Write("Enter table 2 ID: ");
                int table2 = Int32.Parse(Console.ReadLine().ToString());
                Console.Write("Enter relation (E.g. ID = ID): ");
                string relation = Console.ReadLine().ToString();

                // Check if the user wants to make another mapping
                Console.Write("Would you like to add a relation? (Y/N): ");
                response = Console.ReadLine();
            }
        }

        // Function to use SuperView once initialised
        public static void useSuperView()
        {
            Console.WriteLine("");
            
            // Get all of the mappings we have produced
            DataTable mappings = MappingEngine.getMappings();

            // Display the mappings
            foreach (DataRow mapping in mappings.Rows)
            {
                string dataSourceName = Utilities.getDataSourceName((int)mapping["sourceDataSource"]);
                string tableName = Utilities.getTableName((int)mapping["sourceTable"]);
                Console.WriteLine(mapping["targetName"].ToString() + " (Data Source: " + dataSourceName + "| Table: " + tableName + ")");
            }
        }

        // Function to reset SuperView
        public static void reset()
        {
            // Empty the dataSources dictionary
            dataSources = new Dictionary<string, Wrapper>();

            // Reset the local database
            Utilities.resetLocalDatabase();

            Console.WriteLine("SuperView reset!");
            Console.WriteLine("");
        }

        // Function to load all of the data source configurations from text files
        public static void loadDataSourceDefinitions()
        {
            // Add data sources to the local database
            Utilities.storeDataSource("SuperViewTest", "Data Source=(local);Initial Catalog=SuperView;User id=sa;Password=Pa55w0rd;", "WrapperSQLSERVER");

            Utilities.storeDataSource("PatientMealChoices", "Data Source=(local);Initial Catalog=PatientMealChoices;User id=sa;Password=Pa55w0rd;", "WrapperSQLSERVER");

        }

        public static void createDataSourceObjects()
        {
            // Get all the data sources in the system
            DataTable dt = Utilities.getAllDataSources();

            // Loop through each
            foreach (DataRow row in dt.Rows)
            {
                // Create a new object

                // Get the type of object (wrapper) to create
                Type elementType = Type.GetType("SuperView_console." + row["Wrapper"].ToString(),true);
                Object wrapper = Activator.CreateInstance(elementType, row["Name"].ToString(), row["ConnectionString"].ToString(), Int32.Parse(row["ID"].ToString()));

                // Add the object to the dictionary (if it already exists then overwrite)
                if (dataSources.ContainsKey(row["Name"].ToString()))
                {
                    dataSources[row["Name"].ToString()] = (Wrapper)wrapper;
                }
                else
                {
                    dataSources.Add(row["Name"].ToString(), (Wrapper)wrapper);
                }
            }
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
