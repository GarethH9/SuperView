using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace SuperView_console
{
    class QueryEngine
    {
        //Query the virtual data source
        public static Dictionary<string, DataTable> query(string query)
        {
            // Basic implementation
            
            // Get the WHERE clause (this is all we are passing in for now)
            List<string> qWhere = query.Split(' ').ToList<string>();
            string qWhereField = qWhere[0];
            string qWhereOperator = qWhere[1];
            string qWhereValue = qWhere[2];

            // Find the mapping for the WHERE field
            DataTable mapping = MappingEngine.getMappingSource(qWhereField);
            
            // Store the table ID
            int whereSourceTable = (int)mapping.Rows[0]["SourceTable"];
            // Store the datas source ID
            int whereDataSource = (int)mapping.Rows[0]["SourceDataSource"];

            // Build a dictionary to hold our results
            Dictionary<string, DataTable> results = new Dictionary<string, DataTable>();

            // We must now loop through all of our data sources and query them for their data
            foreach (KeyValuePair<string, Wrapper> dataSource in Program.dataSources)
            {
                // Store the wrapper so it's easier to reference
                Wrapper wrapper = dataSource.Value;

                Dictionary<string, Dictionary<string, string>> where = null;
                
                // Does our WHERE apply to this data source?
                if (dataSource.Value.getID() == whereDataSource)
                {
                    // We need to build a WHERE clause for this data source
                    // Get the name of the table the WHERE applies to
                    string whereTableName = MappingEngine.getTableName(whereSourceTable);

                    // Build the WHERE clause dictionary
                    where = new Dictionary<string, Dictionary<string, string>>();
                    // Add the field name to the dictionary
                    where.Add(qWhereField, new Dictionary<string, string>());
                    // Add the operator and value to the dictionary for that field
                    where[qWhereField].Add("operator", qWhereOperator);
                    where[qWhereField].Add("value", qWhereValue);
                }

                // Generate a list of columns that we want from this data source, for now we just get them all
                // TODO - Only get the columns we actually need (can we do this?)
                List<string> columns = new List<string> { "*" };

                // Generate a list of tables that we want for this data source, for now we just get all the mapped tables
                // TODO - Only get tables we actually need (can we do this?)
                DataTable mappedTables = MappingEngine.getMappedTables(wrapper);
                List<string> tables = mappedTables.AsEnumerable().Select(row => row.Field<string>("Name")).ToList();

                results[dataSource.Key] = wrapper.queryWithWhere(columns, tables, where);
            }

            return results;
        }

        public static DataTable joinData(Dictionary<string, DataTable> results)
        {

            DataTable resultsTable = new DataTable();

            DataTable table1 = results["SuperViewTest"];
            DataTable table2 = results["PatientMealChoices"];

            // Prefix the columns in table 1
            var dt1Columns = table1.Columns.OfType<DataColumn>().Select(dc => new DataColumn("SuperViewTest_" + dc.ColumnName, dc.DataType, dc.Expression, dc.ColumnMapping));
            // Add them to the results table
            resultsTable.Columns.AddRange(dt1Columns.ToArray());

            // Prefix the columns in table 2
            var dt2Columns = table2.Columns.OfType<DataColumn>().Select(dc => new DataColumn("PatientMealChoices_"+dc.ColumnName, dc.DataType, dc.Expression, dc.ColumnMapping));
            // Add them to the results tabke
            resultsTable.Columns.AddRange(dt2Columns.ToArray());

            // Perform the join and load the results into the resultsTable Data Table
            var rowData =   from row1 in table1.AsEnumerable()
                            join row2 in table2.AsEnumerable()
                            on row1.Field<string>("NHSNumber") equals row2.Field<string>("NHSNumber")
                            select row1.ItemArray.Concat(row2.ItemArray).ToArray();
                            foreach (object[] values in rowData)
                                resultsTable.Rows.Add(values);

            Program.DisplayDataTable(resultsTable);

            Console.WriteLine("done");

            return resultsTable;

        }

        //Join/manipulate data

        //Return data to the user
    }
}
