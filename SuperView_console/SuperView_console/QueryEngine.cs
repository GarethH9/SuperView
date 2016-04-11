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
        public static Dictionary<Wrapper, DataTable> query(string query)
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
            Dictionary<Wrapper, DataTable> results = new Dictionary<Wrapper, DataTable>();

            // We must now loop through all of our data sources and query them for their data
            foreach (KeyValuePair<string, Wrapper> dataSource in Program.dataSources)
            {
                // Store the wrapper so it's easier to reference
                Wrapper wrapper = dataSource.Value;

                Dictionary<string, Dictionary<string, string>> where = null;

                // Check if our mapping table contains this data source's ID
                bool contains = mapping.AsEnumerable().Any(row => dataSource.Value.getID() == row.Field<int>("SourceDataSource"));

                Console.WriteLine(contains);
                
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

                results[Program.dataSources[dataSource.Key]] = wrapper.queryWithWhere(columns, tables, where);
            }

            return results;
        }

        public static DataTable joinData(Dictionary<Wrapper, DataTable> results)
        {
            // Get all of the relations needed to join the data we have
            //DataTable relations = MappingEngine.getRelationsForTable(

            // Empty data table to hold our results
            DataTable resultsTable = new DataTable();

            // Loop
            foreach (KeyValuePair<Wrapper, DataTable> data in results)
            {
                // Get the relations for this table
                DataTable relations = MappingEngine.getRelationsForDataSource(data.Key.getID());

                // Variable to make our table easier to access
                DataTable table = data.Value;

                resultsTable = joinDataTables(resultsTable, table, "NHSNumber", "NHSNumber");

            }

            Program.DisplayDataTable(resultsTable);

            return new DataTable();
        }

        // Function which performs a left outer join on two data tables
        public static DataTable joinDataTables(DataTable table1, DataTable table2, string t1column, string t2column)
        {
            // Change the column name of the t2 column we're joining in to a temporary name
            // This prevents an error from occuring if the two column names are the same, this shouldn't be an issue because of how we name our columns
            string tempColumnName = t2column + "_join";
            if (table2.Columns.Contains(t2column))
                table2.Columns[t2column].ColumnName = tempColumnName;

            // Create a results table and clone the columns from table 1
            DataTable result = table1.Clone();

            // Get the columns from table 2
            var t2columns = table2.Columns.OfType<DataColumn>().Select(dc => new DataColumn(dc.ColumnName, dc.DataType, dc.Expression, dc.ColumnMapping));

            // Add the table 2's columns to the results table
            result.Columns.AddRange(t2columns.ToArray());
           
            // If the join column doesn't exist in either table then we return a blank table
            if (!table1.Columns.Contains(t1column) && !table2.Columns.Contains(tempColumnName))
            {
                // Return a blank table (column names only)
                result.Columns.Remove(tempColumnName);
                return result;
            }
            else if (!table1.Columns.Contains(t1column))
            {
                // Return table 2
                table2.Columns[tempColumnName].ColumnName = t2column;
                return table2;
            }
            else if (!table2.Columns.Contains(tempColumnName))
            {
                // Return table 2
                return table1;
            }


            // If the column doesn't exist in one of the tables we return the other table
            // Make sure the join columns exist in both tables -- if not then we can't perform a join
            if (!table1.Columns.Contains(t1column) || (!table2.Columns.Contains(t2column) && !table2.Columns.Contains(tempColumnName)))
            {
                //if (!result.Columns.Contains(t1column))
                //    result.Columns.Add(t2column);
                //return result;
            }

            // Perform the join
            var rowData = from t1 in table1.AsEnumerable()
                          join t2 in table2.AsEnumerable() on t1[t1column] equals t2[tempColumnName] into tempRow
                                    from t3 in tempRow.DefaultIfEmpty()
                                    select t1.ItemArray.Concat((t3 == null) ? (table2.NewRow().ItemArray) : t3.ItemArray).ToArray();

            // Add the row data to our result data table
            foreach (object[] values in rowData)
                result.Rows.Add(values);

            //Change column name back to original
            table2.Columns[tempColumnName].ColumnName = t2column;

            // Remove extra column from result
            result.Columns.Remove(tempColumnName);

            return result;
        }

        /*public static DataTable joinData(Dictionary<Wrapper, DataTable> results)
        {
            // Get all of the relations needed to join the data we have
            //DataTable relations = MappingEngine.getRelationsForTable(
            
            // Empty data table to hold our results
            DataTable resultsTable = new DataTable();

            // Loop through each of the wrappers and join their data to the main data set
            foreach (KeyValuePair<Wrapper, DataTable> data in results)
            {
                // Get the relations for this table
                DataTable relations = MappingEngine.getRelationsForDataSource(data.Key.getID());

                //Program.DisplayDataTable(relations);

                //Program.DisplayDataTable(data.Value);
                
                // Variable to make our table easier to access
                DataTable table = data.Value;

                IEnumerable<object> columnNames = (from dc in resultsTable.Columns.Cast<DataColumn>()
                                        select dc.ColumnName).ToArray();

                // Get the columns from the table
                var columns = table.Columns.OfType<DataColumn>().Select(dc => new DataColumn(data.Key.getName() + "_" + dc.ColumnName, dc.DataType, dc.Expression, dc.ColumnMapping));
                // Add the columns to the results table
                resultsTable.Columns.AddRange(columns.ToArray());

                // Join the current table to what we have so far
                var rowData = from row1 in table.AsEnumerable()
                              join row2 in resultsTable.AsEnumerable()
                              on row1.Field<string>("NHSNumber") equals row2.Field<string>(data.Key.getName() + "_" + "NHSNumber") into row2new
                              from row3 in row2new.DefaultIfEmpty()
                              select ConcatWithNull2(columnNames, ConcatWithNull(row1, row3)).ToArray();
                foreach (object[] values in rowData)
                {
                    resultsTable.Rows.Add(values);
                }

                Program.DisplayDataTable(resultsTable);

                Console.WriteLine(data.Key.ToString());
            }

            // Loop through each of the results tables we have and join them to the previous results
            /*foreach (KeyValuePair<string, DataTable> data in results)
            {
                // Variable to make our table easier to access
                DataTable table = data.Value;

                // Join the current table to what we have so far
                var rowData = from row1 in table.AsEnumerable()
                              join row2 in resultsTable.AsEnumerable()
                              on row1.Field<string>("NHSNumber") equals row2.Field<string>(data.Key + "_" + "NHSNumber") into row2new
                              from row3 in row2new.DefaultIfEmpty()
                              select ConcatWithNull(row1, row3).ToArray();
                foreach (object[] values in rowData)
                    resultsTable.Rows.Add(values);

                Console.WriteLine(data.Key.ToString());
            }*/

            /*DataTable table1 = results["SuperViewTest"];
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
            var rowData = from DataRow row1 in table1.AsEnumerable()
                          join row2 in table2.AsEnumerable()
                          on row1.Field<string>("NHSNumber") equals row2.Field<string>("NHSNumber") into gj
                          from subpet in gj.DefaultIfEmpty()
                          select ConcatWithNull(row1, subpet).ToArray();
                          //select row1.ItemArray.Concat(subpet == null ? null : subpet.ItemArray).ToArray();
            foreach (object[] values in rowData)
                resultsTable.Rows.Add(values);

            Program.DisplayDataTable(resultsTable);

            Console.WriteLine("done");

            return resultsTable;

        }*/

        public static IEnumerable<object> ConcatWithNull(DataRow row1, DataRow row2)
        {
            if (row1 == null)
            {
                return row2.ItemArray;
            }
            else if (row2 == null)
            {
                return row1.ItemArray;
            }
            else
            {
                return row1.ItemArray.Concat(row2 == null ? null : row2.ItemArray);
            }
        }

        public static IEnumerable<object> ConcatWithNull2(IEnumerable<object> items1, IEnumerable<object> items2)
        {
            if (items1 == null)
            {
                return items1;
            }
            else if (items2 == null)
            {
                return items1;
            }
            else
            {
                return items1.Concat(items2 == null ? null : items2);
            }
        }
        //Join/manipulate data

        //Return data to the user
    }
}
