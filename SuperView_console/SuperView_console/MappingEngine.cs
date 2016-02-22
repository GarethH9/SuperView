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
        
        // Generate a blank XML
 


        //Get fields from data source

        //Create relationship

        //Get data source from wrapper

        //Transmit query to wrappers

        //Receieve data from wrappers
    }
}
