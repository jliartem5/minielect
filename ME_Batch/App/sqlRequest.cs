using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using Error;

namespace SQL.App_Code
{
    public class sqlRequest
    {

        private MySqlConnection globalConnect;
        private String connectionString;

        /// <summary>
        /// Construct prepare the connectionstring use
        /// </summary>
        public sqlRequest()
        {

            connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["BDD"].ConnectionString;
        }

        private MySqlConnection getConnection()
        {
            if (globalConnect == null) {
                globalConnect = new MySqlConnection();
                globalConnect.ConnectionString = connectionString;
            }
            return globalConnect;
        }

        /// <summary>
        /// Launch stored procedure and return the result
        /// </summary>
        /// <param name="procedureName">take a stored procedure name</param>
        /// <param name="sqlParam">list of sqlparameter object</param>
        /// <param name="Timeout">duration(secondes) of the stop before stored procedure</param>
        /// <returns>return the line in the datatable</returns>
        public DataTable StoredProcedureSelect(string procedureName, int Timeout, MySqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();

            using (MySqlConnection connection = this.getConnection())
            {
                if (connection.ConnectionString == "" && connection.State != ConnectionState.Open)
                {
                    connection.ConnectionString = connectionString;
                }
                MySqlCommand sqlCmd = new MySqlCommand(procedureName, connection);
                sqlCmd.Connection.Open();
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.CommandTimeout = Timeout;
                if (transaction != null)
                {
                    sqlCmd.Transaction = transaction;
                }

                MySqlDataAdapter sda = new MySqlDataAdapter();
                sda.SelectCommand = sqlCmd;
                sda.Fill(dt);
                sda.Dispose();
                sqlCmd.Connection.Close();
            }

            return dt;
        }

        public DataSet StoredProcesureSelectDataSet(string procedureName, List<MySqlParameter> sqlParam, int Timeout = 0, MySqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            DataSet ds = new DataSet();
            using (MySqlConnection connection = this.getConnection())
            {
                if (connection.ConnectionString == "" && connection.State != ConnectionState.Open)
                {
                    connection.ConnectionString = connectionString;
                }
                MySqlCommand sqlCmd = new MySqlCommand(procedureName, connection);
                sqlCmd.Connection.Open();
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.CommandTimeout = Timeout;
                if (transaction != null)
                {
                    sqlCmd.Transaction = transaction;
                }
                MySqlParameterCollection sqlParameters = (MySqlParameterCollection)sqlCmd.Parameters;

                foreach (MySqlParameter item in sqlParam)
                {
                    sqlParameters.Add(item);
                }

                MySqlDataAdapter sda = new MySqlDataAdapter();
                sda.SelectCommand = sqlCmd;
                sda.Fill(ds);
                sda.Dispose();
                sqlCmd.Connection.Close();
            }

            return ds;
        }


        /// <summary>
        /// Launch stored procedure and return the result
        /// </summary>
        /// <param name="procedureName">take a stored procedure name</param>
        /// <param name="sqlParam">list of sqlparameter object</param>
        /// <param name="Timeout">duration(secondes) of the stop before stored procedure</param>
        /// <returns>return the line in the datatable</returns>
        public DataTable StoredProcedureSelect(string procedureName, List<MySqlParameter> sqlParam, int Timeout, MySqlTransaction transaction = null)
        {                
            DataTable dt = new DataTable();
            DataSet ds = this.StoredProcesureSelectDataSet(procedureName, sqlParam, Timeout, transaction);
            return ds.Tables[0];
        }

        public DataSet StoredProcedureSelectSet(string procedureName, List<MySqlParameter> sqlParam, int Timeout, MySqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();
            DataSet ds = this.StoredProcesureSelectDataSet(procedureName, sqlParam, Timeout, transaction);
            return ds;
        }

        /// <summary>
        /// Launch request text and return the result 
        /// </summary>
        /// <param name="request">Write a request</param>
        /// <param name="Timeout">duration(secondes) of the stop before request</param>
        /// <returns>return the line in the datatable</returns>
        public DataTable RequestSelect(string request, int Timeout, MySqlTransaction transaction = null)
        {
            return this.RequestSelect(request, null, 0, transaction);
        }

        public DataTable RequestSelect(string request, List<MySqlParameter> pms = null, int Timeout = 0, MySqlTransaction transaction = null)
        {
            DataTable dt = new DataTable();


            using (MySqlConnection connection = this.getConnection())
            {
                if (connection.ConnectionString == "" && connection.State != ConnectionState.Open)
                {
                    connection.ConnectionString = connectionString;
                }
                MySqlCommand sqlCmd = new MySqlCommand(request, connection);
                sqlCmd.Connection.Open();
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.CommandTimeout = Timeout;
                if (transaction != null)
                {
                    sqlCmd.Transaction = transaction;
                }

                //Ajouter des parametres
                if (pms != null)
                {
                    MySqlParameterCollection sqlParameters = (MySqlParameterCollection)sqlCmd.Parameters;

                    foreach (MySqlParameter item in pms)
                    {
                        sqlParameters.Add(item);
                    }
                }

                MySqlDataAdapter sda = new MySqlDataAdapter();
                sda.SelectCommand = sqlCmd;



                sda.Fill(dt);
                sda.Dispose();
                sqlCmd.Connection.Close();
            }

            return dt;
        }

        /// <summary>
        /// Launch request  
        /// </summary>
        /// <param name="request">Write a request only delete, insert, update</param>
        /// <param name="Timeout">duration(secondes) of the stop before request</param>
        /// <returns>return the line affected </returns>
        public int RequestText(string request, int Timeout = 0, MySqlTransaction transaction = null)
        {
            int returnline;

            using (MySqlConnection connection = this.getConnection())
            {
                if (connection.ConnectionString == "" && connection.State != ConnectionState.Open)
                {
                    connection.ConnectionString = connectionString;
                }
                MySqlCommand sqlCmd = new MySqlCommand(request, connection);
                sqlCmd.Connection.Open();
                sqlCmd.CommandType = CommandType.Text;
                sqlCmd.CommandTimeout = Timeout;
                if (transaction != null) {
                    sqlCmd.Transaction = transaction;
                }

                returnline=sqlCmd.ExecuteNonQuery();
                sqlCmd.Connection.Close();
            }

            return returnline;
        }

        /// <summary>
        /// Launch request  
        /// </summary>
        /// <param name="request">Write a request only delete, insert, update</param>
        /// <param name="Timeout">duration(secondes) of the stop before request</param>
        /// <returns>return the line affected </returns>
        public int StoredProcedure(string procedureName, List<MySqlParameter> sqlParam, int Timeout, MySqlTransaction transaction = null)
        {
            int returnline;

            using (MySqlConnection connection = this.getConnection())
            {
                if (connection.ConnectionString == "" && connection.State != ConnectionState.Open)
                {
                    connection.ConnectionString = connectionString;
                }
                MySqlCommand sqlCmd = new MySqlCommand(procedureName, connection);
                sqlCmd.Connection.Open();
                sqlCmd.CommandType = CommandType.StoredProcedure;
                sqlCmd.CommandTimeout = Timeout;
                if (transaction != null)
                {
                    sqlCmd.Transaction = transaction;
                }
                MySqlParameterCollection sqlParameters = (MySqlParameterCollection)sqlCmd.Parameters;

                foreach (MySqlParameter item in sqlParam)
                {
                    sqlParameters.Add(item);
                } 
                returnline = sqlCmd.ExecuteNonQuery();
                sqlCmd.Connection.Close();
            }

            return returnline;
        }

        public void BuldInsert(DataTable source, string dsetTable) {
            
            using (MySqlConnection conn = this.getConnection())
            {
                if (conn.ConnectionString == "" && conn.State != ConnectionState.Open)
                {
                    conn.ConnectionString = connectionString;
                }
                conn.Open();
                using (MySqlTransaction tran = conn.BeginTransaction(System.Data.IsolationLevel.Serializable))
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = tran;
                        cmd.CommandTimeout = 0;
                        StringBuilder sql = new StringBuilder("INSERT INTO " + dsetTable + "(");

                        StringBuilder valuesSql = new StringBuilder();

                        for(int i=0;i<source.Columns.Count;++i){
                            DataColumn col = source.Columns[i];
                            
                            sql.Append(col.ColumnName);
                            valuesSql.Append("@" + col.ColumnName);
                            if(i < source.Columns.Count-1){
                                sql.Append(",");
                                valuesSql.Append(",");
                            }
                        } 

                        sql.Append(") VALUES(");
                        valuesSql.Append(")");
                        cmd.CommandText = sql.ToString() + valuesSql.ToString();

                        foreach (DataRow row in source.Rows) {
                            List<MySqlParameter> parameters = new List<MySqlParameter>();
                            foreach (DataColumn col in source.Columns) {
                                parameters.Add(new MySqlParameter("@" + col.ColumnName, row[col]));
                            }
                            try {
                                cmd.Parameters.Clear();
                                cmd.Parameters.AddRange(parameters.ToArray());
                                cmd.ExecuteNonQuery();
                            }
                            catch (Exception except) {
                                String errorMsg = " Parameters =>\n";
                                foreach (MySqlParameter param in parameters) {
                                    errorMsg += "<" + param.ParameterName + "> : <" + param.Value + ">\n";
                                }
                                ManageError.Gestion_Log("Error insertion '"+dsetTable+"' : " + except.Message+errorMsg, null, ManageError.Niveau.Erreur);
                            }
                        }
                        tran.Commit();


                        /*
                        using (MySqlDataAdapter adapter = new MySqlDataAdapter(cmd))
                        {
                            adapter.UpdateBatchSize = 1000;
                            using (MySqlCommandBuilder cb = new MySqlCommandBuilder(adapter))
                            {
                                string ignore_error = System.Configuration.ConfigurationManager.AppSettings["Ignore_BulkInsertError"].ToString();
                                if (ignore_error == "1")
                                {
                                    adapter.ContinueUpdateOnError = true;
                                }

                                adapter.FillError += FillError;
                                adapter.Update(source);
                                tran.Commit();
                            }
                        }*/
                    }
                }
            }

        }

        protected static void FillError(object sender, FillErrorEventArgs args)
        {
            if (args.Errors.GetType() == typeof(System.OverflowException))
            {
                //Code to handle Precision Loss
                Console.WriteLine("Error fill:" + args.Errors.Message);
                Console.ReadKey();
                args.Continue = true;
            }
        }

        public MySqlTransaction BeginTransaction()
        {
            using (MySqlConnection connection = this.getConnection())
            {
                if (connection.ConnectionString == "" && connection.State != ConnectionState.Open)
                {
                    connection.ConnectionString = connectionString;
                }

                connection.Open();
                return connection.BeginTransaction();
            }
        }

    }
}
