using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml;
using SQL.App_Code;
using System.Data;
using MySql.Data.MySqlClient;
using Error;
using System.Text.RegularExpressions;

namespace ME_Batch
{
    class ProductCaractere
    {
        public void Interpret(string market, string sourceTable, sqlRequest req)
        {
            switch (market.ToLower()) { 
                case "aliexpress":
                    Interpret_Aliexpress(sourceTable, req);
                    break;

                case "ebay":
                    Interpret_Ebay(sourceTable, req);
                    break;
            }
        }

        private void Interpret_Aliexpress(string tableName, sqlRequest req)
        {

            DataTable caractersRows = req.RequestSelect("SELECT * FROM " + tableName);

            foreach (DataRow row in caractersRows.Rows)
            {
                DataTable structure = req.RequestSelect("SELECT * FROM caracteristique_ebay LIMIT 0");
                String caracteresStr = row["Detail_Technique_raw"].ToString();
                caracteresStr = caracteresStr.Replace("MB\"><", "MB\">");
                caracteresStr = caracteresStr.Replace(">< ", "> ");
                //remove all title & data-title
                caracteresStr = Regex.Replace(caracteresStr, "(data-title=\"[^=]*\">)", ">");
                caracteresStr = Regex.Replace(caracteresStr, "(title=\"[^=]*\">)", ">");
                caracteresStr = Regex.Replace(caracteresStr, "(data-title=\"[^=]*\"\")", "");
                caracteresStr = Regex.Replace(caracteresStr, "(title=\"[^=]*\"\")", "");
                caracteresStr = Regex.Replace(caracteresStr, "(data-title=\"[^=]*\")", "");
                caracteresStr = Regex.Replace(caracteresStr, "(title=\"[^=]*\")", "");
                caracteresStr = Regex.Replace(caracteresStr, "<([0-9]{1,})", "");

                caracteresStr = Regex.Replace(caracteresStr, "(data-title=\"[^>]*\")", "");
                caracteresStr = Regex.Replace(caracteresStr, "(title=\"[^>]*\")", "");

                caracteresStr = Regex.Replace(caracteresStr, "&", "");
                caracteresStr = Regex.Replace(caracteresStr, "<=", "=");

                string ProductID = row["Id"].ToString();

                //D'abord on vide les anciennes traces
                req.RequestSelect("DELETE FROM caracteristique_ebay WHERE Id_Product = @id_product", new List<MySql.Data.MySqlClient.MySqlParameter>() { 
                    new MySqlParameter("@id_product", ProductID)
                });

                try
                {

                    //Et ensuite on reconstruit ses caracteristiques
                    using (StringReader stringReader = new StringReader(caracteresStr))
                    {

                        using (XmlTextReader reader = new XmlTextReader(stringReader))
                        {

                            String title = string.Empty;
                            String value = string.Empty;
                            while (reader.Read())
                            {
                                string classStr = reader.GetAttribute("class");

                                if (classStr != null && classStr.Contains("propery-title"))
                                {
                                    title = reader.ReadString();
                                }
                                if (classStr != null && classStr.Contains("propery-des"))
                                {
                                    value = reader.ReadString();
                                }
                                if (title != string.Empty && value != string.Empty)
                                {
                                    DataRow cRow = structure.NewRow();
                                    cRow["Id_Product"] = ProductID;
                                    cRow["Caractere"] = title;

                                    if (value.Length > 200)
                                    {
                                        value = value.Substring(0, 199);
                                    }

                                    cRow["Valeur"] = value;
                                    structure.Rows.Add(cRow);

                                    title = value = string.Empty;
                                }
                            }
                        }
                    }
                    if (structure.Rows.Count > 0)
                    {
                        req.BuldInsert(structure, "caracteristique_aliexpress");
                    }
                    else {
                        ManageError.Gestion_Log("No caracteres for product ID " + ProductID, null, ManageError.Niveau.Info);
                    }
                }
                catch (Exception except) {
                    ManageError.Gestion_Log("Interpret Product " + ProductID + " error : " + except.Message, null, ManageError.Niveau.Warning);
                }

            }

        }

        private void Interpret_Ebay(string tableName, sqlRequest req)
        {
            DataTable caractersRows = req.RequestSelect("SELECT * FROM " + tableName);

            foreach (DataRow row in caractersRows.Rows)
            {
                String caracteresStr = row["Detail_Technique_raw"].ToString();
                string ProductID = row["Id"].ToString();
                DataTable structure = req.RequestSelect("SELECT * FROM caracteristique_ebay LIMIT 0");

                //D'abord on vide les anciennes traces
                req.RequestSelect("DELETE FROM caracteristique_ebay WHERE Id_Product = @id_product", new List<MySql.Data.MySqlClient.MySqlParameter>() { 
                    new MySqlParameter("@id_product", ProductID)
                });

                //Et ensuite on reconstruit ses caracteristiques
                using (StringReader stringReader = new StringReader(caracteresStr))
                {
                    using (XmlTextReader reader = new XmlTextReader(stringReader))
                    {
                        String title = string.Empty;
                        String value = string.Empty;
                        try
                        {
                            while (reader.Read())
                            {
                                string classStr = reader.GetAttribute("class");

                                if (classStr != null && classStr.Contains("attrLabels"))//searche label
                                {
                                    title = reader.ReadString().Trim();

                                    while (reader.Read())
                                    { //now search value
                                        string widthAttribute = reader.GetAttribute("width");
                                        if (widthAttribute != null && widthAttribute == "50.0%")
                                        {
                                            reader.ReadToDescendant("span");
                                            value = reader.ReadString().Trim();
                                            break;
                                        }
                                    }

                                    if (title != string.Empty && value != string.Empty)
                                    {
                                        DataRow cRow = structure.NewRow();
                                        cRow["Id_Product"] = ProductID;
                                        cRow["Caractere"] = title.Replace(":", "");

                                        if (value.Length > 200) {
                                            value = value.Substring(0, 199);
                                        }

                                        cRow["Valeur"] = value;

                                        structure.Rows.Add(cRow);

                                        title = value = string.Empty;
                                    }
                                }
                            }
                        }
                        catch (Exception e) { 
                            //
                        }
                    }
                }
                try
                {
                    if (structure.Rows.Count > 0)
                    {
                        req.BuldInsert(structure, "caracteristique_ebay");
                    }
                }catch(Exception exp){
                    //Insert error, handle this
                    Console.WriteLine(exp.Message);
                }
            }

        }


    }
}
