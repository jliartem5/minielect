using SQL.App_Code;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using System.Web;
using System.Data.SqlClient;
using MySql.Data.MySqlClient;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;
using System.Configuration;
using OfficeOpenXml;
using NPOI.HSSF.UserModel;
using NPOI.SS.UserModel;
using ME_Batch.App;
using Error;
using System.Xml;

namespace ME_Batch
{
    class Program
    {
        static void Main(string[] args)
        {
            /* String caracteresStr = File.ReadAllText("test.txt");

             caracteresStr = caracteresStr.Replace("MB\"><", "MB\">");
                caracteresStr = caracteresStr.Replace(">< ", "> ");
             //remove all title & data-title
             //(data-title="[^=]*">)
             //(title="[^=]*">)
             //(data-title="[^=]*")
             //(title="[^=]*")
             //(data-title="[^=]*"")
             //(title="[^=]*"")
                //<([0-9]{1,})
                //(data-title="[^>]*")
                //(title="[^>]*")
             //&
                //<= -> =
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

             try
             {

                 //Et ensuite on reconstruit ses caracteristiques
                 using (StringReader stringReader = new StringReader(caracteresStr))
                 {

                     using (XmlTextReader reader = new XmlTextReader(stringReader))
                     {

                         while (reader.Read())
                         {

                         }
                     }
                 }

             }
             catch (Exception except)
             {
                 ManageError.Gestion_Log("Interpret Product error : " + except.Message, null, ManageError.Niveau.Warning);
             }
            */

            if (args.Length < 2)
            {
                return;
            }

            string cmd = args[0];

            string market = args[1];
            switch (cmd)
            {
                case "RefreshKeywords":
                    JArray kw = RefreshKeywordsTree();
                    RefreshProductKeywords(kw, "products_" + market);
                    break;
                case "ImportProducts":
                    ImportProducts(market);
                    break;
                case "CheckPotential":
                    CheckPotential(market);
                    break;
                case "SynchroniseInventory":
                    SynchroniseInventory();
                    break;
            }
        }


        static public void SynchroniseInventory() {
            string synchronySeURL = "http://marketfollowup.minielect.com/inventory-ajax/synchronise-inventory-ebay";

            ManageError.Gestion_Log("Begin synchronyse inventory", null, ManageError.Niveau.Info);
            using (WebClient client = new WebClient()) {

                string result = client.DownloadString(synchronySeURL);

                ManageError.Gestion_Log(result, null, ManageError.Niveau.Info);
            }
            ManageError.Gestion_Log("End synchronyse inventory", null, ManageError.Niveau.Info);

        }

        static public void CheckPotential(string market)
        {

            sqlRequest sql = new sqlRequest();

            DataTable search_composite = sql.RequestSelect("SELECT * FROM potentiel_search_composite", 0);
            WebClient web = new WebClient();
            string searchurl = "https://www.ebay.fr/sch/i.html?LH_BIN=1&_nkw=[WORD]&LH_ItemCondition=3";

            DataTable potentielTableStruct = sql.RequestSelect("SELECT * FROM potential_aliexpress_ebay LIMIT 0", 0);
            DataTable products = sql.StoredProcedureSelect("Potentiel_AliexpressEbay", new List<MySqlParameter>() { 
                new MySqlParameter("cmd", "Get_URL"),
                new MySqlParameter("productID", -1),
                new MySqlParameter("p", -1)
            }, 0);

            //<Search_Composite, List<URL>>
            Dictionary<string, List<string>> all_url_list = new Dictionary<string, List<string>>();

            
            ManageError.Gestion_Log("Begin execute search word...", null, ManageError.Niveau.Info);
            foreach (DataRow prod in products.Rows)
            {
                potentielTableStruct.Rows.Clear();

                Dictionary<string, bool> url_list = new Dictionary<string, bool>();

                DataTable caracteristiques = sql.StoredProcedureSelect("Potentiel_AliexpressEbay", new List<MySqlParameter>() { 
                    new MySqlParameter("cmd", "Get_Caracteristique"),
                    new MySqlParameter("productID", prod["Id"].ToString()),
                    new MySqlParameter("p", -1)
                }, 0);

                foreach (DataRow composite in search_composite.Rows)
                {
                    String cmp = composite["composite"].ToString();
                    string result_seatchword = cmp.Replace("[", "").Replace("]", "");
                    //
                    MatchCollection match = Regex.Matches(cmp, "\\[([^\\]])*\\]");
                    bool all_match = true;
                    foreach (Match cap in match)
                    {
                        string matchColName = cap.Captures[0].Value;
                        matchColName = matchColName.Replace("[", "").Replace("]", "");
                        bool exists = false;
                        foreach (DataRow caract in caracteristiques.Rows)
                        {
                            if (caract["Caractere"].ToString() == matchColName)
                            {
                                result_seatchword = result_seatchword.Replace(matchColName, caract["Valeur"].ToString());
                                exists = true;
                                break;
                            }
                        }
                        if (exists == false)
                        {
                            all_match = false;
                            break;
                        }
                    }

                    //Save result search
                    if (all_url_list.ContainsKey(result_seatchword) == false)
                    {
                        all_url_list[result_seatchword] = new List<string>();
                    }

                    //On a trouver le match 
                    if (all_match)
                    {
                        string resultSeatchURL = searchurl.Replace("[WORD]", HttpUtility.UrlEncode(result_seatchword));
                        try
                        {
                            String webPage = web.DownloadString(resultSeatchURL);

                            //<ul id="ListViewInner">
                            int start = webPage.IndexOf("<ul id=\"ListViewInner\">");
                            if (start == -1)
                            {
                                continue;
                            }

                            //<div id="AnswersPlaceHolderContainer9999">([^=]*</ul>)
                            //get list items
                            int end = webPage.IndexOf("<div id=\"PaginationAndExpansionsContainer\">");
                            webPage = webPage.Substring(start, end - start);
                            webPage = Regex.Replace(webPage, "<div id=\"AnswersPlaceHolderContainer9999\">[^=]*", "</ul>");

                            //<li( )*id="item[a-zA-Z0-9]*"
                            //Search all item begin index
                            MatchCollection itemsMatch = Regex.Matches(webPage, "<li( )*id=\"item[a-zA-Z0-9]*\"");
                            List<string> itemsHtml = new List<string>();
                            int lastIndex = -1;
                            foreach (Match itemM in itemsMatch)
                            {
                                int index = itemM.Index;

                                if (lastIndex != -1)
                                {
                                    itemsHtml.Add(webPage.Substring(lastIndex, index - lastIndex));
                                }

                                lastIndex = index;
                            }

                            //On enregistre le resulta de la recherche dans la table (list)
                            foreach (String item in itemsHtml)
                            {
                                //Prendre 
                                //Prix => text entre <li([^=])*class="lvprice prc"> et prochaine </li>
                                Match prixMatch = Regex.Match(item, "<li([^=])*class=\"lvprice prc\">");
                                int prixMatchEnd = item.IndexOf("</li>", prixMatch.Index);
                                String prix = item.Substring(prixMatch.Index, prixMatchEnd - prixMatch.Index).Trim().Replace("\t", "");
                                prix = RemoveBalise(prix).Replace("\r", "").Replace("\n", "");

                                //Localisation(optionel) => text entre <ul([^=])*class="lvdetails([^"]*)">[^=]*Provenance[^=]: et prochaine </li>
                                Match LocalisationMatch = Regex.Match(item, "<ul([^=])*class=\"lvdetails([^\"]*)\">[^=]*Provenance[^=]:");
                                String Localisation = "";
                                if (LocalisationMatch.Index > 0)
                                {

                                    int LocalisationMatchEnd = item.IndexOf("</li>", LocalisationMatch.Index);
                                    Localisation = item.Substring(LocalisationMatch.Index, LocalisationMatchEnd - LocalisationMatch.Index).Trim().Replace("\t", "");
                                    Localisation = RemoveBalise(Localisation).Replace("\r", "").Replace("\n", "");
                                }

                                //Frais de port => text entre <li([^=])*class="lvshipping"> et prochaine </li>
                                Match LivraisonMatch = Regex.Match(item, "<li([^=])*class=\"lvshipping\">");
                                int LivraisonMatchEnd = item.IndexOf("</li>", LivraisonMatch.Index);
                                String Livraison = item.Substring(LivraisonMatch.Index, LivraisonMatchEnd - LivraisonMatch.Index).Trim().Replace("\t", "");
                                Livraison = RemoveBalise(Livraison).Replace("\r", "").Replace("\n", "");

                                //titre => title='([^']*)'
                                String titre = Regex.Match(item, "title=\"([^\"]*)\"").Value.Trim().Replace("\t", "");
                                //URL => www.ebay.fr/itm/([^\?])*
                                String URL = Regex.Match(item, @"www.ebay.fr/itm/([^\?])*").Value.Trim().Replace("\t", "");
                                //Image => i.ebayimg.com/([^\?])*.jpg
                                String Image = Regex.Match(item, @"i.ebayimg.com/([^\?])*.jpg").Value.Trim().Replace("\t", "");



                                if (url_list.Keys.Contains(URL) == false)
                                {

                                    DataRow row = potentielTableStruct.NewRow();
                                    row["Id_Product_Aliexpress"] = prod["Id"].ToString();
                                    row["Prix_Ebay"] = prix;
                                    row["Localisation_Ebay"] = Localisation;
                                    row["Livraison_Ebay"] = Livraison;
                                    row["Titre_Ebay"] = titre;
                                    row["URL_Ebay"] = URL;
                                    row["Images_Ebay"] = Image;
                                    row["Search_Composite"] = result_seatchword;
                                    potentielTableStruct.Rows.Add(row);
                                    url_list[URL] = true;
                                }

                            }
                        }
                        catch (Exception e)
                        {
                            ManageError.Gestion_Log("Error catch search page for <" + prod["Id"].ToString() + ">:<" + result_seatchword + ">", e, ManageError.Niveau.Warning);
                        }
                    }
                }

                int trycount = 0;
                while (trycount < 10)
                {
                    try
                    {
                        sql.BuldInsert(potentielTableStruct, "potential_aliexpress_ebay");
                        trycount = 99;
                    }
                    catch (Exception e)
                    {
                        trycount++;
                    }

                }
            }

            ManageError.Gestion_Log("End execute search word...", null, ManageError.Niveau.Info);


            //Et ensuite formater le resultat (formater les prix...etc)
            sql.StoredProcedure("Potentiel_AliexpressEbay", new List<MySqlParameter>()
            { 
                new MySqlParameter("@cmd", "Format_Result"),
                new MySqlParameter("@productID", -1),
                new MySqlParameter("@p", -1)
            }, 0);
            


            ManageError.Gestion_Log("Begin fetch Nb_Vendu...", null, ManageError.Niveau.Info);
            //Et ensuite on verifie le nombre de vente pour chaque products et ses potentiels
            foreach (DataRow product in products.Rows)
            {
                string id = product["Id"].ToString();

                DataTable potentials = sql.StoredProcedureSelect("Potentiel_AliexpressEbay", new List<MySqlParameter>()
                    { 
                        new MySqlParameter("@cmd", "Get_AllPotentialToCheck"),
                        new MySqlParameter("@productID", id),
                        new MySqlParameter("@p", -1)
                    }, 0);

                foreach (DataRow potentialRo in potentials.Rows)
                {
                    String ebay_url = potentialRo["URL_Ebay"].ToString();
                    String pageContent = "";
                    string productID = potentialRo["Id_Product_Aliexpress"].ToString();
                    string potentialID = potentialRo["Id"].ToString();
                    int retrycount = 0;
                    while (retrycount < 5)
                    {
                        try
                        {
                            pageContent = web.DownloadString("http://" + ebay_url);
                            retrycount = 99;
                        }
                        catch (Exception e)
                        {
                            retrycount++;
                            if (retrycount == 5)
                            {
                                ManageError.Gestion_Log("Error catch page productId = " + productID + "; Potential ID = " + potentialID, e, ManageError.Niveau.Warning);

                            }
                        }
                    }

                    //Text entre <span([^=])*class="vi-qtyS([^"]*)" et </span>
                    Match match = Regex.Match(pageContent, "<span([^=])*class=\"vi-qtyS([^\"]*)\"");
                    if (match.Index > 0)
                    {
                        int QtyMatchEnd = pageContent.IndexOf("</span>", match.Index);

                        String qty = pageContent.Substring(match.Index, QtyMatchEnd - match.Index).Trim().Replace("\t", "");
                        qty = RemoveBalise(qty).Replace("\r", "").Replace("\n", "");
                        sql.RequestSelect("UPDATE potential_aliexpress_ebay SET Nb_Vendu_Ebay = @nbVendu WHERE URL_Ebay = @url",
                            new List<MySqlParameter>() { 
                            new MySqlParameter("@nbVendu", qty),
                            new MySqlParameter("@url", ebay_url)
                       });
                    }
                }

                //Une fois tous les urls vérifié on met le statut Potentiel_Checked = now()
                sql.RequestSelect("UPDATE products_aliexpress set Potential_CheckDate = now() where Id = @id",
                            new List<MySqlParameter>() { 
                                new MySqlParameter("@id", id)
                       });
            }

            ManageError.Gestion_Log("End fetch Nb_Vendu...", null, ManageError.Niveau.Info);
        }

        static public string RemoveBalise(string str)
        {

            int left = -1;
            int right = -1;
            for (int i = 0; i < str.Length; ++i)
            {
                if (str[i] == '<')
                {
                    left = i;
                }
                if (str[i] == '>')
                {
                    right = i;
                }
                if (left > -1 && right > -1 && right > left)
                {

                    str = str.Remove(left, right - left + 1);
                    str = RemoveBalise(str);
                    break;
                }
            }
            return str;
        }


        static public void ImportProducts(string market)
        {

            sqlRequest sql = new sqlRequest();

            string path = ConfigurationManager.AppSettings["ProductSource_Path"].ToString() + market + @"\";
            string imagePath = ConfigurationManager.AppSettings["ImageTemp_Path"].ToString() + market + @"\";
            string[] columnToIgnore = ConfigurationManager.AppSettings["Ignore_Column"].ToString().Trim().Split(';');

            string table_name = "products_" + market;

            sql.RequestText("TRUNCATE TABLE " + table_name + "_temp");

            //-Lire le fichier Excel
            string[] files = Directory.GetFiles(path);
            foreach (string file in files)
            {
                ManageError.Gestion_Log("Charge file : " + file, null, ManageError.Niveau.Info);

                FileInfo fInfo = new FileInfo(file);
                DataTable tmp_table_structure = sql.RequestSelect("SELECT * FROM " + table_name + "_temp LIMIT 0");

                if (fInfo.Extension == ".xlsx" || fInfo.Extension == ".xls")
                {
                    using (Excel excel = new Excel(fInfo.FullName))
                    {
                        Dictionary<String, bool> urlsTraites = new Dictionary<string, bool>();

                        //Chercher tous les entetes
                        String colName = String.Empty;

                        //Verification des noms de champs
                        int colCount = excel.ColumnCount();
                        List<string> columns = new List<string>();

                        for (int colIndex = 0; colIndex < colCount; ++colIndex)
                        {
                            colName = excel.Cell(colIndex, 0).Trim();
                            if (colName.Length > 0)
                            {
                                bool find = false;
                                //Verifier le champs,//-La table peut contenir des champs dont le fichier ne dispose pas, mais pas l'inverse
                                foreach (DataColumn tableColNameColumn in tmp_table_structure.Columns)
                                {
                                    if (tableColNameColumn.ColumnName.ToString().ToUpper() == colName.ToUpper())
                                    {
                                        find = true;
                                        break;
                                    }
                                }
                                if (find == true)
                                {
                                    columns.Add(colName);
                                }
                                else
                                {
                                    //Rejeter une exception si on trouve une colonne qui n'existe pas dans la structure de la table + ne peut pas ignoré
                                    if (columnToIgnore.Contains(colName) == false)
                                    {
                                        string errorInfo = "Impossible de trouver la colonne <" + colName + "> dans la table " + table_name + "_temp, Modifer la strucuture de cette table.";
                                        ManageError.Gestion_Log(errorInfo, null, ManageError.Niveau.Info);
                                        throw new Exception(errorInfo);
                                    }
                                }
                            }
                        }


                        //-Inserer dans la table temporaire
                        int row_count = excel.RowCount();
                        for (int row_index = 1; row_index < row_count; ++row_index)
                        {
                            DataRow tmp_table_row = tmp_table_structure.NewRow();
                            for (int col_index = 0; col_index < colCount; ++col_index)
                            {
                                string excel_col_name = excel.Cell(col_index, 0);
                                if (columnToIgnore.Contains(excel_col_name))
                                {
                                    continue;
                                }

                                string col_str = columns[col_index];
                                tmp_table_row[col_str] = excel.Cell(col_index, row_index).Trim();
                            }

                            string url = tmp_table_row["URL"].ToString();
                            if (urlsTraites.ContainsKey(url) == false)
                            {
                                tmp_table_structure.Rows.Add(tmp_table_row);
                                urlsTraites[url] = true;
                            }
                        }

                        //Inserer dans la table temporaire
                        sql.BuldInsert(tmp_table_structure, table_name + "_temp");
                        ManageError.Gestion_Log("Insert " + tmp_table_structure.Rows.Count + " into " + table_name + "_temp", null, ManageError.Niveau.Info);


                        //-Calculer les keywods*/
                        JArray kw = RefreshKeywordsTree();
                        RefreshProductKeywords(kw, table_name + "_temp");
                        ManageError.Gestion_Log("Refresh keywords for " + table_name + "_temp", null, ManageError.Niveau.Info);


                        //Traiter la table temporaire pour inserer dans la table principale
                        sql.StoredProcedure(market + "_HandleTempTable", new List<MySqlParameter>(), 0);
                        ManageError.Gestion_Log("Handle tmp table", null, ManageError.Niveau.Info);

                        //-Calculer les caracteres pour les inserer dans la table des caracteres
                        ProductCaractere pc = new ProductCaractere();
                        pc.Interpret(market, table_name + "_temp", sql);
                        ManageError.Gestion_Log("Build caracteres list", null, ManageError.Niveau.Info);

                        //-Telecharger + Upload images

                        DataTable products2download = sql.RequestSelect("SELECT * FROM " + table_name + "_temp", 0);
                        using (WebClient client = new WebClient())
                        {
                            Regex regex = new Regex(@"https?://[^/\s]+/\S+\.(jpg|png|gif)");

                            String login = System.Configuration.ConfigurationManager.AppSettings["sftp_login"].ToString();
                            String password = System.Configuration.ConfigurationManager.AppSettings["sftp_password"].ToString();
                            String adresse = System.Configuration.ConfigurationManager.AppSettings["sftp_adresse"].ToString();

                            int port = System.Convert.ToInt32(System.Configuration.ConfigurationManager.AppSettings["sftp_port"].ToString());
                            string basePath = System.Configuration.ConfigurationManager.AppSettings["sftp_basepath"].ToString();

                            Renci.SshNet.SftpClient oSFTP = new Renci.SshNet.SftpClient(adresse, port, login, password);
                            oSFTP.Connect();

                            foreach (DataRow product_row in products2download.Rows)
                            {
                                //Creer le dossier
                                string directory_path = imagePath + product_row["Id"].ToString() + @"\";
                                if (Directory.Exists(imagePath + product_row["Id"].ToString()) == false)
                                {
                                    Directory.CreateDirectory(directory_path);
                                }
                                string remoteDir = basePath + "img/" + market + "/" + product_row["Id"].ToString() + "/";
                                if (oSFTP.Exists(remoteDir) == false)
                                {
                                    oSFTP.CreateDirectory(remoteDir);
                                }
                                Dictionary<string, string> finished_images = new Dictionary<string, string>();
                                String urls = product_row["images"].ToString();
                                Match match = regex.Match(urls);
                                // int imgCount = 0;

                                while (match.Success)
                                {

                                    /*if (imgCount > 0) {
                                        break;
                                    }*/

                                    String imgUrl = match.Value;


                                    if (finished_images.Keys.Contains(imgUrl) == false && imgUrl.Contains("50x50.jpg") == false)
                                    {
                                        Uri uri = new Uri(imgUrl);
                                        string filename = (finished_images.Count + 1) + Path.GetExtension(uri.LocalPath);
                                        try
                                        {
                                            /*client.DownloadFile(new Uri(imgUrl), directory_path + filename);
                                            if (File.Exists(directory_path + filename))
                                            {
                                                FileStream fs = new FileStream(directory_path + filename, FileMode.Open);
                                                oSFTP.UploadFile(fs, remoteDir + filename, true);
                                                fs.Close();
                                            */
                                            finished_images[imgUrl] = "";//(remoteDir + filename);
                                            /*    File.Delete(directory_path + filename);
                                                imgCount++;
                                            }*/
                                        }
                                        catch (Exception e)
                                        {
                                            ManageError.Gestion_Log("Cannot download image : " + e.Message, null, ManageError.Niveau.Info);
                                        }
                                    }


                                    match = match.NextMatch();
                                }

                                JObject imgObjet = new JObject();
                                foreach (string imageurl in finished_images.Keys)
                                {
                                    imgObjet[imageurl] = finished_images[imageurl];
                                }
                                sql.RequestSelect("UPDATE " + table_name + " SET images = @img_json Where Id = @id", new List<MySqlParameter>() { 
                                        new MySqlParameter("@img_json", WebUtility.HtmlEncode(imgObjet.ToString())),
                                        new MySqlParameter("@id", product_row["Id"].ToString())
                                });

                                Directory.Delete(directory_path);
                            }

                            oSFTP.Disconnect();
                        }
                    }
                }

                fInfo.Delete();
                if (File.Exists(file))
                {
                    File.Delete(file);
                }
            }
        }

        static public void RefreshProductKeywords(JArray keywords, string destTableName)
        {

            sqlRequest req = new sqlRequest();
            DataTable products = req.RequestSelect("SELECT distinct Titre, URL FROM " + destTableName);
            foreach (DataRow prod in products.Rows)
            {
                string title = HttpUtility.HtmlDecode(prod["Titre"].ToString());
                List<string> kw = FindKeywords(title, keywords);
                string resultKW = String.Join(",", kw.ToArray());
                req.RequestSelect("UPDATE " + destTableName + " SET keywords = @kw where URL = @url", new List<MySqlParameter>() { 
                   new MySqlParameter("@kw", resultKW),
                   new MySqlParameter("@url", prod["URL"].ToString())
                });
            }
        }

        static public List<string> FindKeywords(string text, JArray keywords)
        {
            List<string> resultkws = new List<string>();

            for (int i = 0; i < keywords.Count; ++i)
            {
                JObject keyword = keywords[i].Value<JObject>();

                bool containkeyword = (text.Trim().ToUpper().IndexOf(" " + keyword["keyword"].ToString().ToUpper().Trim() + " ") >= 0)
                    || (text.Trim().ToUpper().IndexOf(keyword["keyword"].ToString().ToUpper().Trim() + " ") >= 0)
                    || (text.Trim().ToUpper().IndexOf(" " + keyword["keyword"].ToString().ToUpper().Trim()) >= 0);

                if (containkeyword)
                {
                    if (keyword["childs"] != null)
                    {
                        List<string> subKeywords = FindKeywords(text, keyword["childs"].Value<JArray>());
                        if (subKeywords.Count == 0)
                        {
                            resultkws.Add(keyword["keyword"].ToString());
                        }
                        else
                        {
                            resultkws.AddRange(subKeywords);
                        }
                    }
                    else
                    {
                        resultkws.Add(keyword["keyword"].ToString());
                    }
                }
            }
            return resultkws;
        }


        static public JArray RefreshKeywordsTree()
        {
            sqlRequest req = new sqlRequest();
            DataTable keywords = req.RequestSelect("SELECT UPPER(keyword) as keyword from analyse_keywords order by keyword");

            JArray keywordTree = new JArray();

            for (int i = 0; i < keywords.Rows.Count; )
            {
                List<DataRow> kwRow = new List<DataRow>();

                string rootWord = keywords.Rows[i]["keyword"].ToString().Trim();
                do
                {
                    kwRow.Add(keywords.Rows[i]);
                    ++i;
                } while (i < keywords.Rows.Count && keywords.Rows[i]["keyword"].ToString().Trim().StartsWith(rootWord));

                JObject branche = new JObject();
                BuildBranche(kwRow, branche);
                keywordTree.Add(branche);
            }

            return keywordTree;
        }

        static public int BuildBranche(List<DataRow> kwRow, JObject branche, int start = 0)
        {

            string currentRoot = kwRow[start]["keyword"].ToString().Trim();
            branche["keyword"] = currentRoot;
            JArray childs = new JArray();

            for (start = start + 1; start < kwRow.Count; ++start)
            {
                currentRoot = kwRow[start]["keyword"].ToString().Trim();

                if (currentRoot.StartsWith(branche["keyword"].ToString().Trim()))
                {
                    JObject subBranche = new JObject();
                    start = BuildBranche(kwRow, subBranche, start);
                    childs.Add(subBranche);
                }
                else
                {
                    --start;
                    break;
                }
            }
            if (childs.Count > 0)
            {
                branche["childs"] = childs;
            }

            return start;
        }



    }
}
