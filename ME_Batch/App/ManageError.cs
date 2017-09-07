using System;
using System.Collections.Generic;
using System.Text;
using System.Configuration;
using System.IO;
using log4net.Appender;
using log4net.Repository;
using log4net;
namespace Error
{
    public class ManageError
    {
        public static bool isInitiated = false;
        public enum Niveau
        {
            Info = 1,
            Warning = 2,
            Erreur = 3
        }

        public static void InitialiserLog()
        {

            log4net.Config.XmlConfigurator.Configure();
            log4net.ILog logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        }

        //niveau 1=info,2=warning,3=erreur
        public static void Gestion_Log(string msg, Exception ex, Niveau niveau)
        {
            InitialiserLog();

            log4net.ILog logger = log4net.LogManager.GetLogger("LogFileAppender");


                switch (niveau)
                {
                    case Niveau.Info:
                        logger.Info(msg, ex);
                        break;
                    case Niveau.Warning:
                        logger.Warn(msg, ex);
                        break;
                    case Niveau.Erreur:
                        logger.Error(msg, ex);
                        break;
                }


            log4net.LogManager.ResetConfiguration();
        }

        public static IAppender FindAppenderByName(string name)
        {
            ILoggerRepository rootRep = LogManager.GetRepository();
            foreach (IAppender iApp in rootRep.GetAppenders())
            {
                if (string.Compare(name, iApp.Name, true) == 0)
                {
                    return iApp;
                }
            }
            return null;
        }
    }
}
