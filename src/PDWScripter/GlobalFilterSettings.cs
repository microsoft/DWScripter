// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Newtonsoft.Json;

namespace DWScripter
{
    public class GlobalFilterSettings
    {
        public ObjectsToFilter DatabaseObjectsToFilter;

        public GlobalFilterSettings()
        {

            this.DatabaseObjectsToFilter = new ObjectsToFilter();
        }

        public FilterSettings GetObjects(string featurename, string databasename)
        {
            return (FilterSettings)this.DatabaseObjectsToFilter.Find(delegate (FilterSettings e) { return e.FeatureName == featurename && e.Database == databasename; });
        }

        public FilterSettings GetObjectsFromFile(string InputFilePath,string featurename, string databasename)
        {
            using (StreamReader file = File.OpenText(InputFilePath))
            {
                JsonSerializer serializer = new JsonSerializer();
                GlobalFilterSettings gfs = (GlobalFilterSettings)serializer.Deserialize(file, typeof(GlobalFilterSettings));
                this.DatabaseObjectsToFilter = gfs.DatabaseObjectsToFilter;
            }

            return this.GetObjects(featurename, databasename);

        }

        public void GetFilterSettingsFromFile(string InputFilePath)
        {
            using (StreamReader file = File.OpenText(InputFilePath))
            {
                JsonSerializer serializer = new JsonSerializer();
                GlobalFilterSettings gfs = (GlobalFilterSettings)serializer.Deserialize(file, typeof(GlobalFilterSettings));
                this.DatabaseObjectsToFilter = gfs.DatabaseObjectsToFilter;
            }
        }

        public void PersistFilterSettings(string OutputFilePath)
        {
            if (OutputFilePath != "")
            {
                FileStream fs = new FileStream(OutputFilePath, FileMode.Create);
                StreamWriter sw = new StreamWriter(fs);

                sw.Write(JsonConvert.SerializeObject(this));
                sw.Close();
            }
        }

    }


    public class ObjectsToFilter : List<FilterSettings>
    {

    }
}
