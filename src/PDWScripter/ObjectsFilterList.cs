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
    public class FilterSettings
    {
        public string FeatureName;
        public string Database;
        public string Granularity;
       
        public ObjectsFilterList ObjectsToFilter;

        public FilterSettings()
        {
            this.Granularity = "None";
            this.ObjectsToFilter = new ObjectsFilterList();
        }

        public FilterSettings(string FeatureName,string DatabaseName,string Granularity)
        {
            this.FeatureName = FeatureName;
            this.Database = DatabaseName;
            this.Granularity = Granularity;
            this.ObjectsToFilter = new ObjectsFilterList();
        }

        public List<string> GetSchemas()
        {
            return this.ObjectsToFilter.Select(o =>o.schemaname).Distinct().ToList();
        }

        public List<string> GetSchemaNameObjects()
        {
            return this.ObjectsToFilter.Select(o => o.schemaname+"."+o.objectname).ToList();
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

        public void GetFilterSettingsFromFile(string InputFilePath)
        {
            using (StreamReader file = File.OpenText(InputFilePath))
            {
                JsonSerializer serializer = new JsonSerializer();
                FilterSettings fs =  (FilterSettings)serializer.Deserialize(file, typeof(FilterSettings));
                this.Granularity = fs.Granularity;
                this.ObjectsToFilter = fs.ObjectsToFilter;
            }
        }


        public void SaveFilterSettingsToFile(string InputFilePath)
        {
            if (InputFilePath != "")
            {
               FileStream fs = new FileStream(InputFilePath, FileMode.Create);
               StreamWriter sw = new StreamWriter(fs);
               sw.Write(JsonConvert.SerializeObject(this));
               sw.Close();
            }

        }
    }

    public class ObjectsFilterList : List<ObjectFiltered>
    {

    }
    public class ObjectFiltered
    { 
        public string schemaname;
        public string objectname;
        public string objecttype;
        public Boolean todelete;

        public ObjectFiltered(string schemaname, string objectname, string objecttype, Boolean todelete)
        {
            this.schemaname = schemaname;
            this.objectname = objectname;
            this.objecttype = objecttype;
            this.todelete = todelete;
        }

    }

   

}
