// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using  Newtonsoft.Json;
using System.Data;

namespace DWScripter
{
   public class Helper
    {
        public static string ConvertToJson(TableSt obj)
        {
            return JsonConvert.SerializeObject(obj, Formatting.Indented);
        }
    }
}
