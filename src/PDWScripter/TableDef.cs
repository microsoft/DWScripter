// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public struct TableDef
    {
        public string name;
        public string schema;
        public byte distribution_policy;



        public TableDef(string name, string schema, byte distribution_policy)
        {
            this.name = name;
            this.schema = schema;
            this.distribution_policy = distribution_policy;
        }


        
    }
}
