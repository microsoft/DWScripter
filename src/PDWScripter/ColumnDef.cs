// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public struct ColumnDef
    {
        public Int32 column_id;
        public string name;
        public string type;
        public Int16 max_length;
        public byte precision;
        public byte scale;
        public bool is_nullable;
        public byte distrbution_ordinal;
        public string collation_name;
        public string defaultconstraint;
        public string columnDefinition;

        public ColumnDef(Int32 column_id, string name, string type, Int16 max_length, byte precision, byte scale, bool is_nullable, byte distribution_ordinal, string defaultconstraint, string collation_name)
        {
            this.column_id = column_id;
            this.name = name;
            this.type = type;
            this.max_length = max_length;
            this.precision = precision;
            this.scale = scale;
            this.is_nullable = is_nullable;
            this.distrbution_ordinal = distribution_ordinal;
            this.collation_name = collation_name;
            this.defaultconstraint = defaultconstraint;
            this.columnDefinition = string.Empty;
        }



    }
}
