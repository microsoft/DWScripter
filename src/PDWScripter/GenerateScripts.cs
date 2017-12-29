// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace DWScripter
{
    class Scripting
    {

        public string GenerateColumnDefinition(List<ColumnDef> cols)
        {
            string distColumn = "";
            string columnClause = "";
            StringBuilder columnSelect = new StringBuilder();
            StringBuilder columnspec = new StringBuilder();

            List<ColumnDef> tempCols = new List<ColumnDef>();

            foreach (ColumnDef c in cols)
            {

                StringBuilder columnDefinition = new StringBuilder();

                if (c.distrbution_ordinal == 1)
                {
                    // Save name of Distribution column
                    distColumn = c.name;
                }
                if (c.column_id > 1)
                {
                    columnspec.Append("\r\n\t,");
                    columnSelect.Append("\r\n\t,");
                }
                else
                {
                    columnspec.Append("\t");
                    columnSelect.Append("\t");
                }

                columnDefinition.Append("[" + c.name + "]" + "\t" + c.type + "\t");
                columnspec.Append("[" + c.name + "]" + "\t" + c.type + "\t");
                columnSelect.Append(c.name);
                if (c.type == "bigint" ||
                    c.type == "bit" ||
                    c.type == "date" ||
                    c.type == "datetime" ||
                    c.type == "int" ||
                    c.type == "smalldatetime" ||
                    c.type == "smallint" ||
                    c.type == "smallmoney" ||
                    c.type == "money" ||
                    c.type == "tinyint" ||
                    c.type == "real")
                {
                    // no size params
                }

                else if (
                    c.type == "binary" ||
                    c.type == "varbinary")
                {
                    // max_length only
                    columnspec.Append("(");
                    columnspec.Append(c.max_length);
                    columnspec.Append(")\t");


                    columnDefinition.Append("(");
                    columnDefinition.Append(c.max_length);
                    columnDefinition.Append(")\t");

                }

                else if (
                    c.type == "char" ||
                    c.type == "varchar")
                {
                    // max_length only
                    columnspec.Append("(");
                    columnspec.Append(c.max_length);
                    columnspec.Append(")\t");
                    columnspec.Append("COLLATE\t");
                    columnspec.Append(c.collation_name);
                    columnspec.Append("\t");


                    columnDefinition.Append("(");
                    columnDefinition.Append(c.max_length);
                    columnDefinition.Append(")\t");
                    columnDefinition.Append("COLLATE\t");
                    columnDefinition.Append(c.collation_name);
                    columnDefinition.Append("\t");

                }

                else if (
                    c.type == "nchar" ||
                    c.type == "nvarchar")
                {
                    // max_length only
                    columnspec.Append("(");
                    columnspec.Append(c.max_length / 2);
                    columnspec.Append(")\t");
                    columnspec.Append("COLLATE\t");
                    columnspec.Append(c.collation_name);
                    columnspec.Append("\t");

                    columnDefinition.Append("(");
                    columnDefinition.Append(c.max_length / 2);
                    columnDefinition.Append(")\t");
                    columnDefinition.Append("COLLATE\t");
                    columnDefinition.Append(c.collation_name);
                    columnDefinition.Append("\t");

                }

                else if (
                    c.type == "float")
                {
                    // precision only
                    columnspec.Append("(");
                    columnspec.Append(c.precision);
                    columnspec.Append(")\t");


                    columnDefinition.Append("(");
                    columnDefinition.Append(c.precision);
                    columnDefinition.Append(")\t");
                }

                else if (
                    c.type == "datetime2" ||
                    c.type == "datetimeoffset" ||
                    c.type == "time")
                {
                    // Scale only
                    columnspec.Append("(");
                    columnspec.Append(c.scale);
                    columnspec.Append(")\t");

                    columnDefinition.Append("(");
                    columnDefinition.Append(c.scale);
                    columnDefinition.Append(")\t");

                }

                else if (
                    c.type == "decimal")
                {
                    // Precision and Scale
                    columnspec.Append("(");
                    columnspec.Append(c.precision);
                    columnspec.Append(",");
                    columnspec.Append(c.scale);
                    columnspec.Append(")\t");

                    columnDefinition.Append("(");
                    columnDefinition.Append(c.precision);
                    columnDefinition.Append(",");
                    columnDefinition.Append(c.scale);
                    columnDefinition.Append(")\t");

                }

                else
                {
                    Exception e = new Exception("Unsupported Type " + c.type);
                    throw e;
                }

                columnspec.Append(c.is_nullable ? "NULL" : "NOT NULL");

                columnDefinition.Append(c.is_nullable ? "NULL" : "NOT NULL");

                columnspec.Append(" " + c.defaultconstraint);
                ColumnDef current = cols[cols.IndexOf(c)];
                current.columnDefinition = columnDefinition.ToString();
                tempCols.Add(current);

            }
            columnClause = columnspec.ToString();
            return columnClause;

        }
    }
}
