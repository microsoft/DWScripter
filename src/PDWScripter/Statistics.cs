// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DWScripter
{
    public class Statistics : List<StatDef>
    {

        public virtual int CompareTo(object obj)
        {
            if (obj == null) return 1;
            Statistics otherStatistics = obj as Statistics;
            if (otherStatistics != null)
            {
                if (this == null || otherStatistics == null) return 1;
                this.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                otherStatistics.Sort((a, b) => a.name.ToUpper().CompareTo(b.name));
                if (this.Count != otherStatistics.Count) return 1;

                for (int i = 0; i < this.Count; i++)
                {
                    if (this[i].CompareTo(otherStatistics[i]) == 1) return 1;
                }
            }
            return 0;
        }

        public StatDef GetStat(string StatName)
        {
            return this.Find(delegate (StatDef e) { return e.name == StatName; });
        }

        public Statistics GetStatsWithColumn(string ColumnName)
        {
            Statistics StatsWithColumn = new Statistics();
            foreach (StatDef stat in this)
            {
                if (stat.ContainsColumn(ColumnName))
                    StatsWithColumn.Add(stat);
            }
            return StatsWithColumn;
        }

    }
}
