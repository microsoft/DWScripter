# DWScripter Usage

The DWScripter Utility lets you script Parallel Data Warehouse or SQL Data Warehouse objects from the command prompt. DWScripter will allow you to produce .dsql files, or JSON structures to be used for comparison against a database.
The utility also allows to compare two databases directly, producing a delta .dsql file to be used to update the target database.

## Syntax

````
DWScripter
      -S: Server source
      -D: Database Source
      -E: Trusted connection
      -U: Login source id
      -P: Password source
      -W: work mode [DML|DDL|ALL]
      -O: Work  path, no space allowed
      -M: mode [Full|PersistStructure|Compare|CompareFromFile]]
      -St: Server target
      -dt: Database Target
      -Ut: login id target
      -Pt: password target
      -F: filter on feature for scripting
      -Fp: filters file path, no space allowed
      -X: Exclusion filter
      -t: Command Timoute
      -?, -h, --help: (usage)

````

## Command Line Options

**-S** *server[,port]*
Specifies the SQL Data Warehouse or Parallel Data Warehouse instance to which to connect. For SQL Data Warehouse the value is the entire server name, e.g. **myserver.database.windows.net**. For Parallel Data Warehouse the paramter is the Engine IP (CTL node IP) to connect to APS. e.g. -S:192.168.1.100,17001. The connection port is **requireed** for Analytics Platform System, and is defaulted to 1433 for SQL Data Warehouse.

**-D:** *database*
Source database from which to script out the objects

**-E:**
Trusted Connection. Uses integrated authentication connectin with credentials of the user currently running the tool Command Prompt or PowerShell session.

**-U:** *login_id*
User Name to connect to the SQL Data Warehouse instance or APS (when using SQL Authentication)

**-P:** *password*
Is a User-specified Password (when using SQL Authentication). Passwords are case sensitive.

**-W:** **[DML|DDL|ALL]**
Defines the work mode the tool will operate with. If DML, the tool will only output DML definitions (Stored Procedures, Functions, Views). If DDL the tool will only output DDL (Schemas and Tables definitions with related indexes and statistics definitions). If ALL, all definitions are processed.

**-O:** *work path*
Identifies the folder and file (suffix) where the output files will be placed. If mode (-M) is CompareFromFile -O specifies the folder where the source file is read from **AND** where the output files will be saved.

**-M:** **[FULL|PersistStructure|Compare|CompareFromFile]**

- **FULL:** Creates .dsql scripts. Output is based on the value of -W:[DDL|DML|ALL]. Output files will be named *DatabaseName*_STRUCT_DDL.dsql and *DatabaseName*_STRUCT_DML.dsql
- **PersistStructure:** Creates output in the form of JSON files. Output is based on the value of -W:[DDL|DML|ALL]. Files will be named *DatabaseName*_STRUCT_DDL.json and *DatabaseName*_STRUCT_DML.json
- **Compare:** Generates two .dsql delta files for DML and DDL and a warning script. Requires access to both source and target database. The warning file will contain information about actions that might issue a potential data loss (DROP Table, DROP Column,...)
- **CompareFromFile:** Generates two .dsql delta files for DML and DDL and a warning script by comparing the target database with the .json files

**St:** *server[,port]*
Target server against which run the compare. Required if mode is Compare (-M:Compare), unused otherwise

**-dt:** *database name*
Target database against which run the compare Required if mode is Compare (-M:Compare), unused otherwise

**-Ut:** *login_id*
User name to connect to target server and database

**-Pt:**
User defined password to connect to target server and database. Passwords are case sensitive.

**-F:**
Feature name filter. <>
Requires source database (-D:)

**-Fp:**
Filters to apply to file path. The Json files are automatically selected based on the database name (-D:). Unused if -F is not used<>

**-X:**
Regular expression pattern to define object exclusions. Objects which names match the regular expression provided will be excluded from the scripting.

**-t:**
The time in seconds to wait for the command to execute.

## Limitations and Restrictions

Table, columns and object renaming is not supported. These must be managed with a custom pre-deployment script

The following features and objects are not supported:

- External tables
- External File Formats
- External Data Sources

## Examples

### **Script all objects in .dsql file**

The following example will generate the files:
C:\Dev\APS\DW_STG_DDL.dsql
C:\Dev\APS\DW_STG_DML.dsql

```PowerShell
#Parallel Data Warehouse syntax
.\DWScripter.exe -S:192.168.1.1,17001 -D:Fabrikam_STG_DEV -U:<userlogin> -P:<userPassword> -O:C:\Dev\APS\DW_STG -M:Full -W:ALL -F:ALL

#Azure SQL Data Warehouse Syntax
.\DWScripter.exe -S:FabrikamDW.database.windows.net -D:Fabrikam_STG_DEV -U:<userlogin> -P:<userPassword> -O:C:\Dev\APS\DW_STG -M:Full -W:ALL -F:ALL
```

### **Script all objects in .json file**

This syntax can be used to produce a JSON structure of the database objects as a persisted structure for future comparison. The following example will generate the files:

C:\Dev\APS\DW_STG_DDL.json
C:\Dev\APS\DW_STG_DML.json

```PowerShell
#Parallel Data Warehouse syntax
.\DWScripter.exe -S:192.168.1.1,17001 -D:Fabrikam_STG_DEV -U:<userlogin> -P:<userPassword> -O:C:\Dev\APS\DW_STG -M:PersistStructure

#Azure SQL Data Warehouse syntax
.\DWScripter.exe -S:FabrikamDW.database.windows.net -D:Fabrikam_STG_DEV -U:<userlogin> -P:<userPassword> -O:C:\Dev\APS\DW_STG -M:PersistStructure
```

### **Compare two databases and generate a delta .dsql script**

This syntax will generate delta .dsql scripts and a warning script. The warning file will contain statements that might lead to data loss (DROP table, DROP column...). Access to both databases is required. The following files will be generated

C:\Dev\APS\DW_STG_DDL.dsql
C:\Dev\APS\DW_STG_DML.dsql
C:\Dev\APS\DW_STG_DDL.warn

```PowerShell
#Parallel Data Warehouse syntax
.\DWScripter.exe -S:192.168.1.1,17001 -D:Fabrikam_DWH_DEV -O:C:\Dev\APS\DW_STG-M:Compare -U:<userlogin> -P:<userPassword> -St:10.192.168.10,17001 -Dt:Fabrikam_DWH_INT -Ut:<Targetuserlogin> -Pt:<TargetuserPassword> -F:All

#Azure SQL Data Warehouse syntax
.\DWScripter.exe -S:FabrikamDW.database.windows.net -D:Fabrikam_DWH_DEV -O:C:\Dev\APS\DW_STG -M:Compare -U:<userlogin> -P:<userPassword> -St:pdwQA.database.windows.net -Dt:Fabrikam_DWH_INT -Ut:<Targetuserlogin> -Pt:<TargetuserPassword> -F:All
```

### **Compare a database to a persisted structure**

This syntax will generate delta .dsql scripts and a warning script by comparing a database to a JSON persisted structure. The warning file will contain statements that might lead to data loss (DROP table, DROP column...). Access to both databases is required.
The work path parameter `-O:C:\Dev\APS\DW_STG` indicates the path and file suffix of the .json files to use for comparison. In this case the following files will be used:

C:\Dev\APS\DW_STG_SSL.json
C:\Dev\APS\DW_STG_DDL.json

and the following files will be generated

C:\Dev\APS\DW_STG_DDL.dsql
C:\Dev\APS\DW_STG_DML.dsql
C:\Dev\APS\DW_STG_DDL.warn

```PowerShell
#Parallel Data Warehouse syntax
.\DWScripter.exe -Ut:<userlogin> -Pt:<userPassword> -D:Fabrikam_DWH -O:C:\Dev\APS\DW_STG -M:CompareFromFile -St:192.168.1.1,17001 -Dt:Fabrikam_DWH_INT -F:ALL

#Azure SQL Data Warehouse syntax
.\DWScripter.exe -Ut:<userlogin> -Pt:<userPassword> -D:Fabrikam_DWH -O:C:\Dev\APS\DW_STG -M:CompareFromFile -St:FabrikamDW.database.windows.net -Dt:Fabrikam_DWH_INT -F:ALL
```

### **Exclude all _dev objects from scripting**

This syntax will script out only objects which names do not contain ```_dev```

```PowerShell
#Parallel Data Warehouse syntax
.\DWScripter.exe -S:192.168.1.1,17001 -D:Fabrikam_STG_DEV -U:<userlogin> -P:<userPassword> -O:C:\Dev\APS\DW_STG -M:Full -X:"_dev"

#Azure SQL Data Warehouse Syntax
.\DWScripter.exe -S:FabrikamDW.database.windows.net -D:Fabrikam_STG_DEV -U:<userlogin> -P:<userPassword> -O:C:\Dev\APS\DW_STG -M:Full -X:"_dev"
```

### **Only script _dev or _test objects**

This syntax will only script out objects which names contain ```_dev``` or ```_test```

```PowerShell
#Parallel Data Warehouse syntax
.\DWScripter.exe -S:192.168.1.1,17001 -D:Fabrikam_STG_DEV -E -O:C:\Dev\APS\DW_STG -M:Full -X:"^((?!_dev|_test).)*$"

#Azure SQL Data Warehouse Syntax
.\DWScripter.exe -S:FabrikamDW.database.windows.net -D:Fabrikam_STG_DEV -E -O:C:\Dev\APS\DW_STG -M:Full -X:"^((?!_dev|_test).)*$"
```

### **Running from Linux**

After building the tool you can execute it from the build directory using [```dotnet``` syntax](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-run?tabs=netcore2x).
The following example will generate the files:
home/DWAdmin/Documents/DW_STG_DDL.dsql
home/DWAdmin/Documents/DW_STG_DML.dsql

```bash
#Running the compiled code
#Open a shell window to the path of the compiled code
dotnet DWScripter.dll -S:FabrikamDW.database.windows.net -D:Fabrikam_STG_DEV -U:<userlogin> -P:<userPassword> -O:/home/DWAdmin/Documents/DW_STG -M:Full
```

You can run also the DWScripter from the project folder using the [```dotnet run``` syntax](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-run?tabs=netcore2x).

```bash
#Running from the project file
#Open a shell window to the path of the project file (DWScripter.csproj).
#The project parameter (-p) is optional when running from a path taht contains the project file.
dotnet run [-p DWScripter.csproj] -S:FabrikamDW.database.windows.net -D:Fabrikam_STG_DEV -U:<userlogin> -P:<userPassword> -O:/home/DWAdmin/Documents/DW_STG -M:Full
```
