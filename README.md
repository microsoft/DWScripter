# Introduction

DWScripter is a scripting tool for Analytics Platform System and Azure SQLDW. This Console app will script the ddl and dml for both APS and SQLDW.
Supports: PDW V2, Azure SQLDW, DDL and DML, schemas (version 2.4)

The project is intended as a cross platform tool, and as such is built on .NET Core SDK 2.0.2. You can build the solution and run the tool from the OS of your choice.

## Requirements

PDWScripter requires [Newtonsoft.json](https://github.com/JamesNK/Newtonsoft.Json) version 10.0.3 or above. You will need to add the package to your system. For more information and samples on how to add the package see [Newtonsoft.json NuGet page](https://www.nuget.org/packages/Newtonsoft.Json).

## Getting Started

Dowload the sources and build DWScripter tool. For building instructions see [How To Contribute](./HOW_TO_CONTRIBUTE.md)

The Solution contains two projects:

1. PDWScripter
2. DWScripter

**PDWScripter** is a class library project, made with the purpose of being reusable in custom projects to provide the Data Warehouse object scripting capability. For more information on using the library in your projects, please refer to [PDWScripter How To](./src/PDWScripter/Docs/PDWScripter_How_to.md) and to [PDWScripter License](./src/PDWScripter/Docs/LICENSE)
**DWScripter** is a console application that makes use of the PDWScripter library

DWScripter requires some paramters to identify the instance and databases to be scripted, as well as the object (DML, DDL or both).

You must invoke DWScripter passing all the required parameters.
For more information on the tool usage see [Usage](./USAGE.md)

## Current version is 1.0.0

### USAGE

For more information on the tool usage see [Usage](./USAGE.md)

## Contributing

If you are interested in fixing issues and contributing directly to the code base please see [contributing guidelines](./CONTRIBUTING.md)

Please also review our [Code of Conduct](https://opensource.microsoft.com/codeofconduct/).

## Reporting Security Issues

Security issues and bugs should be reported privately, via email, to the Microsoft Security
Response Center (MSRC) at [secure@microsoft.com](mailto:secure@microsoft.com). You should
receive a response within 24 hours. If for some reason you do not, please follow up via
email to ensure we received your original message. Further information, including the
[MSRC PGP](https://technet.microsoft.com/en-us/security/dn606155) key, can be found in
the [Security TechCenter](https://technet.microsoft.com/en-us/security/default).
