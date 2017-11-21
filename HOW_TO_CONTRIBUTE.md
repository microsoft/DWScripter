# **Contributing to DWScripter**

There are many ways of contributing to DWScripter project: logging bugs, submitting pull requests, reporting issues and creating suggestions

After cloning the repo you can build the solution and start using and testing.

## **Build and Run from Source**

If you want to understand how DWScripter works or want to debug an issue or implement new functionality, you'll want to get the source, build it, and run the tool.

### **Prerequisites**

You will neet [git](https://git-scm.com/) to clone the repo.
In order to build DWScripter you will need .NET Core 2.0.2

### **How to Build**

Once you have obtained the source code and installed .NET Core 2.0.0 you can build the tool following below steps:

1. Open a Command Prompt, Powershell or other shell window to the project's folder (the folder containing the DWScripter.csproj file)
2. Run `dotnet build -r <runtime>`

> Note: runtime is currently configured in DWScripter.csproj file as win10-x64. The `dotnet build` command (without further parameters) will build by default for that runtime. If you wish to build for different runtimes you can use the `-r <runtime>` option parameter to build for your specific needs. For a list of available runtimes see [.NET Core RID Catalog](https://docs.microsoft.com/en-us/dotnet/core/rid-catalog)

#### **Building  for Windows 10 64 bit**

> Example:
`dotnet build -r win10-x64`

#### **Building for Windows 7 64 bit**

>Example
`dotnet build -r win7-x64`

For more reference on building with .NET Core see [dotnet build](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-build?tabs=netcore2x) guide
