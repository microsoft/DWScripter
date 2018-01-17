 Including PDWScripter in your projects

>NOTE: Before including PDWScripter library in your projects please review the [PDWScripter License](./LICENSE.md).

In order to include PDWScripter in your project you can:

- Reference the project. In this case you will need to download the library project and add the project code to your project. For more information on how to add a project reference to your .NET Core project see [here](https://docs.microsoft.com/en-us/dotnet/core/tools/dotnet-add-reference)
- Reference the library. In this case you will need to build the project and include a reference to the compiled library to your project. See the examples below to add a library reference to your .NET Core Project

## Examples

**Add a library reference in Visual Studio 2017**
If Visual Studio 2017 is your tool of choice, to can add a library reference (or a project reference) to your project follow the documentation at:
[Managing references in a project](https://docs.microsoft.com/en-us/visualstudio/ide/managing-references-in-a-project)

**Add a library reference to .NET Core projects**
The ```dotnet add reference``` syntax does not currently allow you to add a library reference to your project of a precompiled library. In order to add such a reference you will need to:

1. Obtain a copy of the compiled library and place it in a reachable location (e.g. a /libRef subfolder in your project)
2. Edit the .csproj ItemGroup section to add the include referene and HintPath to the library location.

In the following example we will add to a default .csproj file a reference to the PDWScripter library. The original .csproj file would look similar to:

```XML
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>netcoreapp2.0</TargetFramework>
  </PropertyGroup>

</Project>
```

We have placed PDWScripter in the libRef subfolder withing our project folder.
To that project file we will add the following portion to allow the framework to locate our library:

```XML

  <ItemGroup>
    <Reference Include="PDWScripter">
      <HintPath>..\LibRef\PDWScripter.dll</HintPath>
    </Reference>
  </ItemGroup>

```

The final .csproj file will look similar to:

```XML
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>netcoreapp2.0</TargetFramework>
  </PropertyGroup>

  <ItemGroup>
    <Reference Include="PDWScripter">
      <HintPath>..\LibRef\PDWScripter.dll</HintPath>
    </Reference>
  </ItemGroup>

</Project>
```
