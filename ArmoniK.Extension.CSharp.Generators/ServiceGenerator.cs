// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Text;
using System.Threading;

using ArmoniK.Extension.CSharp.Generators.Extensions;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace ArmoniK.Extension.CSharp.Generators;

[Generator]
public class ServiceGenerator : IIncrementalGenerator
{
  public const string Attribute = @"
namespace ArmoniK.Extension.CSharp.Generators
{
    [System.AttributeUsage(System.AttributeTargets.Class)]
    public class ServiceAttribute : System.Attribute
    {
      public System.Type HandleType { get; set; }
    }

    [System.AttributeUsage(System.AttributeTargets.Method)]
    public class HandleMethodAttribute : System.Attribute
    {
      public System.Type HandleType { get; set; }
      public string Name { get; set; }
    }

    [System.AttributeUsage(System.AttributeTargets.Parameter)]
    public class HandleAttribute : System.Attribute
    {
    }
}";

  public void Initialize(IncrementalGeneratorInitializationContext context)
  {
    context.RegisterPostInitializationOutput(static ctx => ctx.AddSource("ServiceAttribute.g.cs",
                                                                         Attribute));

    var methodsToGenerate = context.SyntaxProvider.ForAttributeWithMetadataName("ArmoniK.Extension.CSharp.Generators.HandleMethodAttribute",
                                                                                static (_,
                                                                                        _) => true,
                                                                                GetMethodsToGenerate);

    context.RegisterSourceOutput(methodsToGenerate,
                                 Execute);
  }

  private static void Execute(SourceProductionContext ctx,
                              HandleMethod            handleMethod)
  {
    var namespaceSymbol = handleMethod.HandleType.ContainingNamespace;
    var serviceClass    = handleMethod.Method.ContainingType!;
    var handleClass     = handleMethod.HandleType;
    var targetMethod    = handleMethod.Method;

    var serviceName = serviceClass.TypeKind is TypeKind.Interface
                        ? serviceClass.Name.Substring(1)
                        : serviceClass.Name;

    var parameters    = new StringBuilder();
    var useParameters = new StringBuilder();

    foreach (var param in targetMethod.Parameters)
    {
      if (SymbolEqualityComparer.Default.Equals(param,
                                                handleMethod.HandleParameter))
      {
        useParameters.AppendWithSeparator("this",
                                          ", ");
      }
      else
      {
        useParameters.AppendWithSeparator(param.Name,
                                          ", ");
        parameters.AppendWithSeparator(param.Type.ToDisplayString(),
                                       ", ");
        parameters.Append(" ");
        parameters.Append(param.Name);

        if (param.HasExplicitDefaultValue)
        {
          var syntaxNode = param.DeclaringSyntaxReferences[0]
                                .GetSyntax() as ParameterSyntax;
          parameters.Append(" ");
          parameters.Append(syntaxNode?.Default?.ToString() ?? "= default");
        }
      }
    }


    var generated = $@"
namespace {namespaceSymbol} {{
  public partial class {handleClass.Name} {{
    /// <inheritdoc cref=""{targetMethod}"" />
    public {targetMethod.ReturnType} {handleMethod.Name}({parameters})
      => ArmoniKClient.{serviceName}.{targetMethod.Name}({useParameters});
  }}
}}
";

    ctx.AddSource($"{handleClass.Name}.{handleMethod.Name}.g.cs",
                  generated);
  }

  private static HandleMethod GetMethodsToGenerate(GeneratorAttributeSyntaxContext ctx,
                                                   CancellationToken               ct)
  {
    var method = (IMethodSymbol)ctx.SemanticModel.GetDeclaredSymbol(ctx.TargetNode)!;

    var attr       = method.GetAttribute("ArmoniK.Extension.CSharp.Generators.HandleMethodAttribute")!;
    var handleType = attr.GetParam<INamedTypeSymbol?>("HandleType");

    if (handleType is null)
    {
      var classAttr = method.ReceiverType!.GetAttribute("ArmoniK.Extension.CSharp.Generators.ServiceAttribute")!;
      handleType = classAttr.GetParam<INamedTypeSymbol>("HandleType");
    }

    IParameterSymbol? handleParameter = null;

    foreach (var parameter in method.Parameters)
    {
      var parameterAttr = parameter.GetAttribute("ArmoniK.Extension.CSharp.Generators.HandleAttribute");

      if (parameterAttr is not null || handleParameter is null)
      {
        handleParameter = parameter;
      }
    }

    return new HandleMethod
           {
             HandleType = handleType,
             Method     = method,
             Name = attr.GetParam("Name",
                                  method.Name),
             HandleParameter = handleParameter!,
           };
  }

  private readonly record struct HandleMethod(INamedTypeSymbol HandleType,
                                              IMethodSymbol    Method,
                                              string           Name,
                                              IParameterSymbol HandleParameter);
}
