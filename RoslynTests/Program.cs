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

using ArmoniK.Extension.CSharp.Generators;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

using NUnit.Framework;

using VerifyNUnit;

using VerifyTests;

namespace RoslynTests;

public class GeneratorTest
{
  [OneTimeSetUp]
  public void OneTimeSetup()
    => VerifySourceGenerators.Initialize();

  [Test]
  public void Foo()
  {
    var source = @"using ArmoniK.Extension.CSharp.Generators;
namespace Pouet {
  public class Service {
    [HandleMethod(HandleType = typeof(Plop.Handle))]
    public System.Threading.Tasks.Task<int> Foo(int a, [Handle] string b = ""pouet"") => System.Threading.Tasks.Task.FromResult(a + b);
  }
}

namespace Plop {
  public partial class Handle {}
}";

    var syntaxTree = CSharpSyntaxTree.ParseText(source);
    var references = new[]
                     {
                       MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
                     };
    var compilation = CSharpCompilation.Create("Tests",
                                               new[]
                                               {
                                                 syntaxTree,
                                               },
                                               references);
    var             generator = new ServiceGenerator();
    GeneratorDriver driver    = CSharpGeneratorDriver.Create(generator);
    driver = driver.RunGenerators(compilation);

    Verifier.Verify(driver);
  }
}
