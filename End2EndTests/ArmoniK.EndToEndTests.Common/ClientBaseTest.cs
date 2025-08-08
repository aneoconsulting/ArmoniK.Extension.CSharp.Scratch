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

using ArmoniK.Extension.CSharp.Client.Common;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.EndToEndTests.Common;

[PublicAPI]
public abstract class ClientBaseTest<T>
{
  public ClientBaseTest(IConfiguration configuration,
                        ILoggerFactory loggerFactory)
  {
    Configuration = configuration;
    LoggerFactory = loggerFactory;
    Log           = LoggerFactory.CreateLogger<T>();
    Properties    = new Properties(Configuration);
  }

  protected IConfiguration Configuration { get; set; }

  protected static ILogger<T> Log { get; set; }

  protected ILoggerFactory LoggerFactory { get; set; }

  protected Properties Properties { get; set; }

  protected virtual TaskConfiguration InitializeTaskConfiguration()
    => new(5,
           1,
           Environment.GetEnvironmentVariable("PARTITION") ?? "",
           TimeSpan.FromSeconds(300));

  public abstract Task EntryPoint();
}
