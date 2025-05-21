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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ArmoniK.Tests.Common.Domain;

[TestFixture]
public class TaskConfigurationTests
{
  [Test]
  public void CreateTaskConfigurationTest()
  {
    var options = new Dictionary<string, string>
                  {
                    {
                      "option1", "value1"
                    },
                    {
                      "option2", "value2"
                    },
                  };

    var taskConfiguration = new TaskConfiguration(3,
                                                  1,
                                                  "partition1",
                                                  TimeSpan.FromMinutes(30),
                                                  options);

    ClassicAssert.AreEqual(3,
                           taskConfiguration.MaxRetries);
    ClassicAssert.AreEqual(1,
                           taskConfiguration.Priority);
    ClassicAssert.AreEqual("partition1",
                           taskConfiguration.PartitionId);
    ClassicAssert.AreEqual(options,
                           taskConfiguration.Options);
    ClassicAssert.AreEqual(TimeSpan.FromMinutes(30),
                           taskConfiguration.MaxDuration);
  }

  [Test]
  public void TaskConfigurationToTaskOptionsTest()
  {
    var options = new Dictionary<string, string>
                  {
                    {
                      "option1", "value1"
                    },
                    {
                      "option2", "value2"
                    },
                  };

    var taskConfiguration = new TaskConfiguration(3,
                                                  1,
                                                  "partition1",
                                                  TimeSpan.FromMinutes(30),
                                                  options);

    var taskOptions = taskConfiguration.ToTaskOptions();

    ClassicAssert.AreEqual(3,
                           taskOptions.MaxRetries);
    ClassicAssert.AreEqual(1,
                           taskOptions.Priority);
    ClassicAssert.AreEqual("partition1",
                           taskOptions.PartitionId);
    ClassicAssert.AreEqual(2,
                           taskOptions.Options.Count);
    ClassicAssert.AreEqual(TimeSpan.FromMinutes(30),
                           taskOptions.MaxDuration.ToTimeSpan());
  }

  [Test]
  public void TaskConfigurationToTaskOptionsWithoutOptionsTest()
  {
    var taskConfiguration = new TaskConfiguration(3,
                                                  1,
                                                  "partition1",
                                                  TimeSpan.FromMinutes(30));

    var taskOptions = taskConfiguration.ToTaskOptions();

    ClassicAssert.AreEqual(3,
                           taskOptions.MaxRetries);
    ClassicAssert.AreEqual(1,
                           taskOptions.Priority);
    ClassicAssert.AreEqual("partition1",
                           taskOptions.PartitionId);
    ClassicAssert.AreEqual(0,
                           taskOptions.Options.Count);
    ClassicAssert.AreEqual(TimeSpan.FromMinutes(30),
                           taskOptions.MaxDuration.ToTimeSpan());
  }
}
