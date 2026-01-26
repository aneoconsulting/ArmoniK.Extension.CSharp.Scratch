// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

using NUnit.Framework;

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

    Assert.That(taskConfiguration.MaxRetries,
                Is.EqualTo(3));
    Assert.That(taskConfiguration.Priority,
                Is.EqualTo(1));
    Assert.That(taskConfiguration.PartitionId,
                Is.EqualTo("partition1"));
    Assert.That(taskConfiguration.Options,
                Is.EqualTo(options));
    Assert.That(taskConfiguration.MaxDuration,
                Is.EqualTo(TimeSpan.FromMinutes(30)));
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
    Assert.Multiple(() =>
                    {
                      Assert.That(taskOptions.MaxRetries,
                                  Is.EqualTo(3));
                      Assert.That(taskOptions.Priority,
                                  Is.EqualTo(1));
                      Assert.That(taskOptions.PartitionId,
                                  Is.EqualTo("partition1"));
                      Assert.That(taskOptions.Options.Count,
                                  Is.EqualTo(2));
                      Assert.That(taskOptions.MaxDuration.ToTimeSpan(),
                                  Is.EqualTo(TimeSpan.FromMinutes(30)));
                    });
  }

  [Test]
  public void TaskConfigurationToTaskOptionsWithoutOptionsTest()
  {
    var taskConfiguration = new TaskConfiguration(3,
                                                  1,
                                                  "partition1",
                                                  TimeSpan.FromMinutes(30));

    var taskOptions = taskConfiguration.ToTaskOptions();
    Assert.Multiple(() =>
                    {
                      Assert.That(taskOptions.MaxRetries,
                                  Is.EqualTo(3));
                      Assert.That(taskOptions.Priority,
                                  Is.EqualTo(1));
                      Assert.That(taskOptions.PartitionId,
                                  Is.EqualTo("partition1"));
                      Assert.That(taskOptions.Options.Count,
                                  Is.EqualTo(0));
                      Assert.That(taskOptions.MaxDuration.ToTimeSpan(),
                                  Is.EqualTo(TimeSpan.FromMinutes(30)));
                    });
  }
}
