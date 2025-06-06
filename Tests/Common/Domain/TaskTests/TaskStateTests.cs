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

using TaskStatus = ArmoniK.Extension.CSharp.Client.Common.Domain.Task.TaskStatus;

namespace ArmoniK.Tests.Common.Domain;

[TestFixture]
public class TaskStateTests
{
  [Test]
  public void CreateTaskStateTest()
  {
    var createAt  = DateTime.UtcNow;
    var startedAt = DateTime.UtcNow.AddMinutes(-10);
    var endedAt   = DateTime.UtcNow.AddMinutes(-5);

    var taskState = new TaskState(createAt,
                                  endedAt,
                                  startedAt,
                                  TaskStatus.Completed);
    Assert.Multiple(() =>
                    {
                      Assert.That(taskState.CreateAt,
                                  Is.EqualTo(createAt));
                      Assert.That(taskState.EndedAt,
                                  Is.EqualTo(endedAt));
                      Assert.That(taskState.StartedAt,
                                  Is.EqualTo(startedAt));
                      Assert.That(taskState.Status,
                                  Is.EqualTo(TaskStatus.Completed));
                    });
  }

  [TestCase(TaskStatus.Unspecified)]
  [TestCase(TaskStatus.Creating)]
  [TestCase(TaskStatus.Submitted)]
  [TestCase(TaskStatus.Dispatched)]
  [TestCase(TaskStatus.Completed)]
  [TestCase(TaskStatus.Error)]
  [TestCase(TaskStatus.Timeout)]
  [TestCase(TaskStatus.Cancelling)]
  [TestCase(TaskStatus.Cancelled)]
  [TestCase(TaskStatus.Processing)]
  [TestCase(TaskStatus.Processed)]
  [TestCase(TaskStatus.Retried)]
  [TestCase((TaskStatus)99)]
  public void TestTaskStatus(TaskStatus status)
  {
    switch (status)
    {
      case TaskStatus.Unspecified:
      case TaskStatus.Creating:
      case TaskStatus.Submitted:
      case TaskStatus.Dispatched:
      case TaskStatus.Completed:
      case TaskStatus.Error:
      case TaskStatus.Timeout:
      case TaskStatus.Cancelling:
      case TaskStatus.Cancelled:
      case TaskStatus.Processing:
      case TaskStatus.Processed:
      case TaskStatus.Retried:
        var grpcStatus = status.ToGrpcStatus();
        Assert.That(grpcStatus.ToString(),
                    Is.EqualTo(status.ToString()));

        var internalStatus = grpcStatus.ToInternalStatus();
        Assert.That(internalStatus,
                    Is.EqualTo(status));
        break;
      default:
        Assert.That(() => status.ToGrpcStatus(),
                    Throws.Exception.TypeOf<ArgumentOutOfRangeException>());
        break;
    }
  }
}
