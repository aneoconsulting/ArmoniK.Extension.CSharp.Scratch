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

    ClassicAssert.AreEqual(createAt,
                           taskState.CreateAt);
    ClassicAssert.AreEqual(endedAt,
                           taskState.EndedAt);
    ClassicAssert.AreEqual(startedAt,
                           taskState.StartedAt);
    ClassicAssert.AreEqual(TaskStatus.Completed,
                           taskState.Status);
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
        ClassicAssert.AreEqual(status.ToString(),
                               grpcStatus.ToString());

        var internalStatus = grpcStatus.ToInternalStatus();
        ClassicAssert.AreEqual(status,
                               internalStatus);
        break;
      default:
        ClassicAssert.Throws<ArgumentOutOfRangeException>(() =>
                                                          {
                                                            status.ToGrpcStatus();
                                                          });
        break;
    }
  }
}
