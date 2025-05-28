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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extension.CSharp.Client.Common.Enum;

using NUnit.Framework;

using TaskStatus = ArmoniK.Extension.CSharp.Client.Common.Domain.Task.TaskStatus;

namespace ArmoniK.Tests.Common.Domain;

[TestFixture]
public class TaskPaginationTests
{
  [Test]
  public void CreateTaskPaginationTest()
  {
    var taskPagination = new TaskPagination
                         {
                           Page          = 1,
                           PageSize      = 10,
                           Total         = 100,
                           SortDirection = SortDirection.Asc,
                           Filter        = new Filters(),
                         };
    Assert.Multiple(() =>
                    {
                      Assert.That(taskPagination.Page,
                                  Is.EqualTo(1));
                      Assert.That(taskPagination.PageSize,
                                  Is.EqualTo(10));
                      Assert.That(taskPagination.Total,
                                  Is.EqualTo(100));
                      Assert.That(taskPagination.SortDirection,
                                  Is.EqualTo(SortDirection.Asc));
                      Assert.That(taskPagination.Filter,
                                  Is.Not.Null);
                    });
  }
}

[TestFixture]
public class TaskPageTests
{
  [Test]
  public void CreateTaskPageTest()
  {
    var tasksData = new List<Tuple<string, TaskStatus>>
                    {
                      Tuple.Create("task1",
                                   TaskStatus.Completed),
                      Tuple.Create("task2",
                                   TaskStatus.Processing),
                    };

    var taskPage = new TaskPage
                   {
                     TotalTasks = 2,
                     TasksData  = tasksData,
                   };

    Assert.That(taskPage.TotalTasks,
                Is.EqualTo(2));
    Assert.That(taskPage.TasksData,
                Is.EqualTo(tasksData));
  }
}

[TestFixture]
public class TaskDetailedPageTests
{
  [Test]
  public void CreateTaskDetailedPageTest()
  {
    var taskDetails = new List<TaskState>
                      {
                        new()
                        {
                          CreateAt = DateTime.UtcNow,
                          Status   = TaskStatus.Completed,
                        },
                        new()
                        {
                          CreateAt = DateTime.UtcNow,
                          Status   = TaskStatus.Processing,
                        },
                      };

    var taskDetailedPage = new TaskDetailedPage
                           {
                             TotalTasks  = 2,
                             TaskDetails = taskDetails,
                           };

    Assert.That(taskDetailedPage.TotalTasks,
                Is.EqualTo(2));
    Assert.That(taskDetailedPage.TaskDetails,
                Is.EqualTo(taskDetails));
  }
}
