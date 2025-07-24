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
using ArmoniK.Extension.CSharp.Client.Queryable;
using ArmoniK.Extension.CSharp.Client.Queryable.TaskStateQuery;

using NUnit.Framework;

using Tests.Configuration;

using TaskStatus = ArmoniK.Extension.CSharp.Client.Common.Domain.Task.TaskStatus;

namespace Tests.Queryable;

public class FilterTaskTests : BaseTaskFilterTests
{
  [Test]
  public void TaskIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void SessionIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.SessionId == "session1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void PayloadIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("PayloadId",
                                                    "==",
                                                    "payload1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.PayloadId == "payload1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void CreateAtFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.UtcNow;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("CreateAt",
                                                      "==",
                                                      date)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.CreateAt == date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void StartedAtFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.UtcNow;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("StartedAt",
                                                      "==",
                                                      date)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.StartedAt == date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void EndedAtFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.UtcNow;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("EndedAt",
                                                      "==",
                                                      date)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.EndedAt == date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void StatusFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Error)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.Status == TaskStatus.Error);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void MaxDurationFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var duration = new TimeSpan(1000);
    var filter = BuildOr(BuildAnd(BuildFilterDuration("MaxDuration",
                                                      "==",
                                                      duration)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.MaxDuration == duration);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void MaxRetriesFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("MaxRetries",
                                                 "==",
                                                 2)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.MaxRetries == 2);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void PriorityFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Priority",
                                                 "==",
                                                 1)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Priority == 1);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void PartitionIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("PartitionId",
                                                    "==",
                                                    "partition1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.PartitionId == "partition1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "==",
                                                    "lib1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options["library"] == "lib1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterEqualWithLambda()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "==",
                                                    "lib1")));

    var foo = () => "library";
    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options[foo()] == "lib1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }


  [Test]
  public void OptionsFilterStartsWith()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "StartsWith",
                                                    "lib")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options["library"]
                                                   .StartsWith("lib"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterEndsWith()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "EndsWith",
                                                    "lib")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options["library"]
                                                   .EndsWith("lib"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterContains()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "Contains",
                                                    "lib")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options["library"]
                                                   .Contains("lib"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterNotContains()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "NotContains",
                                                    "lib")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => !taskState.TaskOptions.Options["library"]
                                                    .Contains("lib"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OrderByPayloadIdAscending()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));
    var sortCriteria = new TaskField
                       {
                         TaskSummaryField = new TaskSummaryField
                                            {
                                              Field = TaskSummaryEnumField.PayloadId,
                                            },
                       };

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1")
                      .OrderBy(taskState => taskState.PayloadId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter,
                                               sortCriteria)));
  }

  [Test]
  public void OrderByPartitionIdAscending()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));
    var sortCriteria = new TaskField
                       {
                         TaskOptionField = new TaskOptionField
                                           {
                                             Field = TaskOptionEnumField.PartitionId,
                                           },
                       };

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1")
                      .OrderBy(taskState => taskState.TaskOptions.PartitionId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter,
                                               sortCriteria)));
  }

  [Test]
  public void OrderBySpecificOptionAscending()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));
    var sortCriteria = new TaskField
                       {
                         TaskOptionGenericField = new TaskOptionGenericField
                                                  {
                                                    Field = "Library",
                                                  },
                       };

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1")
                      .OrderBy(taskState => taskState.TaskOptions.Options["Library"]);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskStateQueryProvider)((ArmoniKQueryable<TaskState>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter,
                                               sortCriteria)));
  }
}
