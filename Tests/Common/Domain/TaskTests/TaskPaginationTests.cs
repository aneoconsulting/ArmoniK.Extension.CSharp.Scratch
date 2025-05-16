using NUnit.Framework;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using TaskStatus = ArmoniK.Extension.CSharp.Client.Common.Domain.Task.TaskStatus;
using NUnit.Framework.Legacy;
using ArmoniK.Extension.CSharp.Client.Common.Enum;
using ArmoniK.Api.gRPC.V1.Tasks;

namespace ArmoniK.Tests.Common.Domain
{
    [TestFixture]
    public class TaskPaginationTests
    {
        [Test]
        public void CreateTaskPaginationTest()
        {
            var taskPagination = new TaskPagination
            {
                Page = 1,
                PageSize = 10,
                Total = 100,
                SortDirection = SortDirection.Asc,
                Filter = new Filters() 
            };

            ClassicAssert.AreEqual(1, taskPagination.Page);
            ClassicAssert.AreEqual(10, taskPagination.PageSize);
            ClassicAssert.AreEqual(100, taskPagination.Total);
            ClassicAssert.AreEqual(SortDirection.Asc, taskPagination.SortDirection);
            ClassicAssert.IsNotNull(taskPagination.Filter);
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
                Tuple.Create("task1", TaskStatus.Completed),
                Tuple.Create("task2", TaskStatus.Processing)
            };

            var taskPage = new TaskPage
            {
                TotalTasks = 2,
                TasksData = tasksData
            };

            ClassicAssert.AreEqual(2, taskPage.TotalTasks);
            ClassicAssert.AreEqual(tasksData, taskPage.TasksData);
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
                new TaskState
                {
                    CreateAt = DateTime.UtcNow,
                    Status = TaskStatus.Completed
                },
                new TaskState
                {
                    CreateAt = DateTime.UtcNow,
                    Status = TaskStatus.Processing
                }
            };

            var taskDetailedPage = new TaskDetailedPage
            {
                TotalTasks = 2,
                TaskDetails = taskDetails
            };

            ClassicAssert.AreEqual(2, taskDetailedPage.TotalTasks);
            ClassicAssert.AreEqual(taskDetails, taskDetailedPage.TaskDetails);
        }
    }
}
