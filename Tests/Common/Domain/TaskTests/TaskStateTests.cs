using NUnit.Framework;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;

using NUnit.Framework.Legacy;

using TaskStatus = ArmoniK.Extension.CSharp.Client.Common.Domain.Task.TaskStatus;

namespace ArmoniK.Tests.Common.Domain
{
    [TestFixture]
    public class TaskStateTests
    {
        [Test]
        public void CreateTaskStateTest()
        {
            var createAt = DateTime.UtcNow;
            var startedAt = DateTime.UtcNow.AddMinutes(-10);
            var endedAt = DateTime.UtcNow.AddMinutes(-5);

            var taskState = new TaskState(createAt, endedAt, startedAt, Extension.CSharp.Client.Common.Domain.Task.TaskStatus.Completed);

            ClassicAssert.AreEqual(createAt, taskState.CreateAt);
            ClassicAssert.AreEqual(endedAt, taskState.EndedAt);
            ClassicAssert.AreEqual(startedAt, taskState.StartedAt);
            ClassicAssert.AreEqual(Extension.CSharp.Client.Common.Domain.Task.TaskStatus.Completed, taskState.Status);
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
                    var grpcStatus = TaskStatusExt.ToGrpcStatus(status);
                    ClassicAssert.AreEqual(status.ToString(), grpcStatus.ToString());

                    var internalStatus = TaskStatusExt.ToInternalStatus(grpcStatus);
                    ClassicAssert.AreEqual(status, internalStatus);
                    break;
                default:
                    ClassicAssert.Throws<ArgumentOutOfRangeException>(() =>
                    {
                        TaskStatusExt.ToGrpcStatus(status);
                    });
                    break;
            }
        }
    }
}
