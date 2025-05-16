using NUnit.Framework;
using System;
using System.Collections.Generic;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Task;
using NUnit.Framework.Legacy;

namespace ArmoniK.Tests.Common.Domain
{
    [TestFixture]
    public class TaskConfigurationTests
    {
        [Test]
        public void CreateTaskConfigurationTest()
        {
            var options = new Dictionary<string, string>
            {
                { "option1", "value1" },
                { "option2", "value2" }
            };

            var taskConfiguration = new TaskConfiguration(
                maxRetries: 3,
                priority: 1,
                partitionId: "partition1",
                maxDuration: TimeSpan.FromMinutes(30),
                options: options
            );

            ClassicAssert.AreEqual(3, taskConfiguration.MaxRetries);
            ClassicAssert.AreEqual(1, taskConfiguration.Priority);
            ClassicAssert.AreEqual("partition1", taskConfiguration.PartitionId);
            ClassicAssert.AreEqual(options, taskConfiguration.Options);
            ClassicAssert.AreEqual(TimeSpan.FromMinutes(30), taskConfiguration.MaxDuration);
        }

        [Test]
        public void TaskConfigurationToTaskOptionsTest()
        {
            var options = new Dictionary<string, string>
            {
                { "option1", "value1" },
                { "option2", "value2" }
            };

            var taskConfiguration = new TaskConfiguration(
                maxRetries: 3,
                priority: 1,
                partitionId: "partition1",
                maxDuration: TimeSpan.FromMinutes(30),
                options: options
            );

            var taskOptions = taskConfiguration.ToTaskOptions();

            ClassicAssert.AreEqual(3, taskOptions.MaxRetries);
            ClassicAssert.AreEqual(1, taskOptions.Priority);
            ClassicAssert.AreEqual("partition1", taskOptions.PartitionId);
            ClassicAssert.AreEqual(2, taskOptions.Options.Count);
            ClassicAssert.AreEqual(TimeSpan.FromMinutes(30), taskOptions.MaxDuration.ToTimeSpan());
        }

        [Test]
        public void TaskConfigurationToTaskOptionsWithoutOptionsTest()
        {
            var taskConfiguration = new TaskConfiguration(
                maxRetries: 3,
                priority: 1,
                partitionId: "partition1",
                maxDuration: TimeSpan.FromMinutes(30)
            );

            var taskOptions = taskConfiguration.ToTaskOptions();

            ClassicAssert.AreEqual(3, taskOptions.MaxRetries);
            ClassicAssert.AreEqual(1, taskOptions.Priority);
            ClassicAssert.AreEqual("partition1", taskOptions.PartitionId);
            ClassicAssert.AreEqual(0, taskOptions.Options.Count);
            ClassicAssert.AreEqual(TimeSpan.FromMinutes(30), taskOptions.MaxDuration.ToTimeSpan());
        }
    }
}
