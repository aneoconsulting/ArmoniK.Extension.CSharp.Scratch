using NUnit.Framework;
using ArmoniK.Extension.CSharp.Client.Common.Domain.Versions;
using NUnit.Framework.Legacy;

namespace ArmoniK.Tests.Common.Domain
{
    [TestFixture]
    public class VersionsInfoTests
    {
        [Test]
        public void CreateVersionsInfoTest()
        {
            var versionsInfo = new VersionsInfo
            {
                Core = "1.0.0",
                Api = "2.0.0"
            };

            ClassicAssert.AreEqual("1.0.0", versionsInfo.Core);
            ClassicAssert.AreEqual("2.0.0", versionsInfo.Api);
        }

        [Test]
        public void VersionsInfoEqualityTest()
        {
            var versionsInfo1 = new VersionsInfo
            {
                Core = "1.0.0",
                Api = "2.0.0"
            };

            var versionsInfo2 = new VersionsInfo
            {
                Core = "1.0.0",
                Api = "2.0.0"
            };

            ClassicAssert.AreEqual(versionsInfo1, versionsInfo2);
        }

        [Test]
        public void VersionsInfoInequalityTest()
        {
            var versionsInfo1 = new VersionsInfo
            {
                Core = "1.0.0",
                Api = "2.0.0"
            };

            var versionsInfo2 = new VersionsInfo
            {
                Core = "3.0.0",
                Api = "4.0.0"
            };

            ClassicAssert.AreNotEqual(versionsInfo1, versionsInfo2);
        }

        [Test]
        public void VersionsInfoNullOrEmptyTest()
        {
            var versionsInfo = new VersionsInfo
            {
                Core = null,
                Api = string.Empty
            };

            ClassicAssert.IsNull(versionsInfo.Core);
            ClassicAssert.IsEmpty(versionsInfo.Api);
        }
    }
}
