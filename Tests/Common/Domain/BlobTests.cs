// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using System.Reflection.Metadata;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ArmoniK.Tests.Common.Domain
{
    /// <summary>
    ///   Unit tests for the BlobInfo class.
    /// </summary>
    [TestFixture]

    public class BlobTests
    {
        [Test]
        public void CreateBlobInfoTest()
        {
            var blobInfo = new BlobInfo
            {
                SessionId = "sessionId",
                BlobName = "blobName",
                BlobId = "blobId"
            };

            ClassicAssert.AreEqual("sessionId", blobInfo.SessionId);
            ClassicAssert.AreEqual("blobName", blobInfo.BlobName);
            ClassicAssert.AreEqual("blobId", blobInfo.BlobId);
        }

        [Test]
        public void CreateBlobStateTest()
        {
            var blobState = new BlobState
            {
                SessionId = "sessionId",
                BlobName = "blobName",
                BlobId = "blobId",
                Status = BlobStatus.Created,
                CreateAt = DateTime.UtcNow
            };

            ClassicAssert.AreEqual("sessionId", blobState.SessionId);
            ClassicAssert.AreEqual("blobName", blobState.BlobName);
            ClassicAssert.AreEqual("blobId", blobState.BlobId);
            ClassicAssert.AreEqual(BlobStatus.Created, blobState.Status);
        }

        [Test]
        public void CreateBlobPageTest()
        {
            var blobDetails = new BlobState
            {
                SessionId = "sessionId",
                BlobName = "blobName",
                BlobId = "blobId",
                Status = BlobStatus.Created,
                CompletedAt = new DateTime(2025, 05, 15),
                CreateAt = DateTime.UtcNow
            };
            var expectedDate = new DateTime(2025, 05, 15);
            var expectedFormatted = expectedDate.ToString("yy/MM/dd");

            ClassicAssert.AreEqual("sessionId", blobDetails.SessionId);
            ClassicAssert.AreEqual("blobName", blobDetails.BlobName);
            ClassicAssert.AreEqual("blobId", blobDetails.BlobId);
            ClassicAssert.AreEqual(BlobStatus.Created, blobDetails.Status);
            ClassicAssert.AreEqual(expectedFormatted, blobDetails.CreateAt.ToString("yy/MM/dd"));
            ClassicAssert.AreEqual(expectedFormatted, blobDetails.CompletedAt?.ToString("yy/MM/dd"));

        }


        [TestCase(BlobStatus.Unspecified)]
        [TestCase(BlobStatus.Created)]
        [TestCase(BlobStatus.Completed)]
        [TestCase(BlobStatus.Aborted)]
        [TestCase(BlobStatus.Deleted)]
        [TestCase(BlobStatus.Notfound)]
        [TestCase((BlobStatus)99)] 
        public void TestBlobStatus(BlobStatus status)
        {
            var blobDetails = new BlobState
            {
                Status = status
            };
            ClassicAssert.AreEqual(status, blobDetails.Status);

            switch (status)
            {
                case BlobStatus.Unspecified:
                    break;
                case BlobStatus.Created:
                    break;
                case BlobStatus.Completed:
                    break;
                case BlobStatus.Aborted:
                    break;
                case BlobStatus.Deleted:
                    break;
                case BlobStatus.Notfound:
                    break;
                default:
                    Assert.Throws<ArgumentOutOfRangeException>(() =>
                    {
                        SomeMethodThatUsesBlobStatus(status);
                    });
                    break;
            }
        }

        private void SomeMethodThatUsesBlobStatus(BlobStatus status)
        {
            if (!Enum.IsDefined(typeof(BlobStatus), status))
            {
                throw new ArgumentOutOfRangeException(nameof(status), status, "Undefined BlobStatus value");
            }
        }
    }
}