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

using ArmoniK.Extension.CSharp.Client;
using ArmoniK.Extension.CSharp.Client.Handles;
using ArmoniK.Extension.CSharp.Common.Common.Domain.Blob;

using NUnit.Framework;

using Tests.Configuration;

namespace Tests.Handles;

[TestFixture]
public class BlobHandleTests
{
  [SetUp]
  public void SetUp()
  {
    mockedArmoniKClient_ = new MockedArmoniKClient();
    mockBlobInfo_ = new BlobInfo
                    {
                      BlobName  = "testBlob",
                      SessionId = "testSession",
                      BlobId    = "testBlobId",
                    };
  }

  private MockedArmoniKClient? mockedArmoniKClient_;
  private BlobInfo?            mockBlobInfo_;

  [Test]
  public void ConstructorWithBlobInfosShouldInitializeProperties()
  {
    var blobHandle = new BlobHandle(mockBlobInfo_!,
                                    mockedArmoniKClient_!);

    Assert.Multiple(() =>
                    {
                      Assert.That(blobHandle.BlobInfo,
                                  Is.EqualTo(mockBlobInfo_));
                      Assert.That(blobHandle.ArmoniKClient,
                                  Is.Not.Null);
                      Assert.That(blobHandle.ArmoniKClient,
                                  Is.InstanceOf<ArmoniKClient>());
                    });
  }

  [Test]
  public void ConstructorWithIndividualParametersShouldInitializeCorrectly()
  {
    var blobName  = "myBlob";
    var blobId    = "myId";
    var sessionId = "mySession";

    var blobHandle = new BlobHandle(blobName,
                                    blobId,
                                    sessionId,
                                    mockedArmoniKClient_!);

    Assert.Multiple(() =>
                    {
                      Assert.That(blobHandle.BlobInfo.BlobName,
                                  Is.EqualTo(blobName));
                      Assert.That(blobHandle.BlobInfo.BlobId,
                                  Is.EqualTo(blobId));
                      Assert.That(blobHandle.BlobInfo.SessionId,
                                  Is.EqualTo(sessionId));
                      Assert.That(blobHandle.ArmoniKClient,
                                  Is.Not.Null);
                    });
  }

  [Test]
  public void ConstructorWithBlobInfoThrowsArgumentNullExceptionWhenBlobInfoIsNull()
    => Assert.That(() => new BlobHandle(null!,
                                        mockedArmoniKClient_!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("blobInfo"));

  [Test]
  public void ConstructorWithBlobInfo_ThrowsArgumentNullException_WhenClientIsNull()
    => Assert.That(() => new BlobHandle(mockBlobInfo_!,
                                        null!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("armoniKClient"));

  [Test]
  public void ImplicitConversionToBlobInfoShouldReturnCorrectBlobInfo()
  {
    var blobHandle = new BlobHandle(mockBlobInfo_!,
                                    mockedArmoniKClient_!);

    BlobInfo convertedBlobInfo = blobHandle;

    Assert.That(convertedBlobInfo,
                Is.EqualTo(mockBlobInfo_));
  }

  [Test]
  public void ImplicitConversionToBlobInfoThrowsArgumentNullExceptionWhenHandleIsNull()
  {
    BlobHandle? nullHandle = null;

    Assert.That(() =>
                {
                  BlobInfo _ = nullHandle!;
                },
                Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                      .EqualTo("blobHandle"));
  }

  [Test]
  public void ImplicitConversionWorksInMethodParameters()
  {
    var blobHandle = new BlobHandle(mockBlobInfo_!,
                                    mockedArmoniKClient_!);

    // we make sure that we can use BlobHandle where BlobInfo is expected
    Assert.That(() => Assert.That(blobHandle,
                                  Is.Not.Null),
                Throws.Nothing);
  }
}
