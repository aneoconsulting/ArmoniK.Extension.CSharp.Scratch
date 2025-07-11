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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Handles;

using NUnit.Framework;

using Tests.Configuration;

namespace Tests.Handles;

[TestFixture]
public class SessionHandleTests
{
  [SetUp]
  public void SetUp()
  {
    mockedArmoniKClient_ = new MockedArmoniKClient();
    mockSessionInfo_     = new SessionInfo("testSessionId");
  }

  private MockedArmoniKClient? mockedArmoniKClient_;
  private SessionInfo?         mockSessionInfo_;

  [Test]
  public void ConstructorShouldInitializeProperties()
  {
    var sessionHandle = new SessionHandle(mockSessionInfo_!,
                                          mockedArmoniKClient_!);

    Assert.Multiple(() =>
                    {
                      SessionInfo convertedSessionInfo = sessionHandle;
                      Assert.That(convertedSessionInfo,
                                  Is.EqualTo(mockSessionInfo_));
                      Assert.That(convertedSessionInfo.SessionId,
                                  Is.EqualTo("testSessionId"));
                    });
  }

  [Test]
  public void ConstructorThrowsArgumentNullExceptionWhenSessionInfoIsNull()
    => Assert.That(() => new SessionHandle(null!,
                                           mockedArmoniKClient_!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("session"));

  [Test]
  public void ConstructorThrowsArgumentNullExceptionWhenClientIsNull()
    => Assert.That(() => new SessionHandle(mockSessionInfo_!,
                                           null!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("armoniKClient"));

  [Test]
  public void ImplicitConversionToSessionInfoShouldReturnCorrectSessionInfo()
  {
    var sessionHandle = new SessionHandle(mockSessionInfo_!,
                                          mockedArmoniKClient_!);

    SessionInfo convertedSessionInfo = sessionHandle;

    Assert.That(convertedSessionInfo,
                Is.EqualTo(mockSessionInfo_));
  }

  [Test]
  public void ImplicitConversionToSessionInfoThrowsArgumentNullExceptionWhenHandleIsNull()
  {
    SessionHandle? nullHandle = null;

    Assert.That(() =>
                {
                  SessionInfo _ = nullHandle!;
                },
                Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                      .EqualTo("sessionHandle"));
  }

  [Test]
  public void FromSessionInfoCreatesSessionHandleCorrectly()
  {
    var sessionHandle = SessionHandle.FromSessionInfo(mockSessionInfo_!,
                                                      mockedArmoniKClient_!);

    Assert.Multiple(() =>
                    {
                      SessionInfo convertedSessionInfo = sessionHandle;
                      Assert.That(convertedSessionInfo,
                                  Is.EqualTo(mockSessionInfo_));
                      Assert.That(convertedSessionInfo.SessionId,
                                  Is.EqualTo("testSessionId"));
                    });
  }

  [Test]
  public void FromSessionInfoThrowsArgumentNullExceptionWhenSessionInfoIsNull()
    => Assert.That(() => SessionHandle.FromSessionInfo(null!,
                                                       mockedArmoniKClient_!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("sessionInfo"));

  [Test]
  public void FromSessionInfoThrowsArgumentNullExceptionWhenClientIsNull()
    => Assert.That(() => SessionHandle.FromSessionInfo(mockSessionInfo_!,
                                                       null!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("armoniKClient"));

  [Test]
  public void ImplicitConversionWorksInMethodParameters()
  {
    var sessionHandle = new SessionHandle(mockSessionInfo_!,
                                          mockedArmoniKClient_!);

    Assert.That(() => Assert.That(sessionHandle,
                                  Is.Not.Null),
                Throws.Nothing);
  }

  [Test]
  public void CancelSessionAsyncWithCancelledTokenThrows()
  {
    var sessionHandle = new SessionHandle(mockSessionInfo_!,
                                          mockedArmoniKClient_!);

    var cancellationTokenSource = new CancellationTokenSource();
    cancellationTokenSource.Cancel();

    Assert.That(async () => await sessionHandle.CancelSessionAsync(cancellationTokenSource.Token),
                Throws.InstanceOf<OperationCanceledException>());
  }

  [Test]
  public void CloseSessionAsyncWithCancelledTokenThrows()
  {
    var sessionHandle = new SessionHandle(mockSessionInfo_!,
                                          mockedArmoniKClient_!);

    var cancellationTokenSource = new CancellationTokenSource();
    cancellationTokenSource.Cancel();

    Assert.That(async () => await sessionHandle.CloseSessionAsync(cancellationTokenSource.Token),
                Throws.InstanceOf<OperationCanceledException>());
  }

  [Test]
  public void SessionIdPropertyIsAccessible()
  {
    var sessionHandle = new SessionHandle(mockSessionInfo_!,
                                          mockedArmoniKClient_!);

    SessionInfo sessionInfo = sessionHandle;

    Assert.That(sessionInfo.SessionId,
                Is.EqualTo("testSessionId"));
  }

  [Test]
  public void SessionInfoPropertiesArePreserved()
  {
    var sessionHandle = new SessionHandle(mockSessionInfo_!,
                                          mockedArmoniKClient_!);

    SessionInfo convertedSessionInfo = sessionHandle;

    Assert.Multiple(() =>
                    {
                      Assert.That(convertedSessionInfo.SessionId,
                                  Is.EqualTo(mockSessionInfo_!.SessionId));
                      Assert.That(convertedSessionInfo,
                                  Is.EqualTo(mockSessionInfo_));
                    });
  }

  [Test]
  public void TwoSessionHandlesWithSameDataAreEqual()
  {
    var sessionHandle1 = new SessionHandle(mockSessionInfo_!,
                                           mockedArmoniKClient_!);
    var sessionHandle2 = new SessionHandle(mockSessionInfo_!,
                                           mockedArmoniKClient_!);

    SessionInfo sessionInfo1 = sessionHandle1;
    SessionInfo sessionInfo2 = sessionHandle2;

    Assert.That(sessionInfo1,
                Is.EqualTo(sessionInfo2));
  }

  [Test]
  public void MultipleImplicitConversionsWork()
  {
    var sessionHandle = new SessionHandle(mockSessionInfo_!,
                                          mockedArmoniKClient_!);

    SessionInfo sessionInfo1 = sessionHandle;
    SessionInfo sessionInfo2 = sessionHandle;

    Assert.That(sessionInfo1,
                Is.EqualTo(sessionInfo2));
  }
}
