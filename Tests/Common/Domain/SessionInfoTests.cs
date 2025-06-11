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

using NUnit.Framework;

namespace ArmoniK.Tests.Common.Domain;

[TestFixture]
public class SessionInfoTests
{
  [Test]
  public void CreateSessionInfoTest()
  {
    var sessionId   = "session123";
    var sessionInfo = new SessionInfo(sessionId);

    Assert.That(sessionInfo.SessionId,
                Is.EqualTo(sessionId));
  }

  [Test]
  public void SessionInfoEqualityTest()
  {
    var sessionId    = "session123";
    var sessionInfo1 = new SessionInfo(sessionId);
    var sessionInfo2 = new SessionInfo(sessionId);

    Assert.That(sessionInfo1,
                Is.EqualTo(sessionInfo2));
  }

  [Test]
  public void SessionInfoInequalityTest()
  {
    var sessionInfo1 = new SessionInfo("session123");
    var sessionInfo2 = new SessionInfo("session456");

    Assert.That(sessionInfo1,
                Is.Not.EqualTo(sessionInfo2));
  }

  [Test]
  public void SessionInfoGetHashCodeEqualForSameSessionId()
  {
    var sessionId    = "session123";
    var sessionInfo1 = new SessionInfo(sessionId);
    var sessionInfo2 = new SessionInfo(sessionId);

    Assert.That(sessionInfo1.GetHashCode(),
                Is.EqualTo(sessionInfo2.GetHashCode()));
  }

  [Test]
  public void SessionInfoGetHashCodeDifferentForDifferentSessionId()
  {
    var sessionInfo1 = new SessionInfo("session123");
    var sessionInfo2 = new SessionInfo("session456");

    Assert.That(sessionInfo1.GetHashCode(),
                Is.Not.EqualTo(sessionInfo2.GetHashCode()));
  }

  [Test]
  public void SessionInfoToStringContainsSessionId()
  {
    var sessionId   = "session123";
    var sessionInfo = new SessionInfo(sessionId);

    var stringRepresentation = sessionInfo.ToString();

    Assert.That(stringRepresentation,
                Does.Contain(sessionId));
  }

  [Test]
  public void SessionInfoEqualityOperatorReturnsTrueForSameSessionId()
  {
    var sessionInfo1 = new SessionInfo("session123");
    var sessionInfo2 = new SessionInfo("session123");

    Assert.That(sessionInfo1 == sessionInfo2,
                Is.True);
  }

  [Test]
  public void SessionInfoInequalityOperatorReturnsTrueForDifferentSessionId()
  {
    var sessionInfo1 = new SessionInfo("session123");
    var sessionInfo2 = new SessionInfo("session456");

    Assert.That(sessionInfo1 != sessionInfo2,
                Is.True);
  }
}
