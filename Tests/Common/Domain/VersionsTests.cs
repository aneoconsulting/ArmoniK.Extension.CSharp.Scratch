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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Versions;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ArmoniK.Tests.Common.Domain;

[TestFixture]
public class VersionsInfoTests
{
  [Test]
  public void CreateVersionsInfoTest()
  {
    var versionsInfo = new VersionsInfo
                       {
                         Core = "1.0.0",
                         Api  = "2.0.0",
                       };

    ClassicAssert.AreEqual("1.0.0",
                           versionsInfo.Core);
    ClassicAssert.AreEqual("2.0.0",
                           versionsInfo.Api);
  }

  [Test]
  public void VersionsInfoEqualityTest()
  {
    var versionsInfo1 = new VersionsInfo
                        {
                          Core = "1.0.0",
                          Api  = "2.0.0",
                        };

    var versionsInfo2 = new VersionsInfo
                        {
                          Core = "1.0.0",
                          Api  = "2.0.0",
                        };

    ClassicAssert.AreEqual(versionsInfo1,
                           versionsInfo2);
  }

  [Test]
  public void VersionsInfoInequalityTest()
  {
    var versionsInfo1 = new VersionsInfo
                        {
                          Core = "1.0.0",
                          Api  = "2.0.0",
                        };

    var versionsInfo2 = new VersionsInfo
                        {
                          Core = "3.0.0",
                          Api  = "4.0.0",
                        };

    ClassicAssert.AreNotEqual(versionsInfo1,
                              versionsInfo2);
  }

  [Test]
  public void VersionsInfoNullOrEmptyTest()
  {
    var versionsInfo = new VersionsInfo
                       {
                         Core = null,
                         Api  = string.Empty,
                       };

    ClassicAssert.IsNull(versionsInfo.Core);
    ClassicAssert.IsEmpty(versionsInfo.Api);
  }
}
