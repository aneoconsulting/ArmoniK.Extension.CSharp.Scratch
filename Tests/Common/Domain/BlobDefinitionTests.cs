// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

using NUnit.Framework;

namespace Tests.Common.Domain;

public class BlobDefinitionTests
{
  private byte[] data_         = [];
  private string testFileName_ = string.Empty;

  [SetUp]
  public async Task Setup()
  {
    var tempPath = Path.GetTempPath();
    testFileName_ = Path.Combine(tempPath,
                                 "TestBlobFile");
    data_ = Enumerable.Range(0,
                             100)
                      .Select(i => (byte)i)
                      .ToArray();
    await File.WriteAllBytesAsync(testFileName_,
                                  data_)
              .ConfigureAwait(false);
  }

  [Test]
  public async Task TestBlobFile1()
  {
    var blob = BlobDefinition.FromFile("file1",
                                       testFileName_);
    blob.RefreshFile();
    var result = await blob.GetDataAsync()
                           .SingleAsync()
                           .ConfigureAwait(false);

    Assert.That(result.ToArray(),
                Is.EqualTo(data_));
  }

  [Test]
  public async Task TestBlobFile2()
  {
    var blob = BlobDefinition.FromFile("file1",
                                       testFileName_);
    blob.RefreshFile();
    var globalBytes = new List<byte>();
    await foreach (var result in blob.GetDataAsync(30)
                                     .ConfigureAwait(false))
    {
      globalBytes.AddRange(result.Span);
    }

    Assert.That(globalBytes.ToArray(),
                Is.EqualTo(data_));
  }

  [Test]
  public async Task TestBlobFile3()
  {
    var blob = BlobDefinition.FromFile("file1",
                                       testFileName_);
    blob.RefreshFile();
    var result = await blob.GetDataAsync(200)
                           .SingleAsync()
                           .ConfigureAwait(false);

    Assert.That(result.ToArray(),
                Is.EqualTo(data_));
  }
}
