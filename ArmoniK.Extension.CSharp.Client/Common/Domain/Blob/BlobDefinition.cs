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

using System;
using System.IO;
using System.Text;

using ArmoniK.Extension.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extension.CSharp.Client.Handles;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

/// <summary>
///   Description of a blob that serve to its creation
/// </summary>
public class BlobDefinition
{
  private BlobDefinition(string               name,
                         ReadOnlyMemory<byte> content,
                         bool                 manualDeletion)
  {
    Name           = name;
    Data           = content;
    ManualDeletion = manualDeletion;
  }

  /// <summary>
  ///   Blob name
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  ///   The raw data
  /// </summary>
  public ReadOnlyMemory<byte>? Data { get; init; }

  /// <summary>
  ///   Whether the blob should be manually deleted by the user
  /// </summary>
  public bool ManualDeletion { get; init; }

  /// <summary>
  ///   Handle once the blob has been registered
  /// </summary>
  public BlobHandle? BlobHandle { get; internal set; } = null;

  internal SessionInfo? SessionInfo { get; set; }

  /// <summary>
  ///   Create an output blob definition
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <param name="manualDeletion">Whether the blob should be manually deleted</param>
  /// <returns></returns>
  public static BlobDefinition CreateOutputBlobDefinition(string name,
                                                          bool   manualDeletion = false)
    => new(name,
           null,
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a file
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="filePath">The file containing the data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromFile(string blobName,
                                        string filePath,
                                        bool   manualDeletion = false)
    => new(blobName,
           File.ReadAllBytes(filePath),
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a string
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <param name="encoding">The encoding used for the string, when null UTF-8 is used</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromString(string    blobName,
                                          string    content,
                                          Encoding? encoding       = null,
                                          bool      manualDeletion = false)
    => new(blobName,
           (encoding ?? Encoding.UTF8).GetBytes(content)
                                      .AsMemory(),
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a read only memory
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromReadOnlyMemory(string               blobName,
                                                  ReadOnlyMemory<byte> content,
                                                  bool                 manualDeletion = false)
    => new(blobName,
           content,
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a byte array
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromByteArray(string blobName,
                                             byte[] content,
                                             bool   manualDeletion = false)
    => new(blobName,
           content,
           manualDeletion);
}
