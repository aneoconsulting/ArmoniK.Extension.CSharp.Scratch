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
  private BlobDefinition(ReadOnlyMemory<byte> content,
                         bool                 manualDeletion)
  {
    Data           = content;
    ManualDeletion = manualDeletion;
  }

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

  internal static BlobDefinition CreateOutputBlobDefinition(bool manualDeletion)
    => new(null,
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a file
  /// </summary>
  /// <param name="filePath">The file containing the data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromFile(string filePath,
                                        bool   manualDeletion = false)
    => new(File.ReadAllBytes(filePath),
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a string
  /// </summary>
  /// <param name="content">The raw data</param>
  /// <param name="encoding">The encoding used for the string, when null UTF-8 is used</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromString(string    content,
                                          Encoding? encoding       = null,
                                          bool      manualDeletion = false)
    => new((encoding ?? Encoding.UTF8).GetBytes(content)
                                      .AsMemory(),
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a read only memory
  /// </summary>
  /// <param name="content">The raw data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromReadOnlyMemory(ReadOnlyMemory<byte> content,
                                                  bool                 manualDeletion = false)
    => new(content,
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a byte array
  /// </summary>
  /// <param name="content">The raw data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromByteArray(byte[] content,
                                             bool   manualDeletion = false)
    => new(content,
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from an int
  /// </summary>
  /// <param name="content">The raw data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromInt(int  content,
                                       bool manualDeletion = false)
    => new(BitConverter.GetBytes(content),
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a double
  /// </summary>
  /// <param name="content">The raw data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns></returns>
  public static BlobDefinition FromDouble(double content,
                                          bool   manualDeletion = false)
    => new(BitConverter.GetBytes(content),
           manualDeletion);
}
