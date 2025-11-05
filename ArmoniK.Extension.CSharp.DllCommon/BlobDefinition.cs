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

using System.Text;

using ArmoniK.Extension.CSharp.DllCommon.Handles;

namespace ArmoniK.Extension.CSharp.DllCommon;

/// <summary>
///   Description of a blob that serve to its creation
/// </summary>
public class BlobDefinition
{
  private BlobDefinition(string                name,
                         ReadOnlyMemory<byte>? content = null)
  {
    Name = name;
    Data = content;
  }

  /// <summary>
  ///   User defined blob's name
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  ///   The raw data
  /// </summary>
  public ReadOnlyMemory<byte>? Data { get; init; }

  /// <summary>
  ///   Handle once the blob has been registered
  /// </summary>
  public BlobHandle? BlobHandle { get; internal set; }

  internal static BlobDefinition CreateOutput(string name)
    => new(name);

  /// <summary>
  ///   Creates a BlobDefinition from a BlobHandle
  /// </summary>
  /// <param name="blobHandle">The blob handle</param>
  /// <returns>The newly created BlobDefinition</returns>
  public static BlobDefinition FromBlobHandle(BlobHandle blobHandle)
    => new(blobHandle.BlobId)
       {
         BlobHandle = blobHandle,
       };

  /// <summary>
  ///   Creates a BlobDefinition from a file
  /// </summary>
  /// <param name="name">User defined blob's name</param>
  /// <param name="filePath">The file containing the data</param>
  /// <returns>The newly created BlobDefinition</returns>
  public static BlobDefinition FromFile(string name,
                                        string filePath)
    => new(name,
           File.ReadAllBytes(filePath));

  /// <summary>
  ///   Creates a BlobDefinition from a string
  /// </summary>
  /// <param name="name">User defined blob's name</param>
  /// <param name="content">The raw data</param>
  /// <returns>The newly created BlobDefinition</returns>
  public static BlobDefinition FromString(string name,
                                          string content)
    => new(name,
           Encoding.UTF8.GetBytes(content)
                   .AsMemory());

  /// <summary>
  ///   Creates a BlobDefinition from a read only memory
  /// </summary>
  /// <param name="name">User defined blob's name</param>
  /// <param name="content">The raw data</param>
  /// <returns>The newly created BlobDefinition</returns>
  public static BlobDefinition FromReadOnlyMemory(string               name,
                                                  ReadOnlyMemory<byte> content)
    => new(name,
           content);

  /// <summary>
  ///   Creates a BlobDefinition from a byte array
  /// </summary>
  /// <param name="name">User defined blob's name</param>
  /// <param name="content">The raw data</param>
  /// <returns>The newly created BlobDefinition</returns>
  public static BlobDefinition FromByteArray(string name,
                                             byte[] content)
    => new(name,
           content);
}
