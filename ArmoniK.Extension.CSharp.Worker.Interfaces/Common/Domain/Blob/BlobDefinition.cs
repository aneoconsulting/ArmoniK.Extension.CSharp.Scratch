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

using ArmoniK.Extension.CSharp.Common.Exceptions;
using ArmoniK.Extension.CSharp.Worker.Interfaces.Handles;

namespace ArmoniK.Extension.CSharp.Worker.Interfaces.Common.Domain.Blob;

/// <summary>
///   Description of a blob that serve to its creation
/// </summary>
public class BlobDefinition
{
  private readonly FileInfo? file_;
  private          long      dataSize_;

  private BlobDefinition(string                name,
                         ReadOnlyMemory<byte>? content = null)
  {
    Name      = name;
    Data      = content;
    HasData   = content != null;
    dataSize_ = content?.Length ?? 0;
  }

  private BlobDefinition(string   name,
                         FileInfo file)
  {
    Name      = name;
    file_     = file;
    HasData   = true;
    dataSize_ = 0; // will be refreshed on task submission time
  }

  /// <summary>
  ///   User defined blob's name
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  ///   The blob's raw data.
  /// </summary>
  public ReadOnlyMemory<byte>? Data { get; private set; }

  /// <summary>
  ///   Indicates whether the blob definition was created with its data.
  /// </summary>
  public bool HasData { get; init; }

  /// <summary>
  ///   The size in bytes occupied by the blob in an RPC.
  /// </summary>
  public long TotalSize
    => Name.Length + dataSize_;

  /// <summary>
  ///   Handle once the blob has been registered
  /// </summary>
  public BlobHandle? BlobHandle { get; set; }

  /// <summary>
  ///   Fetch the last state the file, whenever the blob comes from a file.
  /// </summary>
  public void RefreshFile()
  {
    if (file_ != null)
    {
      file_.Refresh();
      dataSize_ = file_.Length;
    }
  }

  /// <summary>
  ///   Fetch the blob's data asynchronously, whenever the blob comes from a file.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async System.Threading.Tasks.Task FetchDataAsync(CancellationToken cancellationToken)
  {
    if (file_ != null)
    {
      Data = await File.ReadAllBytesAsync(file_.FullName,
                                          cancellationToken)
                       .ConfigureAwait(false);
      dataSize_ = Data.Value.Length;
    }
  }

  /// <summary>
  ///   Create an output blob definition
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition CreateOutput(string name)
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
  {
    var file = new FileInfo(filePath);
    if (!file.Exists)
    {
      throw new ArmoniKSdkException($"The file {file.FullName} does not exists.");
    }

    return new BlobDefinition(name,
                              file);
  }

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
