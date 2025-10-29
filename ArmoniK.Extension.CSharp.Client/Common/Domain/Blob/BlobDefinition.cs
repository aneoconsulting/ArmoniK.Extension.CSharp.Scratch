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
using System.Collections.Generic;
using System.IO;
using System.Text;

using ArmoniK.Extension.CSharp.Client.Exceptions;
using ArmoniK.Extension.CSharp.Client.Handles;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

/// <summary>
///   Description of a blob that serve to its creation
/// </summary>
public class BlobDefinition
{
  private readonly long                  dataSize_;
  private readonly ReadOnlyMemory<byte>? data_;
  private readonly string                filePath_;

  /// <summary>
  ///   Creation of a blob definition with known data
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <param name="content">The blob data</param>
  /// <param name="manualDeletion">Whether the blob should be manually deleted</param>
  private BlobDefinition(string               name,
                         ReadOnlyMemory<byte> content,
                         bool                 manualDeletion)
  {
    Name           = name;
    data_          = content;
    HasData        = true;
    filePath_      = string.Empty;
    dataSize_      = content.Length;
    ManualDeletion = manualDeletion;
  }

  /// <summary>
  ///   Creation of a blob definition with a data file
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <param name="file">The FileInfo instance</param>
  /// <param name="manualDeletion">Whether the blob should be manually deleted</param>
  private BlobDefinition(string   name,
                         FileInfo file,
                         bool     manualDeletion)
  {
    Name           = name;
    data_          = null;
    HasData        = true;
    filePath_      = file.FullName;
    dataSize_      = file.Length;
    ManualDeletion = manualDeletion;
  }

  /// <summary>
  ///   Creation of a blob definition with no data
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <param name="manualDeletion">Whether the blob should be manually deleted</param>
  private BlobDefinition(string name,
                         bool   manualDeletion)
  {
    Name           = name;
    data_          = null;
    HasData        = false;
    filePath_      = string.Empty;
    dataSize_      = 0;
    ManualDeletion = manualDeletion;
  }

  /// <summary>
  ///   Blob name
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  ///   Whether the blob should be manually deleted by the user
  /// </summary>
  public bool ManualDeletion { get; init; }

  /// <summary>
  ///   The size in bytes occupied by the blob in an RPC.
  /// </summary>
  public long TotalSize
    => Name.Length + dataSize_;

  /// <summary>
  ///   Indicates whether the blob definition was created with its data.
  /// </summary>
  public bool HasData { get; init; }

  /// <summary>
  ///   Handle once the blob has been registered
  /// </summary>
  public BlobHandle? BlobHandle { get; internal set; }

  /// <summary>
  ///   Get the blob data by chunks of maximum size 'maxSize'
  /// </summary>
  /// <param name="maxSize">The maximum size of the chunks, 0 for no limit</param>
  /// <returns>An enumeration of the chunks</returns>
  public IEnumerable<ReadOnlyMemory<byte>> GetData(int maxSize = 0)
  {
    if (data_.HasValue)
    {
      if (maxSize == 0)
      {
        yield return data_.Value;
      }
      else
      {
        var offset        = 0;
        var remainingSize = (int)dataSize_;
        while (remainingSize > maxSize)
        {
          yield return data_.Value.Slice(offset,
                                         maxSize);
          remainingSize -= maxSize;
          offset        += maxSize;
        }

        if (offset == 0)
        {
          yield return data_.Value;
        }
        else
        {
          yield return data_.Value.Slice(offset,
                                         remainingSize);
        }
      }
    }
    else if (!string.IsNullOrEmpty(filePath_))
    {
      if (maxSize == 0)
      {
        yield return File.ReadAllBytes(filePath_);
      }
      else
      {
        using var fileStream = new FileStream(filePath_,
                                              FileMode.Open,
                                              FileAccess.Read);
        var buffer = new byte[maxSize];
        int bytesRead;

        while ((bytesRead = fileStream.Read(buffer,
                                            0,
                                            maxSize)) > 0)
        {
          if (bytesRead < maxSize)
          {
            var lastBlock = new byte[bytesRead];
            Array.Copy(buffer,
                       lastBlock,
                       bytesRead);
            yield return lastBlock;
          }
          else
          {
            yield return buffer;
            buffer = new byte[maxSize];
          }
        }
      }
    }
  }

  /// <summary>
  ///   Create an output blob definition
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <param name="manualDeletion">Whether the blob should be manually deleted</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition CreateOutputBlobDefinition(string name,
                                                          bool   manualDeletion = false)
    => new(name,
           manualDeletion);

  /// <summary>
  ///   Creates a BlobDefinition from a blob handle
  /// </summary>
  /// <param name="handle">The blob handle</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition FromBlobHandle(BlobHandle handle)
    => new(handle.BlobInfo.BlobName,
           false)
       {
         BlobHandle = handle,
       };

  /// <summary>
  ///   Creates a BlobDefinition from a file
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="filePath">The file containing the data</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition FromFile(string blobName,
                                        string filePath,
                                        bool   manualDeletion = false)
  {
    var file = new FileInfo(filePath);
    if (!file.Exists)
    {
      throw new ArmoniKSdkException($"The file {file.FullName} does not exists.");
    }

    return new BlobDefinition(blobName,
                              file,
                              manualDeletion);
  }

  /// <summary>
  ///   Creates a BlobDefinition from a string
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <param name="encoding">The encoding used for the string, when null UTF-8 is used</param>
  /// <param name="manualDeletion">Whether the blob created should be deleted manually</param>
  /// <returns>The newly created blob definition</returns>
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
  /// <returns>The newly created blob definition</returns>
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
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition FromByteArray(string blobName,
                                             byte[] content,
                                             bool   manualDeletion = false)
    => new(blobName,
           content,
           manualDeletion);
}
