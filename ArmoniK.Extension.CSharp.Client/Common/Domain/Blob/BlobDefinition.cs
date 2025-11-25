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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;

using ArmoniK.Extension.CSharp.Client.Exceptions;
using ArmoniK.Extension.CSharp.Client.Handles;

namespace ArmoniK.Extension.CSharp.Client.Common.Domain.Blob;

/// <summary>
///   Description of a blob that serve to its creation
/// </summary>
public class BlobDefinition
{
  private readonly FileInfo?             file_;
  private          long                  dataSize_;
  private          ReadOnlyMemory<byte>? data_;

  /// <summary>
  ///   Creation of a blob definition with known data
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <param name="content">The blob data</param>
  private BlobDefinition(string               name,
                         ReadOnlyMemory<byte> content)
  {
    Name      = name;
    data_     = content;
    HasData   = true;
    file_     = null;
    dataSize_ = content.Length;
  }

  /// <summary>
  ///   Creation of a blob definition with a data file
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <param name="file">The FileInfo instance</param>
  private BlobDefinition(string   name,
                         FileInfo file)
  {
    Name      = name;
    data_     = null;
    HasData   = true;
    file_     = file;
    dataSize_ = 0; // will be refreshed on task submission time
  }

  /// <summary>
  ///   Creation of a blob definition with no data
  /// </summary>
  /// <param name="name">The blob name</param>
  private BlobDefinition(string name)
  {
    Name      = name;
    data_     = null;
    HasData   = false;
    file_     = null;
    dataSize_ = 0;
  }

  /// <summary>
  ///   Blob name
  /// </summary>
  public string Name { get; init; }

  /// <summary>
  ///   Whether the blob should be created as manually deleted by the user.
  ///   Warning: when the blob definition is created from a BlobHandle, the present property will
  ///   always be false and will not reflect the actual status whether the blob should be manually deleted.
  /// </summary>
  internal bool ManualDeletion { get; private set; }

  /// <summary>
  ///   Defines the callback on the blob once its data is available or when an error occurs.
  /// </summary>
  public ICallback? CallBack { get; private set; }

  /// <summary>
  ///   The size in bytes occupied by the blob in an RPC.
  /// </summary>
  internal long TotalSize
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
  ///   Indicates whether the blob may be a pipe,
  ///   when true it is not necessarily a pipe, when false we're sure it's not a pipe.
  /// </summary>
  internal bool MayBeAPipe
    => dataSize_ == 0 && file_ != null;

  /// <summary>
  ///   Fetch the last state the file, whenever the blob comes from a file.
  /// </summary>
  internal void RefreshFile()
  {
    if (file_ != null)
    {
      file_.Refresh();
      dataSize_ = file_.Length;
    }
  }

  internal void SetData(ReadOnlyMemory<byte> data)
  {
    data_     = data;
    dataSize_ = data.Length;
  }

  /// <summary>
  ///   Get the blob data by chunks of maximum size 'maxSize'.
  ///   At least 1 chunk will always be returned, even if it is empty.
  /// </summary>
  /// <param name="maxSize">The maximum size of the chunks, 0 for no limit</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An enumeration of chunks</returns>
  internal async IAsyncEnumerable<ReadOnlyMemory<byte>> GetDataAsync(int                                        maxSize           = 0,
                                                                     [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    if (data_.HasValue)
    {
      if (maxSize == 0)
      {
        yield return data_!.Value;
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
    else if (!string.IsNullOrEmpty(file_?.FullName))
    {
      using var fileStream = new FileStream(file_!.FullName,
                                            FileMode.Open,
                                            FileAccess.Read);
      if (maxSize == 0)
      {
        // We are not in the case of a pipe, then <dataSize_> hold the actual data size
        if (dataSize_ > 0)
        {
          var buffer = new byte[dataSize_];

          while (await fileStream.ReadAsync(buffer,
                                            0,
                                            (int)dataSize_,
                                            cancellationToken)
                                 .ConfigureAwait(false) > 0)
          {
            yield return buffer;
          }
        }
        else
        {
          // we must return at least one element, even an empty one
          yield return Array.Empty<byte>();
        }
      }
      else
      {
        // We are either in the case of an actual file (dataSize_ > 0) or a pipe (dataSize_ == 0).
        var remainingBytes = dataSize_ > 0
                               ? (int)dataSize_
                               : maxSize;
        var bytesToRead = remainingBytes > maxSize
                            ? maxSize
                            : remainingBytes;
        var buffer     = new byte[bytesToRead];
        var chunksRead = 0;
        int bytesRead;

        while ((bytesRead = await fileStream.ReadAsync(buffer,
                                                       0,
                                                       bytesToRead,
                                                       cancellationToken)
                                            .ConfigureAwait(false)) > 0)
        {
          chunksRead++;
          if (bytesRead < bytesToRead)
          {
            var lastBlock = new byte[bytesRead];
            Array.Copy(buffer,
                       lastBlock,
                       bytesRead);
            yield return lastBlock;
            break;
          }

          yield return buffer;

          if (dataSize_ > 0)
          {
            remainingBytes -= bytesRead;
            bytesToRead = remainingBytes > maxSize
                            ? maxSize
                            : remainingBytes;
          }

          if (bytesToRead == 0)
          {
            break;
          }

          buffer = new byte[bytesToRead];
        }

        if (chunksRead == 0)
        {
          // we must return at least one element, even an empty one
          yield return Array.Empty<byte>();
        }
      }
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
  ///   Set the blob as to be manually deleted
  /// </summary>
  /// <returns>The updated BlobDefinition</returns>
  public BlobDefinition WithManualDeletion()
  {
    ManualDeletion = true;
    return this;
  }

  /// <summary>
  ///   Defines the callback to be invoked once the data is available.
  /// </summary>
  /// <param name="callBack">The callback</param>
  /// <returns>The updated BlobDefinition</returns>
  public BlobDefinition WithCallback(ICallback callBack)
  {
    CallBack = callBack;
    return this;
  }

  /// <summary>
  ///   Creates a BlobDefinition from a blob handle
  /// </summary>
  /// <param name="handle">The blob handle</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition FromBlobHandle(BlobHandle handle)
    => new(handle.BlobInfo.BlobName)
       {
         BlobHandle = handle,
       };

  /// <summary>
  ///   Creates a BlobDefinition from a file
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="filePath">The file containing the data</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition FromFile(string blobName,
                                        string filePath)
  {
    var file = new FileInfo(filePath);
    if (!file.Exists)
    {
      throw new ArmoniKSdkException($"The file {file.FullName} does not exists.");
    }

    return new BlobDefinition(blobName,
                              file);
  }

  /// <summary>
  ///   Creates a BlobDefinition from a string
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <param name="encoding">The encoding used for the string, when null UTF-8 is used</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition FromString(string    blobName,
                                          string    content,
                                          Encoding? encoding = null)
    => new(blobName,
           (encoding ?? Encoding.UTF8).GetBytes(content)
                                      .AsMemory());

  /// <summary>
  ///   Creates a BlobDefinition from a read only memory
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition FromReadOnlyMemory(string               blobName,
                                                  ReadOnlyMemory<byte> content)
    => new(blobName,
           content);

  /// <summary>
  ///   Creates a BlobDefinition from a byte array
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <returns>The newly created blob definition</returns>
  public static BlobDefinition FromByteArray(string blobName,
                                             byte[] content)
    => new(blobName,
           content);
}
