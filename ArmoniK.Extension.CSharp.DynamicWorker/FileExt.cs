// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
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
//

namespace ArmoniK.Extension.CSharp.DynamicWorker;

internal static class FileExt
{
  /// <summary>
  ///   Try deleting a directory recursively.
  ///   Do not throw any error in case the directory does not exist (eg: already deleted)
  /// </summary>
  /// <param name="path">Path of the directory to delete</param>
  /// <returns>Whether the directory has been deleted</returns>
  internal static bool TryDeleteDirectory(string path)
  {
    try
    {
      Directory.Delete(path,
                        true);

      return true;
    }
    catch (IOException)
    {
      if (Directory.Exists(path))
      {
        throw;
      }
    }

    return false;
  }

  /// <summary>
  ///   Try creating a new directory.
  ///   Do not throw any error in case the directory already exists.
  /// </summary>
  /// <param name="path">Path of the directory to create</param>
  /// <returns>Whether the directory has been created</returns>
  internal static bool TryCreateDirectory(string path)
  {
    try
    {
      Directory.CreateDirectory(path);

      return true;
    }
    catch (IOException)
    {
      if (!Directory.Exists(path))
      {
        throw;
      }
    }

    return false;
  }

  /// <summary>
  ///   Moves the content of the source directory to the destination directory.
  /// </summary>
  /// <param name="sourceDirectory">The source directory.</param>
  /// <param name="destinationDirectory">The destination directory.</param>
  /// <exception cref="ArmoniKSdkException">Thrown when there is an error moving the directory content.</exception>
  internal static void MoveDirectoryContent(string sourceDirectory,
                                           string destinationDirectory)
  {
    TryCreateDirectory(destinationDirectory);

    // Create all directories in destination if they do not exist
    foreach (var dirPath in Directory.GetDirectories(sourceDirectory,
                                                      "*",
                                                      SearchOption.AllDirectories))
    {
      Directory.CreateDirectory(dirPath.Replace(sourceDirectory,
                                                destinationDirectory));
    }

    // Move all files from the source to the destination
    foreach (var newPath in Directory.GetFiles(sourceDirectory,
                                                "*.*",
                                                SearchOption.AllDirectories))
    {
      File.Move(newPath,
                newPath.Replace(sourceDirectory,
                                destinationDirectory));
    }
  }

}
