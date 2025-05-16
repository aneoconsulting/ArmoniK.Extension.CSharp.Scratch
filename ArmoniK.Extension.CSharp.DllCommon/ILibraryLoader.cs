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

using System.Runtime.Loader;

using ArmoniK.Api.Worker.Worker;

namespace ArmoniK.Extension.CSharp.DllCommon;

/// <summary>
///   Provides functionality to load and manage dynamic libraries for the ArmoniK project.
/// </summary>
public interface ILibraryLoader : IDisposable
{
  /// <summary>
  ///   Loads a library asynchronously based on the task handler and cancellation token provided.
  /// </summary>
  /// <param name="taskHandler">The task handler containing the task options.</param>
  /// <param name="cancellationToken">The cancellation token to cancel the operation.</param>
  /// <returns>A task representing the asynchronous operation, containing the name of the dynamic library loaded.</returns>
  Task<string> LoadLibraryAsync(ITaskHandler      taskHandler,
                                CancellationToken cancellationToken);

  /// <summary>
  ///   Gets an instance of a class from the dynamic library.
  /// </summary>
  /// <typeparam name="T">Type that the created instance must be convertible to.</typeparam>
  /// <param name="dynamicLibrary">The dynamic library definition.</param>
  /// <returns>An instance of the class specified by <paramref name="dynamicLibrary" />.</returns>
  T GetClassInstance<T>(TaskLibraryDefinition dynamicLibrary)
    where T : class;

  /// <summary>
  ///   Gets the assembly load context for the specified library context key.
  /// </summary>
  /// <param name="libraryContextKey">The key of the library context.</param>
  /// <returns>The assembly load context associated with the specified key.</returns>
  AssemblyLoadContext GetAssemblyLoadContext(string libraryContextKey);

  /// <summary>
  ///   Reset the service
  /// </summary>
  void ResetService();
}
